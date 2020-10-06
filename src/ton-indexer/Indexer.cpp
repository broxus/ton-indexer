#include "Indexer.hpp"

#include <block/block-auto.h>
#include <block/block.h>
#include <td/utils/port/thread_local.h>
#include <tonlib/LastBlock.h>
#include <tonlib/LastConfig.h>
#include <vm/boc.h>
#include <vm/cells/MerkleProof.h>

#include <thread>
#include <ton/lite-tl.hpp>
#include <utility>

#include "Repo.hpp"

namespace tdx
{
namespace
{
auto next_block_id(const ton::BlockIdExt& block_id, int step) -> ton::BlockId
{
    return ton::BlockId{block_id.id.workchain, block_id.id.shard, block_id.id.seqno + step};
}

auto split_next_block_id_(const ton::BlockIdExt& block_id, int step, bool left) -> ton::BlockId
{
    return ton::BlockId{block_id.id.workchain, ton::shard_child(block_id.id.shard, left), block_id.id.seqno + step};
}

}  // namespace

Indexer::Indexer(ExtClientRef ext_client_ref, Repo& repo, ton::BlockId block_id, td::actor::ActorShared<> parent, SpawnActor spawn_indexer, td::uint32 step)
    : block_id_simple_{block_id}
    , parent_{std::move(parent)}
    , repo_{repo}
    , spawn_indexer_{std::move(spawn_indexer)}
    , step_{step}
{
    client_.set_client(std::move(ext_client_ref));
}

auto Indexer::process_transaction(const td::Bits256& addr, td::Ref<vm::Cell>&& value) -> td::Status
{
    const auto thread_id = td::get_thread_id();

    block::gen::Transaction::Record trans;
    if (!tlb::unpack_cell_inexact(value, trans)) {
        return td::Status::Error("failed to unpack transaction");
    }

    const auto transaction_hash = value->get_hash().bits();
    repo_.save_transaction(thread_id, block_id_, addr, transaction_hash, trans.lt);

    if (auto in_msg_ref = trans.r1.in_msg->prefetch_ref(); in_msg_ref.not_null()) {
        repo_.save_message(thread_id, in_msg_ref->get_hash().bits(), false, 0, block_id_.id.workchain, addr, transaction_hash, trans.lt);
    }

    vm::Dictionary dict{trans.r1.out_msgs, 15};
    for (td::int32 i = 0; i < trans.outmsg_cnt; ++i) {
        auto out_msg_ref = dict.lookup_ref(td::BitArray<15>{i});
        repo_.save_message(thread_id, out_msg_ref->get_hash().bits(), true, i, block_id_.id.workchain, addr, transaction_hash, trans.lt);
    }

    return td::Status::OK();
}

auto Indexer::process_result() -> td::Status
{
    const auto thread_id = td::get_thread_id();

    repo_.save_block_id(thread_id, block_id_);
    repo_.check_commit(thread_id);

    TRY_RESULT(block_root, vm::std_boc_deserialize(block_data_))

    std::vector<lite_api::liteServer_transactionId> result;
    try {
        block::gen::Block::Record blk;
        block::gen::BlockExtra::Record extra;
        if (!(tlb::unpack_cell(block_root, blk) && tlb::unpack_cell(std::move(blk.extra), extra))) {
            return td::Status::Error(PSLICE() << "cannot find account transaction data in block " << block_id_.to_str());
        }

        vm::AugmentedDictionary acc_dict{vm::load_cell_slice_ref(extra.account_blocks), 256, block::tlb::aug_ShardAccountBlocks};

        bool allow_same = true;
        td::Bits256 cur_addr{};
        while (true) {
            td::Ref<vm::CellSlice> value;
            try {
                value = acc_dict.extract_value(acc_dict.vm::DictionaryFixed::lookup_nearest_key(cur_addr.bits(), 256, true, allow_same));
            }
            catch (const vm::VmError& err) {
                return td::Status::Error(PSLICE() << "error while traversing account block dictionary: " << err.get_msg());
            }
            if (value.is_null()) {
                break;
            }

            allow_same = false;

            block::gen::AccountBlock::Record acc_blk;
            if (!(tlb::csr_unpack(std::move(value), acc_blk) && acc_blk.account_addr == cur_addr)) {
                return td::Status::Error(PSLICE() << "invalid AccountBlock for account " << cur_addr.to_hex());
            }

            vm::AugmentedDictionary trans_dict{vm::DictNonEmpty(), std::move(acc_blk.transactions), 64, block::tlb::aug_AccountTransactions};
            td::BitArray<64> cur_trans{};
            while (true) {
                td::Ref<vm::Cell> tvalue;
                try {
                    tvalue = trans_dict.extract_value_ref(trans_dict.vm::DictionaryFixed::lookup_nearest_key(cur_trans.bits(), 64, true));
                }
                catch (const vm::VmError& err) {
                    return td::Status::Error(PSLICE() << "error while traversing transaction dictionary of an AccountBlock: " << err.get_msg());
                }
                if (tvalue.is_null()) {
                    break;
                }

                TRY_STATUS(process_transaction(cur_addr, std::move(tvalue)))
            }
        }

        if (block_id_.is_masterchain()) {
            block_id_simple_ = next_block_id(block_id_, step_);
        }
        else {
            block::gen::BlockInfo::Record info;
            if (!tlb::unpack_cell(std::move(blk.info), info)) {
                return td::Status::Error("failed to unpack block info");
            }

            if (info.after_merge && ton::is_right_child(block_id_.id.shard)) {
                LOG(WARNING) << "Merging right child into left " << block_id_.id.to_str();
                stop();
                return td::Status::OK();
            }

            if (info.before_split) {
                const auto left_block = split_next_block_id_(block_id_, 1, true);
                const auto right_block = split_next_block_id_(block_id_, step_, false);

                LOG(WARNING) << "Split " << block_id_.id.to_str() << " -> " << left_block.to_str() << " and " << right_block.to_str();

                spawn_indexer_(left_block, step_);
                block_id_simple_ = right_block;
            }
            else {
                block_id_simple_ = next_block_id(block_id_, step_);
            }
        }
    }
    catch (const vm::VmError& err) {
        return td::Status::Error(PSLICE() << "error while parsing AccountBlocks of block " << block_id_.to_str() << " : " << err.get_msg());
    }

    LOG(INFO) << "Next block: " << block_id_simple_.to_str();
    td::actor::send_closure(actor_id(this), &Indexer::start_up_with_lookup);
    return td::Status::OK();
}

void Indexer::finish_query()
{
    check(process_result());
}

void Indexer::start_up()
{
    start_up_with_lookup();
}

void Indexer::start_up_with_lookup()
{
    auto block_header_handler = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<lite_api_ptr<lite_api::liteServer_blockHeader>> R) {
        if (R.is_error()) {
            td::actor::send_closure(SelfId, &Indexer::check, R.move_as_error());
        }
        else {
            td::actor::send_closure(SelfId, &Indexer::got_block_header, R.move_as_ok());
        }
    });
    client_.send_query(lite_api::liteServer_lookupBlock(1, ton::create_tl_lite_block_id_simple(block_id_simple_), 0, 0), std::move(block_header_handler));
    pending_queries_ = 1;
}

void Indexer::proceed_with_block_id(const ton::BlockIdExt& block_id)
{
    auto block_data_handler = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<lite_api_ptr<lite_api::liteServer_blockData>> R) {
        if (R.is_error()) {
            td::actor::send_closure(SelfId, &Indexer::check, R.move_as_error());
        }
        else {
            td::actor::send_closure(SelfId, &Indexer::got_block_data, R.move_as_ok());
        }
    });
    client_.send_query(lite_api::liteServer_getBlock(ton::create_tl_lite_block_id(block_id)), std::move(block_data_handler));
    pending_queries_++;
}

void Indexer::got_block_header(lite_api_ptr<lite_api::liteServer_blockHeader>&& result)
{
    const auto block_id = from_lite_api(*result->id_);
    block_id_ = block_id;
    proceed_with_block_id(block_id);

    data_ = std::move(result->header_proof_);
    check_finished();
}

void Indexer::got_block_data(lite_api_ptr<lite_api::liteServer_blockData>&& result)
{
    block_data_ = std::move(result->data_);
    check_finished();
}

}  // namespace tdx
