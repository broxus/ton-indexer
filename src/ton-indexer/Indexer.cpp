#include "Indexer.hpp"

#include <block/block-auto.h>
#include <block/block.h>
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

Indexer::Indexer(ExtClientRef ext_client_ref,
                 const std::string& db_url,
                 ton::BlockId block_id,
                 td::actor::ActorShared<> parent,
                 SpawnActor spawn_indexer,
                 td::uint32 step)
    : block_id_simple_{block_id}
    , parent_{std::move(parent)}
    , repo_{db_url}
    , spawn_indexer_{std::move(spawn_indexer)}
    , step_{step}
{
    client_.set_client(std::move(ext_client_ref));
}

auto Indexer::process_result() -> td::Status
{
    repo_.save_block_id(block_id_);
    for (const auto& transaction_id : transactions_) {
        repo_.save_transaction(block_id_, *transaction_id);
    }
    repo_.check_commit();

    TRY_RESULT(block_header_root, vm::std_boc_deserialize(data_))
    auto virt_root = vm::MerkleProof::virtualize(block_header_root, 1);
    if (virt_root.is_null()) {
        return td::Status::Error("invalid merkle proof");
    }

    ton::RootHash vhash{virt_root->get_hash().bits()};
    std::vector<ton::BlockIdExt> prev{};
    ton::BlockIdExt masterchain_blk_id{};
    bool after_split = false;
    if (auto res = block::unpack_block_prev_blk_ext(virt_root, block_id_, prev, masterchain_blk_id, after_split); res.is_error()) {
        LOG(ERROR) << "failed to unpack block header " << block_id_.to_str() << ": " << res;
        return td::Status::Error("failed to unpack block header");
    }

    if (block_id_.is_masterchain()) {
        block_id_simple_ = next_block_id(block_id_, step_);
    }
    else {
        block::gen::Block::Record blk;
        if (!tlb::unpack_cell(std::move(virt_root), blk)) {
            return td::Status::Error("failed to unpack block record");
        }

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

    LOG(WARNING) << "Next block: " << block_id_simple_.to_str();
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
    auto block_transactions_handler = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<lite_api_ptr<lite_api::liteServer_blockTransactions>> R) {
        if (R.is_error()) {
            td::actor::send_closure(SelfId, &Indexer::check, R.move_as_error());
        }
        else {
            td::actor::send_closure(SelfId, &Indexer::got_transactions, R.move_as_ok());
        }
    });
    client_.send_query(lite_api::liteServer_listBlockTransactions(ton::create_tl_lite_block_id(block_id), 7, 1024, nullptr, false, false),
                       std::move(block_transactions_handler));
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

void Indexer::got_shard_info(lite_api_ptr<tonlib::lite_api::liteServer_allShardsInfo>&& result)
{
    shard_data_ = std::move(result->data_);
    check_finished();
}

void Indexer::got_transactions(lite_api_ptr<tonlib::lite_api::liteServer_blockTransactions>&& result)
{
    trans_req_count_ = result->req_count_;

    for (auto&& transaction_id : result->ids_) {
        transactions_.emplace_back(std::move(transaction_id));
    }

    if (result->incomplete_ && !transactions_.empty()) {
        const auto& last = transactions_.back();
        auto last_account = last->account_;

        auto block_transactions_handler =
            td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<lite_api_ptr<lite_api::liteServer_blockTransactions>> R) {
                if (R.is_error()) {
                    td::actor::send_closure(SelfId, &Indexer::check, R.move_as_error());
                }
                else {
                    td::actor::send_closure(SelfId, &Indexer::got_transactions, R.move_as_ok());
                }
            });
        client_.send_query(                              //
            lite_api::liteServer_listBlockTransactions(  //
                ton::create_tl_lite_block_id(block_id_),
                /*mode*/ 7 + 128,
                /*count*/ 1024,
                lite_api::make_object<ton::lite_api::liteServer_transactionId3>(last_account, last->lt_),
                /*reverse_order*/ false,
                /*want_proof*/ false),
            std::move(block_transactions_handler));
    }
    else {
        check_finished();
    }
}

}  // namespace tdx
