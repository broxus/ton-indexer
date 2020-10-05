#include "Indexer.hpp"

#include <utility>

#include "block/block-auto.h"
#include "block/block-parse.h"
#include "block/block.h"
#include "ton/lite-tl.hpp"
#include "tonlib/LastBlock.h"
#include "tonlib/LastConfig.h"
#include "vm/boc.h"
#include "vm/cellops.h"
#include "vm/cells/MerkleProof.h"

namespace tdx
{
constexpr pqxx::zview INSERT_BLOCK_STMT = "insert_block";

Indexer::Indexer(ExtClientRef ext_client_ref, const std::string& db_url, ton::BlockIdExt block_id, td::actor::ActorShared<> parent, SpawnActor spawn_indexer)
    : block_id_{block_id}
    , parent_{std::move(parent)}
    , conn_{db_url}
    , repo_{conn_}
    , spawn_indexer_{std::move(spawn_indexer)}
{
    client_.set_client(std::move(ext_client_ref));
}

Indexer::Indexer(ExtClientRef ext_client_ref,
                 const std::string& db_url,
                 ton::BlockId block_id,
                 int mode,
                 td::int64 lt,
                 td::int32 utime,
                 td::actor::ActorShared<> parent,
                 SpawnActor spawn_indexer)
    : mode_{mode}
    , block_id_simple_{block_id}
    , lt_{lt}
    , utime_{utime}
    , parent_{std::move(parent)}
    , conn_{db_url}
    , repo_{conn_}
    , spawn_indexer_{std::move(spawn_indexer)}
{
    client_.set_client(std::move(ext_client_ref));
}

Indexer::~Indexer()
{
    check_commit(/*force*/ true);
}

auto Indexer::parse_result() -> td::Status
{
    if (!block_id_.has_value()) {
        return td::Status::Error("block not found");
    }
    auto& block_id = *block_id_;

    save_block_id(block_id);

    TRY_RESULT(block_header_root, vm::std_boc_deserialize(data_))
    auto virt_root = vm::MerkleProof::virtualize(block_header_root, 1);
    if (virt_root.is_null()) {
        return td::Status::Error("invalid merkle proof");
    }

    ton::RootHash vhash{virt_root->get_hash().bits()};
    std::vector<ton::BlockIdExt> prev{};
    ton::BlockIdExt masterchain_blk_id{};
    bool after_split = false;
    if (auto res = block::unpack_block_prev_blk_ext(virt_root, block_id, prev, masterchain_blk_id, after_split); res.is_error()) {
        LOG(ERROR) << "failed to unpack block header " << block_id.to_str() << ": " << res;
        return td::Status::Error("failed to unpack block header");
    }

    block::gen::Block::Record blk;
    if (!tlb::unpack_cell(virt_root, blk)) {
        return td::Status::Error("failed to unpack block record");
    }

    block::gen::BlockInfo::Record info;
    if (!tlb::unpack_cell(blk.info, info)) {
        return td::Status::Error("failed to unpack block info");
    }

    if (info.after_merge && ton::is_right_child(block_id.id.shard)) {
        LOG(WARNING) << "Merging right child into left " << block_id.id.to_str();
        stop();
        return td::Status::OK();
    }

    if (info.before_split) {
        const auto left_block = ton::BlockId{block_id.id.workchain, ton::shard_child(block_id.id.shard, true), block_id.id.seqno + 1};
        const auto right_block = ton::BlockId{block_id.id.workchain, ton::shard_child(block_id.id.shard, false), block_id.id.seqno + 1};

        LOG(WARNING) << "Split " << block_id.id.to_str() << " -> " << left_block.to_str() << " and " << right_block.to_str();

        block_id_simple_ = left_block;
        spawn_indexer_(right_block);
    }
    else {
        block_id_simple_ = ton::BlockId{block_id.id.workchain, block_id.id.shard, block_id.id.seqno + 1};
    }

    block_id_.reset();
    mode_ = 1;

    LOG(WARNING) << "Next block: " << block_id_simple_.to_str();
    td::actor::send_closure(actor_id(this), &Indexer::start_up_with_lookup);
    return td::Status::OK();
}

void Indexer::finish_query()
{
    check(parse_result());
}

void Indexer::save_block_id(const ton::BlockIdExt& block_id)
{
    const auto& root_hash_slice = block_id.root_hash.as_slice();
    const auto& file_hash_slice = block_id.file_hash.as_slice();

    repo_.exec_prepared0(INSERT_BLOCK_STMT,
                         static_cast<td::int16>(block_id.id.workchain),
                         static_cast<td::int64>(block_id.id.shard),
                         static_cast<td::int32>(block_id.id.seqno),
                         pqxx::binarystring{root_hash_slice.data(), root_hash_slice.size()},
                         pqxx::binarystring{file_hash_slice.data(), file_hash_slice.size()});

    check_commit();
}

void Indexer::start_up()
{
    conn_.prepare(INSERT_BLOCK_STMT, "INSERT INTO block VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING");

    if (block_id_.has_value()) {
        start_up_with_block_id(*block_id_);
    }
    else {
        start_up_with_lookup();
    }
}

void Indexer::start_up_with_block_id(const ton::BlockIdExt& block_id)
{
    auto block_header_handler = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<lite_api_ptr<lite_api::liteServer_blockHeader>> R) {
        if (R.is_error()) {
            td::actor::send_closure(SelfId, &Indexer::check, R.move_as_error());
        }
        else {
            td::actor::send_closure(SelfId, &Indexer::got_block_header, R.move_as_ok(), QueryMode::Get);
        }
    });
    client_.send_query(lite_api::liteServer_getBlockHeader(ton::create_tl_lite_block_id(block_id), 0), std::move(block_header_handler));
    pending_queries_ = 1;

    proceed_with_block_id(block_id);
}

void Indexer::start_up_with_lookup()
{
    auto block_header_handler = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<lite_api_ptr<lite_api::liteServer_blockHeader>> R) {
        if (R.is_error()) {
            td::actor::send_closure(SelfId, &Indexer::check, R.move_as_error());
        }
        else {
            td::actor::send_closure(SelfId, &Indexer::got_block_header, R.move_as_ok(), QueryMode::Lookup);
        }
    });
    client_.send_query(lite_api::liteServer_lookupBlock(mode_, ton::create_tl_lite_block_id_simple(block_id_simple_), lt_, utime_),
                       std::move(block_header_handler));
    pending_queries_ = 1;
}

void Indexer::proceed_with_block_id(const ton::BlockIdExt& block_id)
{
    // TODO: uncomment when other info will be needed

    //  if (block_id.is_masterchain()) {
    //    auto all_shards_info_handler = td::PromiseCreator::lambda(
    //        [SelfId = actor_id(this)](td::Result<lite_api_ptr<lite_api::liteServer_allShardsInfo>> R) {
    //          if (R.is_error()) {
    //            td::actor::send_closure(SelfId, &Indexer::check, R.move_as_error());
    //          } else {
    //            td::actor::send_closure(SelfId, &Indexer::got_shard_info, R.move_as_ok());
    //          }
    //        });
    //    client_.send_query(lite_api::liteServer_getAllShardsInfo(ton::create_tl_lite_block_id(block_id)),
    //                       std::move(all_shards_info_handler));
    //    pending_queries_++;
    //  }
    //
    //  auto block_transactions_handler = td::PromiseCreator::lambda(
    //      [SelfId = actor_id(this)](td::Result<lite_api_ptr<lite_api::liteServer_blockTransactions>> R) {
    //        if (R.is_error()) {
    //          td::actor::send_closure(SelfId, &Indexer::check, R.move_as_error());
    //        } else {
    //          td::actor::send_closure(SelfId, &Indexer::got_transactions, R.move_as_ok());
    //        }
    //      });
    //  client_.send_query(lite_api::liteServer_listBlockTransactions(ton::create_tl_lite_block_id(block_id), 7, 1024,
    //                                                                nullptr, false, false),
    //                     std::move(block_transactions_handler));
    //  pending_queries_++;
}

void Indexer::got_block_header(lite_api_ptr<lite_api::liteServer_blockHeader>&& result, QueryMode query_mode)
{
    if (query_mode == QueryMode::Lookup) {
        const auto block_id = from_lite_api(*result->id_);
        block_id_ = block_id;
        proceed_with_block_id(block_id);
    }

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
    if (!block_id_.has_value()) {
        check(td::Status::Error("block not found"));
        return;
    }
    const auto& block_id = *block_id_;

    trans_req_count_ = result->req_count_;

    for (auto&& transaction_id : result->ids_) {
        transactions_.emplace_back(to_tonlib_api(*transaction_id));
    }

    if (result->incomplete_ && !transactions_.empty()) {
        const auto& last = transactions_.back();
        auto last_account = to_bits256(last->account_, "last.account");

        if (last_account.is_ok()) {
            auto block_transactions_handler =
                td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<lite_api_ptr<lite_api::liteServer_blockTransactions>> R) {
                    if (R.is_error()) {
                        td::actor::send_closure(SelfId, &Indexer::check, R.move_as_error());
                    }
                    else {
                        td::actor::send_closure(SelfId, &Indexer::got_transactions, R.move_as_ok());
                    }
                });
            client_.send_query(  //
                lite_api::liteServer_listBlockTransactions(
                    ton::create_tl_lite_block_id(block_id),
                    7 + 128,
                    1024,
                    lite_api::make_object<ton::lite_api::liteServer_transactionId3>(last_account.move_as_ok(), last->lt_),
                    false,
                    false),
                std::move(block_transactions_handler));
        }
        else {
            check(last_account.move_as_error());
        }
    }
    else {
        check_finished();
    }
}

}  // namespace tdx
