#pragma once

#include <td/actor/actor.h>
#include <tonlib/Config.h>
#include <tonlib/ExtClient.h>
#include <tonlib/ExtClientOutbound.h>
#include <tonlib/Stuff.h>
#include <tonlib/TonlibCallback.h>

#include <pqxx/pqxx>

namespace tdx
{
using namespace tonlib;

class Indexer : public td::actor::Actor {
    enum class QueryMode { Get, Lookup };

public:
    using SpawnActor = std::function<void(const ton::BlockId&)>;

    Indexer(ExtClientRef ext_client_ref, const std::string& db_url, ton::BlockIdExt block_id, td::actor::ActorShared<> parent, SpawnActor spawn_indexer);
    Indexer(ExtClientRef ext_client_ref,
            const std::string& db_url,
            ton::BlockId block_id,
            int mode,
            td::int64 lt,
            td::int32 utime,
            td::actor::ActorShared<> parent,
            SpawnActor spawn_indexer);
    ~Indexer();

private:
    auto parse_result() -> td::Status;
    void finish_query();

    void save_block_id(const ton::BlockIdExt& block_id);

    void start_up() override;
    void start_up_with_block_id(const ton::BlockIdExt& block_id);
    void start_up_with_lookup();
    void proceed_with_block_id(const ton::BlockIdExt& block_id);

    void got_block_header(lite_api_ptr<ton::lite_api::liteServer_blockHeader>&& result, QueryMode query_mode);
    void got_shard_info(lite_api_ptr<ton::lite_api::liteServer_allShardsInfo>&& result);
    void got_transactions(lite_api_ptr<ton::lite_api::liteServer_blockTransactions>&& result);

    void hangup() override { check(TonlibError::Cancelled()); }

    void check_finished()
    {
        if (!--pending_queries_) {
            finish_query();
        }
    }

    void check_commit(bool force = false)
    {
        if (++pending_inserts_ > MAX_PENDING_INSERTS || force) {
            repo_.commit();
            pending_inserts_ = 0;
        }
    }

    void check(td::Status status)
    {
        if (status.is_error()) {
            stop();
        }
    }

    constexpr static auto MAX_PENDING_INSERTS = 10;

    std::optional<ton::BlockIdExt> block_id_{};
    int mode_{};
    ton::BlockId block_id_simple_{};
    td::int64 lt_{};
    td::int32 utime_{};

    td::int32 pending_queries_ = 0;
    td::int32 pending_inserts_ = 0;

    td::BufferSlice data_;
    td::BufferSlice shard_data_;

    std::vector<tonlib_api_ptr<tonlib_api::liteServer_transactionId>> transactions_{};
    td::uint32 trans_req_count_{};

    td::actor::ActorShared<> parent_;
    ExtClient client_;

    pqxx::connection conn_;
    pqxx::work repo_;
    SpawnActor spawn_indexer_;
};

}  // namespace tdx
