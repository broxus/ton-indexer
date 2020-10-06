#pragma once

#include <td/actor/actor.h>
#include <tonlib/Config.h>
#include <tonlib/ExtClient.h>
#include <tonlib/ExtClientOutbound.h>
#include <tonlib/Stuff.h>
#include <tonlib/TonlibCallback.h>

#include "Repo.hpp"

namespace tdx
{
using namespace tonlib;

class Indexer final : public td::actor::Actor {
public:
    using SpawnActor = std::function<void(ton::BlockId /*block_id*/, td::uint32 /*count*/)>;

    Indexer(ExtClientRef ext_client_ref, Repo& repo, ton::BlockId block_id, td::actor::ActorShared<> parent, SpawnActor spawn_indexer, td::uint32 step = 1);

private:
    auto process_transaction(const td::Bits256& addr, td::Ref<vm::Cell>&& value) -> td::Status;
    auto process_result() -> td::Status;

    void finish_query();

    void start_up() final;
    void start_up_with_lookup();
    void proceed_with_block_id(const ton::BlockIdExt& block_id);
    void got_block_header(lite_api_ptr<ton::lite_api::liteServer_blockHeader>&& result);
    void got_block_data(lite_api_ptr<ton::lite_api::liteServer_blockData>&& result);

    void hangup() final { check(TonlibError::Cancelled()); }

    void check_finished()
    {
        if (!--pending_queries_) {
            finish_query();
        }
    }

    void check(td::Status status)
    {
        if (status.is_error()) {
            LOG(ERROR) << status.move_as_error().message();
            stop();
        }
    }

    ton::BlockIdExt block_id_{};
    ton::BlockId block_id_simple_{};

    td::int32 pending_queries_ = 0;

    td::BufferSlice data_;
    td::BufferSlice block_data_;

    td::actor::ActorShared<> parent_;
    ExtClient client_;

    Repo& repo_;
    SpawnActor spawn_indexer_;
    td::uint32 step_;
};

}  // namespace tdx
