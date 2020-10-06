#pragma once

#include <td/actor/actor.h>
#include <tonlib/ExtClient.h>
#include <tonlib/LastBlock.h>
#include <tonlib/LastConfig.h>
#include <tonlib/TonlibClient.h>

#include "Repo.hpp"

namespace tdx
{
class App final : public td::actor::Actor {
public:
    struct Options {
        td::uint32 thread_count{4};
        td::uint32 masterchain_actor_count{10};
        td::uint32 workchain_actor_count{10};
        td::BufferSlice config{};
        std::string db_url{};
    };

    explicit App(Options&& options);
    ~App() final;

    void start_processing_masterchain();
    void start_processing_workchain(td::int32 workchain);
    void spawn_indexer(ton::BlockId block_id, td::uint32 count);

private:
    void start_up() final;

    auto get_client_ref() -> tonlib::ExtClientRef;
    void init_ext_client();
    void init_last_block(tonlib::LastBlockState state);
    void init_last_config();

    void hangup_shared() final;
    void hangup() final;
    void try_stop();

    Options options_;
    tonlib::Config config_;
    std::shared_ptr<tonlib::KeyValue> kv_;

    bool is_closing_{false};
    td::uint32 ref_cnt_{1};

    td::actor::ActorOwn<ton::adnl::AdnlExtClient> raw_client_;
    td::actor::ActorOwn<tonlib::LastBlock> raw_last_block_;
    td::actor::ActorOwn<tonlib::LastConfig> raw_last_config_;
    tonlib::ExtClient client_;
    Repo repo_;

    tonlib::LastBlockStorage last_block_storage_;
    std::string last_state_key_;

    td::CancellationTokenSource source_;

    std::map<td::int64, td::actor::ActorOwn<>> actors_;
    td::int64 actor_id_{1};
};

}  // namespace tdx
