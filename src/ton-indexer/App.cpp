#include "App.hpp"

#include <tonlib/ExtClientLazy.h>

#include "Indexer.hpp"

namespace tdx
{
App::App(Options&& options)
    : options_{std::move(options)}
{
}

App::~App() = default;

void App::spawn_indexer(const ton::BlockId& block_id)
{
    auto id = actor_id_++;
    actors_[id] = td::actor::create_actor<Indexer>(  //
        "Indexer",
        client_.get_client(),
        options_.db_url,
        block_id,
        /*mode*/ 1,
        /*lt*/ 0,
        /*utime*/ 0,
        actor_shared(this, id),
        [actor_id = actor_id(this)](const ton::BlockId& block_id) { td::actor::send_closure(actor_id, &App::spawn_indexer, block_id); });
}

void App::start_up()
{
    config_ = tonlib::Config::parse(options_.config.as_slice().str()).move_as_ok();
    last_state_key_ = "mainnet";

    tonlib::LastBlockState last_state;
    auto zero_state = ton::ZeroStateIdExt(config_.zero_state_id.id.workchain, config_.zero_state_id.root_hash, config_.zero_state_id.file_hash);
    last_state.zero_state_id = zero_state;
    last_state.last_block_id = config_.zero_state_id;
    last_state.last_key_block_id = config_.zero_state_id;
    last_state.init_block_id = ton::BlockIdExt{};
    last_state.vert_seqno = static_cast<int>(config_.hardforks.size());

    kv_ = std::shared_ptr<tonlib::KeyValue>(tonlib::KeyValue::create_inmemory().move_as_ok().release());

    last_block_storage_.set_key_value(kv_);

    init_ext_client();
    LOG(INFO) << "init ext client";
    init_last_block(last_state);
    LOG(INFO) << "init last block";
    init_last_config();
    LOG(INFO) << "init last config";

    client_.set_client(get_client_ref());

    spawn_indexer(ton::BlockId{0, 0x8000000000000000ull, 1});
}

auto App::get_client_ref() -> tonlib::ExtClientRef
{
    return tonlib::ExtClientRef{.andl_ext_client_ = raw_client_.get(),
                                .last_block_actor_ = raw_last_block_.get(),
                                .last_config_actor_ = raw_last_config_.get()};
}

void App::init_ext_client()
{
    auto lite_clients_size = config_.lite_clients.size();
    CHECK(lite_clients_size != 0)
    auto lite_client_id = td::Random::fast(0, td::narrow_cast<int>(lite_clients_size) - 1);
    auto& lite_client = config_.lite_clients[lite_client_id];

    ref_cnt_++;
    class Callback : public tonlib::ExtClientLazy::Callback {
    public:
        explicit Callback(td::actor::ActorShared<> parent)
            : parent_(std::move(parent))
        {
        }

    private:
        td::actor::ActorShared<> parent_;
    };
    raw_client_ = tonlib::ExtClientLazy::create(lite_client.adnl_id, lite_client.address, td::make_unique<Callback>(td::actor::actor_shared()));
}

void App::init_last_block(tonlib::LastBlockState state)
{
    ref_cnt_++;
    class Callback : public tonlib::LastBlock::Callback {
    public:
        explicit Callback(td::actor::ActorShared<App> client)
            : client_(std::move(client))
        {
        }
        void on_state_changed(tonlib::LastBlockState state) override { LOG(INFO) << "state changed"; }
        void on_sync_state_changed(tonlib::LastBlockSyncState sync_state) override { LOG(INFO) << "sync state changed"; }

    private:
        td::actor::ActorShared<App> client_;
    };

    LOG(INFO) << last_state_key_;
    last_block_storage_.save_state(last_state_key_, state);

    LOG(INFO) << "saved state";

    raw_last_block_ = td::actor::create_actor<tonlib::LastBlock>(  //
        td::actor::ActorOptions().with_name("LastBlock").with_poll(false),
        get_client_ref(),
        state,
        config_,
        source_.get_cancellation_token(),
        td::make_unique<Callback>(td::actor::actor_shared(this)));
}

void App::init_last_config()
{
    ref_cnt_++;
    class Callback : public tonlib::LastConfig::Callback {
    public:
        explicit Callback(td::actor::ActorShared<App> client)
            : client_(std::move(client))
        {
        }

    private:
        td::actor::ActorShared<App> client_;
    };

    raw_last_config_ = td::actor::create_actor<tonlib::LastConfig>(td::actor::ActorOptions().with_name("LastConfig").with_poll(false),
                                                                   get_client_ref(),
                                                                   td::make_unique<Callback>(td::actor::actor_shared(this)));
}

void App::hangup_shared()
{
    auto it = actors_.find(get_link_token());
    if (it != actors_.end()) {
        actors_.erase(it);
    }
    else {
        ref_cnt_--;
    }
    try_stop();
}

void App::hangup()
{
    source_.cancel();
    is_closing_ = true;
    ref_cnt_--;
    raw_client_ = {};
    raw_last_block_ = {};
    raw_last_config_ = {};
    try_stop();
}

void App::try_stop()
{
    if (is_closing_ && ref_cnt_ == 0 && actors_.empty()) {
        stop();
    }
}

}  // namespace tdx
