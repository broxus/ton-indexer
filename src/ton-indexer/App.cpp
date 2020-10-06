#include "App.hpp"

#include <tonlib/ExtClientLazy.h>

#include <list>

#include "Indexer.hpp"

namespace tdx
{
constexpr auto INITIAL_SHARD = 0x8000000000000000ull;

App::App(Options&& options)
    : options_{std::move(options)}
    , repo_{options_.db_url, options_.thread_count + 1}
{
}

App::~App() = default;

void App::start_processing_masterchain()
{
    ton::BlockId last_masterchain_block;
    if (std::vector<ton::BlockId> last_masterchain_blocks = repo_.get_last_blocks(td::get_thread_id(), -1); !last_masterchain_blocks.empty()) {
        last_masterchain_block = last_masterchain_blocks[0];
    }
    else {
        last_masterchain_block = ton::BlockId{-1, INITIAL_SHARD, 1};
    }
    LOG(WARNING) << "Started processing masterchain from " << last_masterchain_block.to_str();
    spawn_indexer(last_masterchain_block, options_.masterchain_actor_count);
}

void App::start_processing_workchain(td::int32 workchain)
{
    std::vector<ton::BlockId> last_workchain_blocks = repo_.get_last_blocks(td::get_thread_id(), workchain);
    if (last_workchain_blocks.empty()) {
        spawn_indexer(ton::BlockId{workchain, INITIAL_SHARD, 1}, options_.workchain_actor_count);
    }

    // Sort by seqno and level
    std::sort(last_workchain_blocks.begin(), last_workchain_blocks.end(), [](const ton::BlockId& left, const ton::BlockId& right) {
        return left.seqno > right.seqno || td::lower_bit64(left.shard) < td::lower_bit64(right.shard);
    });

    std::vector<ton::BlockId> top_blocks{};

    // Graph node in helper trees
    struct Item {
        const ton::BlockId* block{};
        td::int32 child_count{};
    };

    // Helper trees
    std::list<std::list<Item>> leaves;

    // Process each block
    for (const auto& block : last_workchain_blocks) {
        bool is_leaf = true;

        // Try to find child in helper trees
        for (auto& leaf : leaves) {
            for (auto it = leaf.begin(); it != leaf.end(); ++it) {
                if (!ton::shard_is_parent(block.shard, it->block->shard)) {
                    continue;
                }
                // If child found
                const auto child_it = it;
                // Insert current block right after it
                leaf.insert(++it, Item{.block = &block, .child_count = 0});
                // Remove child from tree if all it's parents are found
                if (++child_it->child_count == 2) {
                    leaf.erase(child_it);
                }
                // Mark as parent and proceed to next block
                is_leaf = false;
                break;
            }
            if (!is_leaf) {
                break;
            }
        }

        // If no children are found
        if (is_leaf) {
            // Add workers task
            top_blocks.emplace_back(block);
            // Create leaf tree
            leaves.emplace_back(std::list{Item{.block = &block, .child_count = 0}});
        }
    }

    for (const auto& block : top_blocks) {
        LOG(WARNING) << "Started processing workchain from " << block.to_str();
        spawn_indexer(block, options_.workchain_actor_count);
    }
}

void App::spawn_indexer(ton::BlockId block_id, td::uint32 count)
{
    for (auto i = 0; i < count; ++i) {
        auto id = actor_id_++;
        actors_[id] = td::actor::create_actor<Indexer>(  //
            "Indexer",
            client_.get_client(),
            repo_,
            block_id,
            actor_shared(this, id),
            [actor_id = actor_id(this)](const ton::BlockId& block_id, td::uint32 count) {
                td::actor::send_closure(actor_id, &App::spawn_indexer, block_id, count);
            },
            count);

        ++block_id.seqno;
    }
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

    start_processing_masterchain();
    start_processing_workchain(0);
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
