#pragma once

#include <tonlib/Stuff.h>

#include <bitset>
#include <mutex>
#include <pqxx/pqxx>

namespace tdx
{
using namespace tonlib;

class Repo {
    struct Conn {
        explicit Conn(const std::string& db_url)
            : conn_{db_url}
            , work_{conn_}
        {
        }
        ~Conn() = default;

        pqxx::connection conn_;
        pqxx::work work_;

        td::int32 pending_inserts_ = 0;
    };

public:
    explicit Repo(const std::string& db_url, td::uint32 connection_count);
    ~Repo();

    void save_block_id(td::int32 conn_id, const ton::BlockIdExt& block_id);
    void save_transaction(td::int32 conn_id, const ton::BlockIdExt& block_id, const lite_api::liteServer_transactionId& transaction_id);
    void check_commit(td::int32 conn_id, bool force = false);

    auto get_last_blocks(td::int32 conn_id, td::int32 workchain) -> std::vector<ton::BlockId>;

    Conn& conn(td::uint32 thread_id);

private:
    constexpr static auto MAX_PENDING_INSERTS = 32;

    std::vector<uint8_t> connections_{};
    std::bitset<64> connection_flags_{};
};

}  // namespace tdx
