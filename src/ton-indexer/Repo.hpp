#pragma once

#include <tonlib/Stuff.h>

#include <mutex>
#include <pqxx/pqxx>

namespace tdx
{
using namespace tonlib;

class Repo {
public:
    explicit Repo(const std::string& db_url);
    ~Repo();

    void save_block_id(const ton::BlockIdExt& block_id);
    void save_transaction(const ton::BlockIdExt& block_id, const lite_api::liteServer_transactionId& transaction_id);
    void check_commit(bool force = false);

    pqxx::connection& conn() { return conn_; }
    [[nodiscard]] const pqxx::connection& conn() const { return conn_; }

private:
    constexpr static auto MAX_PENDING_INSERTS = 10;
    td::int32 pending_inserts_ = 0;

    pqxx::connection conn_;
    pqxx::work work_;
};

}  // namespace tdx
