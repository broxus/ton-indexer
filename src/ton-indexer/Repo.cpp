#include "Repo.hpp"

namespace tdx
{
constexpr pqxx::zview INSERT_BLOCK_STMT = "insert_block";
constexpr pqxx::zview INSERT_TRANSACTION_STMT = "insert_transaction";

Repo::Repo(const std::string& db_url)
    : conn_{db_url}
    , work_{conn_}
{
    conn_.prepare(INSERT_BLOCK_STMT, "INSERT INTO block VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING");
    conn_.prepare(INSERT_TRANSACTION_STMT, "INSERT INTO transaction VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING");
}

Repo::~Repo()
{
    check_commit(/*force*/ true);
}

void Repo::save_block_id(const ton::BlockIdExt& block_id)
{
    const auto root_hash_slice = block_id.root_hash.as_slice();
    const auto file_hash_slice = block_id.file_hash.as_slice();

    work_.exec_prepared0(INSERT_BLOCK_STMT,
                         static_cast<td::int16>(block_id.id.workchain),
                         static_cast<td::int64>(block_id.id.shard),
                         static_cast<td::int32>(block_id.id.seqno),
                         pqxx::binarystring{root_hash_slice.data(), root_hash_slice.size()},
                         pqxx::binarystring{file_hash_slice.data(), file_hash_slice.size()});
}

void Repo::save_transaction(const ton::BlockIdExt& block_id, const lite_api::liteServer_transactionId& transaction_id)
{
    const auto account_addr_slice = transaction_id.account_.as_slice();
    const auto transaction_hash_slice = transaction_id.hash_.as_slice();

    work_.exec_prepared0(INSERT_TRANSACTION_STMT,
                         static_cast<td::int16>(block_id.id.workchain),
                         pqxx::binarystring{account_addr_slice.data(), account_addr_slice.size()},
                         pqxx::binarystring{transaction_hash_slice.data(), transaction_hash_slice.size()},
                         static_cast<td::int64>(transaction_id.lt_),
                         static_cast<td::int16>(block_id.id.workchain),
                         static_cast<td::int64>(block_id.id.shard),
                         static_cast<td::int32>(block_id.id.seqno));
}

void Repo::check_commit(bool force)
{
    if (++pending_inserts_ > MAX_PENDING_INSERTS || force) {
        work_.commit();
        pending_inserts_ = 0;
    }
}

}  // namespace tdx
