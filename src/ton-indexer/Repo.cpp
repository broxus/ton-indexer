#include "Repo.hpp"

namespace tdx
{
constexpr pqxx::zview INSERT_BLOCK_STMT = "insert_block";
constexpr pqxx::zview INSERT_TRANSACTION_STMT = "insert_transaction";

Repo::Repo(const std::string& db_url, td::uint32 connection_count)
{
    connections_.resize(static_cast<size_t>(connection_count) * sizeof(Conn));
    for (size_t i = 0; i < connection_count; ++i) {
        LOG(WARNING) << i << " " << db_url;

        Conn* item = new (reinterpret_cast<Conn*>(connections_.data()) + i) Conn{db_url};

        item->conn_.prepare(INSERT_BLOCK_STMT, "INSERT INTO block VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING");
        item->conn_.prepare(INSERT_TRANSACTION_STMT, "INSERT INTO transaction VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING");
    }
}

Repo::~Repo()
{
    for (size_t i = 0; i < connections_.size(); ++i) {
        check_commit(i, /*force*/ true);
        (reinterpret_cast<Conn*>(connections_.data()) + i)->~Conn();
    }
}

void Repo::save_block_id(td::int32 conn_id, const ton::BlockIdExt& block_id)
{
    const auto root_hash_slice = block_id.root_hash.as_slice();
    const auto file_hash_slice = block_id.file_hash.as_slice();

    conn(conn_id).work_.exec_prepared0(INSERT_BLOCK_STMT,
                                       static_cast<td::int16>(block_id.id.workchain),
                                       static_cast<td::int64>(block_id.id.shard),
                                       static_cast<td::int32>(block_id.id.seqno),
                                       pqxx::binarystring{root_hash_slice.data(), root_hash_slice.size()},
                                       pqxx::binarystring{file_hash_slice.data(), file_hash_slice.size()});
}

void Repo::save_transaction(td::int32 conn_id, const ton::BlockIdExt& block_id, const lite_api::liteServer_transactionId& transaction_id)
{
    const auto account_addr_slice = transaction_id.account_.as_slice();
    const auto transaction_hash_slice = transaction_id.hash_.as_slice();

    conn(conn_id).work_.exec_prepared0(INSERT_TRANSACTION_STMT,
                                       static_cast<td::int16>(block_id.id.workchain),
                                       pqxx::binarystring{account_addr_slice.data(), account_addr_slice.size()},
                                       pqxx::binarystring{transaction_hash_slice.data(), transaction_hash_slice.size()},
                                       static_cast<td::int64>(transaction_id.lt_),
                                       static_cast<td::int16>(block_id.id.workchain),
                                       static_cast<td::int64>(block_id.id.shard),
                                       static_cast<td::int32>(block_id.id.seqno));
}

void Repo::check_commit(td::int32 conn_id, bool force)
{
    auto& c = conn(conn_id);
    if (++c.pending_inserts_ > MAX_PENDING_INSERTS || force) {
        c.work_.commit();
        c.pending_inserts_ = 0;
    }
}

Repo::Conn& Repo::conn(td::uint32 thread_id)
{
    return *(reinterpret_cast<Conn*>(connections_.data()) + thread_id);
}

}  // namespace tdx
