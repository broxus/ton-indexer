#include "Repo.hpp"

namespace tdx
{
constexpr pqxx::zview INSERT_BLOCK_STMT = "insert_block";
constexpr pqxx::zview INSERT_TRANSACTION_STMT = "insert_transaction";
constexpr pqxx::zview INSERT_MESSAGE_STMT = "insert_message";
constexpr pqxx::zview SELECT_LAST_BLOCKS = "select_last_blocks";

Repo::Repo(const std::string& db_url, td::uint32 connection_count)
{
    connections_.resize(static_cast<size_t>(connection_count) * sizeof(Conn));
    for (size_t i = 0; i < connection_count; ++i) {
        LOG(WARNING) << i << " " << db_url;

        Conn* item = new (reinterpret_cast<Conn*>(connections_.data()) + i) Conn{db_url};

        item->conn_.prepare(INSERT_BLOCK_STMT, "INSERT INTO block VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING");
        item->conn_.prepare(INSERT_TRANSACTION_STMT, "INSERT INTO transaction VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING");
        item->conn_.prepare(INSERT_MESSAGE_STMT, "INSERT INTO message VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING");
        item->conn_.prepare(SELECT_LAST_BLOCKS, "SELECT workchain, shard, MAX(seqno) FROM block WHERE workchain=$1 GROUP BY shard, workchain");
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

void Repo::save_transaction(td::int32 conn_id, const ton::BlockIdExt& block_id, const td::Bits256& addr, const td::Bits256& hash, td::uint64 lt)
{
    const auto account_addr_slice = addr.as_slice();
    const auto transaction_hash_slice = hash.as_slice();

    conn(conn_id).work_.exec_prepared0(INSERT_TRANSACTION_STMT,
                                       static_cast<td::int16>(block_id.id.workchain),
                                       pqxx::binarystring{account_addr_slice.data(), account_addr_slice.size()},
                                       pqxx::binarystring{transaction_hash_slice.data(), transaction_hash_slice.size()},
                                       static_cast<td::int64>(lt),
                                       static_cast<td::int16>(block_id.id.workchain),
                                       static_cast<td::int64>(block_id.id.shard),
                                       static_cast<td::int32>(block_id.id.seqno));
}

void Repo::save_message(td::int32 conn_id,
                        const td::Bits256& body_hash,
                        bool out,
                        size_t n,
                        ton::WorkchainId workchain,
                        const td::Bits256& addr,
                        const td::Bits256& hash,
                        td::uint64 lt)
{
    const auto body_hash_slice = body_hash.as_slice();
    const auto account_addr_slice = addr.as_slice();
    const auto transaction_hash_slice = hash.as_slice();

    conn(conn_id).work_.exec_prepared0(INSERT_MESSAGE_STMT,
                                       pqxx::binarystring{body_hash_slice.data(), body_hash_slice.size()},
                                       out,
                                       static_cast<td::int16>(n),
                                       static_cast<td::int32>(workchain),
                                       pqxx::binarystring{account_addr_slice.data(), account_addr_slice.size()},
                                       pqxx::binarystring{transaction_hash_slice.data(), transaction_hash_slice.size()},
                                       static_cast<td::int64>(lt));
}

void Repo::check_commit(td::int32 conn_id, bool force)
{
    auto& c = conn(conn_id);
    if (++c.pending_inserts_ > MAX_PENDING_INSERTS || force) {
        c.work_.commit();
        c.pending_inserts_ = 0;
    }
}

auto Repo::get_last_blocks(td::int32 conn_id, td::int32 workchain) -> std::vector<ton::BlockId>
{
    auto rows = conn(conn_id).work_.exec_prepared(SELECT_LAST_BLOCKS, static_cast<td::int32>(workchain));

    std::vector<ton::BlockId> results;
    results.reserve(rows.size());
    for (const auto& row : rows) {
        results.emplace_back(
            ton::BlockId{row[0].as<td::int32>(), static_cast<ton::ShardId>(row[1].as<td::int64>()), static_cast<ton::BlockSeqno>(row[2].as<td::int32>())});
    }

    return results;
}

Repo::Conn& Repo::conn(td::uint32 thread_id)
{
    return *(reinterpret_cast<Conn*>(connections_.data()) + thread_id);
}

}  // namespace tdx
