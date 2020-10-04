#include <iostream>
#include <pqxx/pqxx>

constexpr auto DB_URL = "postgresql://postgres:123456@127.0.0.1:5432/ton-explorer";

int main()
{
    try {
        pqxx::connection conn{DB_URL};
        std::cout << "Connected to " << conn.dbname() << std::endl;

        pqxx::work work{conn};
        pqxx::result result{work.exec("SELECT * FROM transaction")};
        std::cout << "Found " << result.size() << " transactions:\n";
        for (const auto& row : result) {
            std::cout << row[0].view() << "\n";
        }
    }
    catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

    return 0;
}
