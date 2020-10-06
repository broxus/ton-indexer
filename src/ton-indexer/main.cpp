#include <td/utils/OptionsParser.h>
#include <td/utils/filesystem.h>
#include <td/utils/port/signals.h>

#include <iostream>

#include "App.hpp"

static auto parse_options(int argc, char** argv) -> tdx::App::Options
{
    td::OptionsParser args;
    tdx::App::Options program_options{};

    args.add_option('h', "help", "prints help", [&]() -> td::Status {
        std::cout << (PSLICE() << args).c_str();
        std::exit(2);
    });

    args.add_option('c', "config", "global config", [&](td::Slice arg) {
        TRY_RESULT(global_config, td::read_file(arg.str()))
        program_options.config = std::move(global_config);
        return td::Status::OK();
    });

    args.add_option('d', "db", "postgres db url", [&](td::Slice arg) {
        program_options.db_url = arg.str();
        return td::Status::OK();
    });

    args.add_option('t', "threads", "worker thread count (default 4)", [&](td::Slice arg) {
        TRY_RESULT(thread_count, td::to_integer_safe<td::uint32>(arg))
        program_options.thread_count = thread_count;
        return td::Status::OK();
    });

    args.add_option('m', "masterchain", "masterchain worker count (default 10)", [&](td::Slice arg) {
        TRY_RESULT(actor_count, td::to_integer_safe<td::uint32>(arg))
        program_options.masterchain_actor_count = actor_count;
        return td::Status::OK();
    });

    args.add_option('w', "workchain", "workchain worker count (default 10)", [&](td::Slice arg) {
        TRY_RESULT(actor_count, td::to_integer_safe<td::uint32>(arg))
        program_options.workchain_actor_count = actor_count;
        return td::Status::OK();
    });

    auto status = args.run(argc, argv);
    if (status.is_error()) {
        std::cerr << status.move_as_error().message().c_str() << std::endl;
        std::exit(1);
    }

    if (program_options.config.empty() || program_options.db_url.empty()) {
        std::cout << (PSLICE() << args).c_str();
        std::exit(1);
    }

    return program_options;
}

int main(int argc, char** argv)
{
#ifndef VERBOSE
    SET_VERBOSITY_LEVEL(verbosity_INFO);
    td::set_default_failure_signal_handler();
#endif

    auto options = parse_options(argc, argv);

    td::actor::Scheduler scheduler({options.thread_count});
    scheduler.run_in_context([&] { td::actor::create_actor<tdx::App>("ton-indexer", std::move(options)).release(); });
    scheduler.run();
    return 0;
}
