#include <eosio/mysql_plugin/mysql_plugin.hpp>
#include <fc/io/json.hpp>
#include <fc/variant.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/chrono.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include <queue>

#include <mysql.h>

namespace eosio {

static appbase::abstract_plugin& _mysql_plugin = app().register_plugin<mysql_plugin>();

class mysql_plugin_impl {
public:
    mysql_plugin_impl();
    ~mysql_plugin_impl();

    fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
    fc::optional<boost::signals2::scoped_connection> irreversible_block_connection;
    fc::optional<boost::signals2::scoped_connection> accepted_transaction_connection;
    fc::optional<boost::signals2::scoped_connection> applied_transaction_connection;

    void accepted_transaction( const chain::transaction_metadata_ptr& t );
    void applied_transaction( const chain::transaction_trace_ptr& t );
    void accepted_block( const chain::block_state_ptr& bs );
    void irreversible_block( const chain::block_state_ptr& bs );

    void consume_blocks();

    void process_applied_transaction( const chain::transaction_trace_ptr& t );
    void _process_applied_transaction( const chain::transaction_trace_ptr& t );
    void process_irreversible_block(const chain::block_state_ptr& bs);
    void _process_irreversible_block(const chain::block_state_ptr& bs);
    void _merge_action_traces(std::vector<chain::action_trace>& vec, const std::vector<chain::action_trace>& traces);

    void init();
    void wipe_database();
    void init_tables();
    void reset();

     template<typename Queue, typename Entry> void queue(Queue& queue, const Entry& e);

public:
    bool enabled = false;
    string host = "127.0.0.1";
    uint32_t port = 3306;
    string user = "";
    string password = "";
    string database = "";

    bool wipe_database_on_startup = false;
    uint32_t start_block_num = 0;

    uint32_t max_queue_size = 100;
    int queue_sleep_time = 0;
    size_t abi_cache_size = 0;

    boost::mutex mtx;
    boost::condition_variable condition;
    boost::thread consume_thread;
    std::atomic_bool done{false};
    fc::microseconds abi_serializer_max_time;

    std::deque<chain::transaction_trace_ptr> transaction_trace_queue;
    std::deque<chain::transaction_trace_ptr> transaction_trace_process_queue;
    std::deque<chain::block_state_ptr> irreversible_block_state_queue;
    std::deque<chain::block_state_ptr> irreversible_block_state_process_queue;

private:
    MYSQL *conn;
};

mysql_plugin_impl::mysql_plugin_impl()
{
}

mysql_plugin_impl::~mysql_plugin_impl()
{
    try {
        done = true;
        condition.notify_one();

        consume_thread.join();
    } catch(std::exception& e) {
        elog("Exception on mysql_plugin shutdown of consume thread: ${e}", ("e", e.what()));
    }
}

void mysql_plugin_impl::init()
{
    try {
        conn = mysql_init(NULL);
        if (!conn) {
            elog("mysql_init failed, ${errno}: ${error}", ("errno", mysql_errno(conn))("error", mysql_error(conn)));
            return;
        }
        if (!mysql_real_connect(conn, host.c_str(), user.c_str(), password.c_str(), database.c_str(), port, NULL, 0)) {
            elog("mysql_real_connect failed, ${errno}: ${error}", ("errno", mysql_errno(conn))("error", mysql_error(conn)));
            return;
        }
        init_tables();
    } catch(...) {
        if (conn) {
            mysql_close(conn);
            conn = NULL;
        }
    }

    consume_thread = boost::thread([this] { consume_blocks(); });
}

void mysql_plugin_impl::wipe_database()
{
    if (!conn) {
        return;
    }

    const char* sql = "DROP TABLE IF EXISTS accounts, records;";
    if (mysql_query(conn, sql)) {
        elog("mysql_query failed, ${errno}: ${error}", ("errno", mysql_errno(conn))("error", mysql_error(conn)));
        return;
    }

    init_tables();
}

void mysql_plugin_impl::init_tables()
{
    if (!conn) {
        elog("mysql connection not exists");
        return;
    }
    const char* sql = "CREATE TABLE IF NOT EXISTS `accounts`("
                      "`account` char(12) NOT NULL DEFAULT '', "
                      "`balance` bigint(20) NOT NULL DEFAULT '0', "
                      "PRIMARY KEY (`account`), "
                      "KEY `idx_balance` (`balance`) "
                      ") ENGINE=InnoDB DEFAULT CHARSET=utf8; ";
    if (mysql_query(conn, sql)) {
        elog("mysql_query failed, ${errno}: ${error}", ("errno", mysql_errno(conn))("error", mysql_error(conn)));
        return;
    }

    sql = "CREATE TABLE IF NOT EXISTS `records` ("
          "`seq` bigint(20) NOT NULL DEFAULT '0', "
          "`from` char(12) NOT NULL DEFAULT '', "
          "`to` char(12) NOT NULL DEFAULT '', "
          "`quantity` bigint(20) NOT NULL DEFAULT '0', "
          "`memo` varchar(256) NOT NULL DEFAULT '', "
          "`status` tinyint(4) NOT NULL DEFAULT '0', "
          "`time` int(11) NOT NULL DEFAULT '0', "
          "`transaction_id` char(64) NOT NULL DEFAULT '', "
          "`block_id` char(64) NOT NULL DEFAULT '', "
          "`block_num` int(11) NOT NULL DEFAULT '0', "
          "PRIMARY KEY (`seq`), "
          "KEY `idx_from` (`from`), "
          "KEY `idx_to` (`to`), "
          "KEY `idx_time` (`time`), "
          "KEY `idx_transaction_id` (`transaction_id`), "
          "KEY `idx_block_id` (`block_id`)"
          ") ENGINE=InnoDB DEFAULT CHARSET=utf8;";
    if (mysql_query(conn, sql)) {
        elog("mysql_query failed, ${errno}: ${error}", ("errno", mysql_errno(conn))("error", mysql_error(conn)));
        return;
    }
}

template<typename Queue, typename Entry>
void mysql_plugin_impl::queue( Queue& queue, const Entry& e ) {
   boost::mutex::scoped_lock lock( mtx );
   auto queue_size = queue.size();
   if( queue_size > max_queue_size ) {
      lock.unlock();
      condition.notify_one();
      queue_sleep_time += 10;
      if( queue_sleep_time > 1000 )
         wlog("queue size: ${q}", ("q", queue_size));
      boost::this_thread::sleep_for( boost::chrono::milliseconds( queue_sleep_time ));
      lock.lock();
   } else {
      queue_sleep_time -= 10;
      if( queue_sleep_time < 0 ) queue_sleep_time = 0;
   }
   queue.emplace_back( e );
   lock.unlock();
   condition.notify_one();
}

void mysql_plugin_impl::accepted_transaction( const chain::transaction_metadata_ptr& t )
{
}

void mysql_plugin_impl::applied_transaction( const chain::transaction_trace_ptr& t )
{
    try {
        // if (!t->producer_block_id.valid()) {
            // return;
        // }
        queue(transaction_trace_queue, t);
    } catch (fc::exception& e) {
         elog("FC Exception while applied_transaction ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_transaction");
   }
}

void mysql_plugin_impl::accepted_block( const chain::block_state_ptr& bs )
{
}

void mysql_plugin_impl::irreversible_block( const chain::block_state_ptr& bs )
{
    try {
        const auto signed_block_num = bs->block->block_num();
        ilog("irreversible_block, [block_num=${num1}] [signed_block_num=${num2}]", ("num1", bs->block_num)("num2", signed_block_num));
        queue(irreversible_block_state_queue, bs);
   } catch (fc::exception& e) {
      elog("FC Exception while applied_irreversible_block ${e}", ("e", e.to_string()));
   } catch (std::exception& e) {
      elog("STD Exception while applied_irreversible_block ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while applied_irreversible_block");
   }
}

void mysql_plugin_impl::consume_blocks()
{
    try {
        while (true) {
            boost::mutex::scoped_lock lock(mtx);
            while (transaction_trace_queue.empty() &&
                    irreversible_block_state_queue.empty() &&
                    !done) {
                condition.wait(lock);
            }

            size_t transaction_trace_size = transaction_trace_queue.size();
            if (transaction_trace_size > 0) {
                transaction_trace_process_queue = move(transaction_trace_queue);
                transaction_trace_queue.clear();
            }
            size_t irreversible_block_size = irreversible_block_state_queue.size();
            if (irreversible_block_size > 0) {
                irreversible_block_state_process_queue = move(irreversible_block_state_queue);
                irreversible_block_state_queue.clear();
            }

            lock.unlock();

            if (done) {
                ilog("draining queue, size: ${q}", ("q", transaction_trace_size + irreversible_block_size));
            }

            // process transactions
            auto start_time = fc::time_point::now();
            auto size = transaction_trace_process_queue.size();
            while (!transaction_trace_process_queue.empty()) {
                const auto& t = transaction_trace_process_queue.front();
                process_applied_transaction(t);
                transaction_trace_process_queue.pop_front();
            }
            auto time = fc::time_point::now() - start_time;
            auto per = size > 0 ? time.count()/size : 0;
            if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
                ilog( "process_applied_transaction,  time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );


            // process irreversible blocks
            start_time = fc::time_point::now();
            size = irreversible_block_state_process_queue.size();
            while (!irreversible_block_state_process_queue.empty()) {
                const auto& bs = irreversible_block_state_process_queue.front();
                process_irreversible_block(bs);
                irreversible_block_state_process_queue.pop_front();
            }
            time = fc::time_point::now() - start_time;
            per = size > 0 ? time.count()/size : 0;
            if( time > fc::microseconds(500000) ) // reduce logging, .5 secs
                ilog( "process_irreversible_block,   time per: ${p}, size: ${s}, time: ${t}", ("s", size)("t", time)("p", per) );

            if (transaction_trace_size == 0 && irreversible_block_size == 0 && done) {
                break;
            }
        }
        ilog("mysql_plugin consume thread shutdown gracefully");
    } catch (fc::exception& e) {
        elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
    } catch (std::exception& e) {
        elog("STD Exception while consuming block ${e}", ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while consuming block");
    }
}

void mysql_plugin_impl::process_applied_transaction( const chain::transaction_trace_ptr& t )
{
   try {
      _process_applied_transaction( t );
   } catch (fc::exception& e) {
      elog("FC Exception while processing applied transaction trace: ${e}", ("e", e.to_detail_string()));
   } catch (std::exception& e) {
      elog("STD Exception while processing applied transaction trace: ${e}", ("e", e.what()));
   } catch (...) {
      elog("Unknown exception while processing applied transaction trace");
   }
}

void mysql_plugin_impl::process_irreversible_block(const chain::block_state_ptr& bs)
{
  try {
      _process_irreversible_block( bs );
  } catch (fc::exception& e) {
     elog("FC Exception while processing irreversible block: ${e}", ("e", e.to_detail_string()));
  } catch (std::exception& e) {
     elog("STD Exception while processing irreversible block: ${e}", ("e", e.what()));
  } catch (...) {
     elog("Unknown exception while processing irreversible block");
  }
}

void mysql_plugin_impl::_merge_action_traces(std::vector<chain::action_trace>& vec, const std::vector<chain::action_trace>& traces)
{
    for (const auto& atrace : traces) {
        chain::name account = atrace.act.account;
        chain::name action = atrace.act.name;
        chain::name receiver = atrace.receipt.receiver;
        if (account != receiver) {
            continue;
        }

        // 不解析uid的transfer和charge的inline actions
        if (account == name{"uid"} && (action == name{"transfer"} || action == name{"charge"})) {
            vec.push_back(atrace);
            continue;
        }
        if (account == name{"hashstorevip"} && action == name{"transfer"}) {
            vec.push_back(atrace);
        }

        // inline actions
        _merge_action_traces(vec, atrace.inline_traces);
    }
}

void mysql_plugin_impl::_process_applied_transaction( const chain::transaction_trace_ptr& t )
{
    std::vector<chain::action_trace> action_traces;
    _merge_action_traces(action_traces, t->action_traces);
    if (action_traces.size() == 0) {
        return;
    }
    std::string sql = "replace into records (`seq`, `from`, `to`, `quantity`, `memo`, `status`, `time`, `transaction_id`, `block_id`, `block_num`) values ";

    for (const auto& atrace : action_traces) {
        auto account = atrace.act.account;
        auto action = atrace.act.name;
        auto trx_id = atrace.trx_id;
        auto block_num = atrace.block_num;
        auto block_time = atrace.block_time;
        auto block_id = atrace.producer_block_id.valid() ? string(*(atrace.producer_block_id)) : "";
        auto sequence = atrace.receipt.global_sequence;
        auto& chain = app().get_plugin<chain_plugin>().chain();
        auto trace_variant = chain.to_variant_with_abi(atrace, abi_serializer_max_time);
        auto data = trace_variant.get_object()["act"].get_object()["data"].get_object();

        string from = "";
        string to = "";
        asset quantity;
        string memo = "";

        // 充值记录
        if (account == name{"hashstorevip"} && action == name{"transfer"}) {
            from = data["from"].as<string>();
            auto original_to = data["to"].as<string>();
            quantity = data["quantity"].as<asset>();
            memo = data["memo"].as<string>();
            if (quantity.get_symbol() != symbol(4, "HST")) {
                continue;
            }
            // 仅解析uid账户数据
            if (original_to != name{"uid"}) {
                continue;
            }
            std::vector<string> vec;
            split(vec, memo, boost::is_any_of("^"));
            if (vec.size() == 1) {
                to = vec[0] + ".uid";
            } else if (vec.size() >= 2 && vec[0] == "tf") {
                to = vec[1];
            } else {
                continue;
            }
        }
        // 转出记录
        else if (account == name{"uid"} && (action == name{"transfer"} || action == name{"charge"})) {
            auto to_field = (action == name{"transfer"}) ? "to" : "contract";
            from = data["username"].as<string>();
            to = data[to_field].as<string>();
            quantity = data["quantity"].as<asset>();
            memo = data["memo"].as<string>();

        } else {
            elog("should never reach here, [account=${account}] [action=${action}]", ("account", account)("action", action));
            continue;
        }

        sql += "('" + std::to_string(sequence) + "','" + from + "','" + to + "','" +
            std::to_string(quantity.get_amount()) + "','" + memo + "','0','" +
            std::to_string(block_time.to_time_point().sec_since_epoch()) + "','" +
            string(trx_id) + "','" + block_id + "','" + std::to_string(block_num) + "'),";
    }
    sql.replace(sql.size()-1, 1, ";");

    if (mysql_query(conn, sql.c_str())) {
        elog("mysql_query failed, ${errno}: ${error}", ("errno", mysql_errno(conn))("error", mysql_error(conn)));
    }
}

void mysql_plugin_impl::_process_irreversible_block(const chain::block_state_ptr& bs)
{
    const auto signed_block_num = bs->block->block_num();
    ilog("_process_irreversible_block, [block_num=${num1}] [signed_block_num=${num2}]", ("num1", bs->block_num)("num2", signed_block_num));
    if (bs->block_num < start_block_num) {
        return;
    }

    struct rec {
        string from;
        string to;
        string quantity;

        rec(string f, string t, string q) :from(f), to(t), quantity(q) {}
    };
    std::vector<rec> records;
    for (const auto& trx: bs->block->transactions) {
        transaction_id_type trx_id;
        if (trx.trx.contains<chain::packed_transaction>()) {
            trx_id = trx.trx.get<chain::packed_transaction>().id();
        } else {
            trx_id = trx.trx.get<transaction_id_type>();
        }

        // udpate status
        std::string sql = "UPDATE records SET status=1 WHERE transaction_id='" + trx_id.str() + "';";
        if (mysql_query(conn, sql.c_str())) {
            elog("mysql_query failed, ${errno}: ${error}", ("errno", mysql_errno(conn))("error", mysql_error(conn)));
            return;
        }

        // update balance
        sql = "SELECT `from`, `to`, `quantity` FROM records WHERE transaction_id='" + trx_id.str() + "';";
        if (mysql_query(conn, sql.c_str())) {
            elog("mysql_query failed, ${errno}: ${error}", ("errno", mysql_errno(conn))("error", mysql_error(conn)));
            return;
        }
        auto res = mysql_store_result(conn);
        if (!res) {
            elog("mysql_store_result failed, ${errno}: ${error}", ("errno", mysql_errno(conn))("error", mysql_error(conn)));
            return;
        }
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(res))) {
            dlog("records become irreversible [from=${from}] [to=${to}] [quantity=${quantity}]",
                    ("from", row[0])("to", row[1])("quantity", row[2]));
            records.emplace_back(row[0], row[1], row[2]);
        }
        mysql_free_result(res);

        for (const auto& r : records) {
            if (r.to != "hashstorevip") {
                sql = "INSERT INTO accounts(`account`, `balance`) values ('" + r.to + "','" +
                    r.quantity + "') ON DUPLICATE KEY UPDATE balance=balance+" + r.quantity + ";";
                if (mysql_query(conn, sql.c_str())) {
                    elog("mysql_query failed, ${errno}: ${error}", ("errno", mysql_errno(conn))("error", mysql_error(conn)));
                    continue;
                }
            }

            if (r.from != "hashstorevip") {
                sql = "UPDATE accounts set balance=balance-" + r.quantity + " WHERE account='" + r.from + "';";
                if (mysql_query(conn, sql.c_str())) {
                    elog("mysql_query failed, ${errno}: ${error} [${sql}]", ("errno", mysql_errno(conn))("error", mysql_error(conn))("sql", sql));
                    continue;
                }
            }
        }
    }
}

/********************************
 *
 *          mysql_plugin
 *
 ********************************/

mysql_plugin::mysql_plugin()
    :my(new mysql_plugin_impl)
{
}

mysql_plugin::~mysql_plugin()
{
}

void mysql_plugin::set_program_options(options_description& cli, options_description& cfg)
{
    cfg.add_options()
        ("mysql-plugin-enable", bpo::bool_switch()->default_value(false), "weather enable the mysql_plugin")
        ("mysql-plugin-host", bpo::value<string>()->default_value("127.0.0.1"),   "the mysql host")
        ("mysql-plugin-port", bpo::value<uint32_t>()->default_value(3306),  "the mysql port")
        ("mysql-plugin-user",    bpo::value<string>()->default_value("root"), "the mysql user")
        ("mysql-plugin-password", bpo::value<string>()->default_value(""), "the mysql password")
        ("mysql-plugin-database", bpo::value<string>()->default_value("eos"), "the myqsl database")
        ("mysql-block-start", bpo::value<uint32_t>()->default_value(0), "the myqsl start block num");
}

void mysql_plugin::plugin_initialize(const variables_map& options)
{
    try {
        if (options.count("mysql-plugin-enable")) {
            my->enabled = options.at("mysql-plugin-enable").as<bool>();
        }

        if (my->enabled) {
            if (options.at("replay-blockchain").as<bool>() ||
                    options.at("hard-replay-blockchain").as<bool>() ||
                    options.at("delete-all-blocks").as<bool>()) {
                if (options.at("mysql-wipe").as<bool>()) {
                    ilog("Wiping mysql on startup");
                    my->wipe_database_on_startup = true;
                } else if (options.count("mysql-block-start") == 0) {
                    EOS_ASSERT(false, chain::plugin_config_exception, "--mysql-wipe required with replay-blockchain, --hard-replay-blockchain, or --delete-all-blocks"
                            " --mysql-wipe will remove all EOS collections from mysql.");
                }
            }

            if (options.count("mysql-plugin-host")) {
                my->host = options.at("mysql-plugin-host").as<string>();
            }
            if (options.count("mysql-plugin-port")) {
                my->port = options.at("mysql-plugin-port").as<uint32_t>();
            }
            if (options.count("mysql-plugin-user")) {
                my->user = options.at("mysql-plugin-user").as<string>();
            }
            if (options.count("mysql-plugin-password")) {
                my->password = options.at("mysql-plugin-password").as<string>();
            }
            if (options.count("mysql-plugin-database")) {
                my->database = options.at("mysql-plugin-database").as<string>();
            }
            if (options.count( "mysql-block-start" )) {
                my->start_block_num = options.at("mysql-block-start" ).as<uint32_t>();
            }

            my->abi_serializer_max_time = app().get_plugin<chain_plugin>().get_abi_serializer_max_time();
            auto& chain = app().get_plugin<chain_plugin>().chain();
            my->accepted_transaction_connection.emplace(
                    chain.accepted_transaction.connect([&](const chain::transaction_metadata_ptr& t) {
                        my->accepted_transaction(t);
            }));
            my->applied_transaction_connection.emplace(
                    chain.applied_transaction.connect([&](const chain::transaction_trace_ptr& t) {
                        my->applied_transaction(t);
            }));
            my->accepted_block_connection.emplace(
                    chain.accepted_block.connect([&](const chain::block_state_ptr& bs) {
                        my->accepted_block(bs);
            }));
            my->irreversible_block_connection.emplace(
                    chain.irreversible_block.connect([&](const chain::block_state_ptr& bs) {
                        my->irreversible_block(bs);
            }));

            my->init();
            if (my->wipe_database_on_startup) {
                my->wipe_database();
            }
        } else {
            ilog("mysql plugin not enabled");
        }
    } FC_LOG_AND_RETHROW()
}

void mysql_plugin::plugin_startup()
{
}

void mysql_plugin::plugin_shutdown()
{
    my->accepted_transaction_connection.reset();
    my->applied_transaction_connection.reset();
    my->accepted_block_connection.reset();
    my->irreversible_block_connection.reset();

    my.reset();
}

}
