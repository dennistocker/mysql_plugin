#pragma once

#include <eosio/chain_plugin/chain_plugin.hpp>
#include <appbase/application.hpp>
#include <memory>

namespace eosio {

using mysql_plugin_impl_ptr = std::shared_ptr<class mysql_plugin_impl>;

class mysql_plugin : public plugin<mysql_plugin> {
public:
    APPBASE_PLUGIN_REQUIRES((chain_plugin))

    mysql_plugin();
    virtual ~mysql_plugin();

    virtual void set_program_options(options_description& cli, options_description& cfg) override;

    void plugin_initialize(const variables_map& options);
    void plugin_startup();
    void plugin_shutdown();

private:
    mysql_plugin_impl_ptr my;
};

}
