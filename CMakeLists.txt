#find_package(mysqlclient)
#if (mysqlclient_FOUND)
    file(GLOB HEADERS "include/eosio/mysql_plugin/*.hpp")

    add_library(mysql_plugin
                mysql_plugin.cpp 
                )

    target_include_directories(mysql_plugin PUBLIC  "${CMAKE_CURRENT_SOURCE_DIR}/include")
    target_include_directories(mysql_plugin PUBLIC  "/usr/local/opt/mysql-client/include/mysql")


    target_link_libraries(mysql_plugin mysqlclient chain_plugin appbase)
#else()
    message ("Cannot Found mysqlclient library, Please install it")
#endif()
