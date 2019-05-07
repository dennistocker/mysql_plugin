# 编译
1. 将代码放在eos源代码plugins目录下
2. 在plugins/CMakeLists.txt 文件中加入mysql_plugin

    `add_subdirectory(mysql_plugin)`
3. 在programs/nodeos/CMakeLists.txt 文件中加入mysql_plugin

    `target_link_libraries( ${NODE_EXECUTABLE_NAME} PRIVATE -Wl,${whole_archive_flag} mysql_plugin -Wl,${no_whole_archive_flag} )`
4. 按原步骤编译EOS源代码

# 配置
以下配置可以配置在config.ini中，也可在命令行中设置
1. mysql-plugin-enable = true: 是否启用mysql插件，默认不启用
2. mysql-plugin-host = 127.0.0.1: 配置mysql服务地址，默认本机
3. mysql-plugin-port = 3306: 配置mysql服务端口，默认3306
4. mysql-plugin-user = root: 配置mysql用户名，默认root
5. mysql-plugin-password = : 配置mysql密码，默认为空
6. mysql-plugin-database = eos: 配置mysql数据库名称，默认eos
7. mysql-block-start = 0: 配置开始解析的块高，默认为0 
8. mysql-wipe = false：当以`replay-blockchain`、`hard-replay-blockchain`、`delete-all-blocks`参数启动nodeos时，可配置此参数，清空mysql数据库，从初始块开始统计。

