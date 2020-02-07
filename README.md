# mysql_master_ha

## 判断主库异常
1、主库无法正常连接

2、从库IO进程状态(Slave_IO_Running)异常

## 新主库选取
1、从库提升为主库优先级别(master_server_rank)最高者为新主库

2、相同优先级别(master_server_rank)时获取到MASTER的日志最大者为新主库

## BINLOG日志补齐流程

1、等待新主库所有RELAY LOG应用完成

2、获取新主库应用老主库的BINLOG位点(Relay_Master_Log_File+Exec_Master_Log_Pos)

3、根据新主库已应用完成的位点，循环(主库服务器+所有BINLOG服务器)进行：

    1、截取BINLOG，多个BINLOG文件截取合并成一个新文件。
    2、使用MySQL命令连接到新主库执行新文件中脚本(使用binary-mode参数)

4、获取新主库已执行GTID集合(Executed_Gtid_Set),循环所有从库进行：

    1、对比新主库的GTID集合和当前从库获取到的GTID集合(Retrieved_Gtid_Set)，
       如果当前从库有额外事务，则将新主库作为从库挂到当前从库中进行数据同步。

5、将新主库提升为主库，将剩余从库和将BINLOG服务器挂载到新主库下。

PS1: 由于GTID存在，重复执行BINLOG服务器上截取到的日志不会污染数据。

PS2: 当主库异常但仍能SSH时，可将主库当成BINLOG服务器处理。

## 待完成
1、群集和实例信息从MySQL数据库中读取，方便管理配置。

2、实现主库正常情况下主从切换。

3、实现多监控节点，避免监控节点出现单点故障。