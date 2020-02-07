#!/usr/bin/env python
# -*- coding: utf-8 -*-
# =============================================================================
#     FileName:
#         Desc:
#       Author:
#        Email:
#     HomePage:
#      Version:
#   LastChange:
#      History:
# =============================================================================

import datetime, time
from mysql_cluster import MySQLCluster
from binlog_server import BinlogServer
from master_server import MasterServer
from slave_server import SlaveServer
from logger_helper import LoggerHelper
from multiprocessing import Process

logger = LoggerHelper.get_logger()


class ClusterManager(object):
    def __init__(self):
        self.cluster_list = []

    def init_clusters(self):
        mysql_cluster1 = MySQLCluster()
        mysql_cluster1.master_server = MasterServer(mysql_host="192.168.199.238")
        mysql_cluster1.slave_servers.append(SlaveServer(mysql_host="192.168.199.123", master_server_rank=100))
        mysql_cluster1.slave_servers.append(SlaveServer(mysql_host="192.168.199.118", master_server_rank=200))
        self.cluster_list.append(mysql_cluster1)

    def check_clusters(self):
        for cluster_item in self.cluster_list:
            master_server = cluster_item.master_server
            logger.info("检查主服务器：{0}:{1}".format(master_server.mysql_host, master_server.mysql_port))
            cluster_item.master_server.check_server()
            for slave_server in cluster_item.slave_servers:
                logger.info("检查从服务器：{0}:{1}".format(slave_server.mysql_host, slave_server.mysql_port))
                slave_server.check_server(master_server=cluster_item.master_server)
            for binlog_server in cluster_item.binlog_servers:
                logger.info("检查BINLOG服务器：{0}".format(binlog_server.mysql_host))
                binlog_server.check_server(master_server=cluster_item.master_server)

    def monitor_clusters(self):
        for cluster_item in self.cluster_list:
            master_server = cluster_item.master_server
            logger.info("检查主服务器：{0}:{1}".format(master_server.mysql_host, master_server.mysql_port))
            if master_server.get_mysql_server().ping_server():
                logger.info("主服务器：{0}:{1}正常".format(master_server.mysql_host, master_server.mysql_port))
            else:
                logger.warning("主服务器：{0}:{1}异常".format(master_server.mysql_host, master_server.mysql_port))
                master_connect_count = len(cluster_item.slave_servers)
                for slave_server in cluster_item.slave_servers:
                    if not slave_server.is_io_thread_running():
                        logger.warning("从服务器：{0}:{1}无法正常连接主库".format(slave_server.mysql_host, slave_server.mysql_port))
                        master_connect_count -= 1
                    else:
                        logger.warning("从服务器：{0}:{1}能正常连接主库".format(slave_server.mysql_host, slave_server.mysql_port))
                if master_connect_count == 0:
                    logger.warning(
                        "所有从库服务器都无法正常连接主库{0}:{1},开启故障自动转移".format(master_server.mysql_host, master_server.mysql_port))
                    sub_process = Process(target=cluster_item.failover_cluster, args=())
                    sub_process.start()


if __name__ == '__main__':
    LoggerHelper.init_logging()
    cm = ClusterManager()
    cm.init_clusters()
    while True:
        cm.monitor_clusters()
        time.sleep(1)
