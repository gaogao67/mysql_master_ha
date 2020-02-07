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
import os, time, datetime
import traceback
from base_config import BaseConfig
from linux_server import LinuxServer
from mysql_server import MySQLServer
from base_server import BaseServer
from slave_server import SlaveServer
from binlog_server import BinlogServer
from master_server import MasterServer
from logger_helper import LoggerHelper

logger = LoggerHelper.get_logger()


class MySQLCluster(object):
    def __init__(self):
        self.master_server = None
        self.slave_servers = list()
        self.binlog_servers = list()

    def failover_cluster(self):
        logger.warning("开始故障切换")
        new_master = self.get_new_master()
        logger.warning("新主服务器为：{0}:{1}".format(new_master.mysql_host, new_master.mysql_port))
        new_master.apply_all_relay_log()
        if new_master is None:
            logger.warning("未找到合适的新主库服务器")
        self.sync_from_master_server(
            new_master=new_master,
            old_master=self.master_server)
        for binlog_server in self.binlog_servers:
            self.sync_from_binlog_server(
                new_master=new_master,
                binlog_server=binlog_server
            )
        for slave_server in self.slave_servers:
            self.sync_from_slave_server(
                slave_server=slave_server,
                new_master=new_master
            )
        for slave_server in self.slave_servers:
            slave_server.change_master_with_gtid_mode(new_master)
            slave_server.start_slave()
        for binlog_server in self.binlog_servers:
            binlog_server.change_master(new_master)
        new_master.reset_slave()
        logger.warning("故障切换完成")

    def sync_from_binlog_server(self, new_master: SlaveServer, binlog_server: BinlogServer):
        binlog_server.sync_binlog_to_new_master(new_master)

    def sync_from_master_server(self, new_master: SlaveServer, old_master: MasterServer):
        if old_master.check_ssh_connection():
            old_master.get_binlog_server().sync_binlog_to_new_master(new_master=new_master)

    def sync_from_slave_server(self, new_master: SlaveServer, slave_server: SlaveServer):
        slave_server.sync_binlog_to_new_master(new_master=new_master)

    def get_new_master(self):
        new_master = None
        max_binlog_file = None
        max_binlog_index = None
        max_binlog_pos = None
        for slave_server in self.slave_servers:
            if slave_server.master_server_rank < 0:
                continue
            if new_master is None:
                new_master = slave_server
                max_binlog_file, max_binlog_index, max_binlog_pos = slave_server.get_max_binlog_pos()
                continue
            if slave_server.master_server_rank > new_master.master_server_rank:
                new_master = slave_server
                max_binlog_file, max_binlog_index, max_binlog_pos = slave_server.get_max_binlog_pos()
                continue
            if slave_server.master_server_rank == new_master.master_server_rank:
                cur_binlog_file, cur_binlog_index, cur_binlog_pos = slave_server.get_max_binlog_pos()
                if cur_binlog_index > max_binlog_index or (
                        cur_binlog_index == max_binlog_index and cur_binlog_pos > max_binlog_pos):
                    new_master = slave_server
                    max_binlog_file, max_binlog_index, max_binlog_pos = slave_server.get_max_binlog_pos()
                    continue
        return new_master
