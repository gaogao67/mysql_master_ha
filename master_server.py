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
from base_server import BaseServer
from binlog_server import BinlogServer
from linux_server import LinuxServer
from logger_helper import LoggerHelper

logger = LoggerHelper.get_logger(__name__)


class MasterServer(BaseServer):
    def __init__(self,
                 mysql_host,
                 mysql_port=None,
                 mysql_user=None,
                 mysql_password=None,
                 repl_user=None,
                 repl_password=None,
                 ssh_port=None,
                 ssh_user=None,
                 ssh_password=None,
                 binlog_dir=None,
                 working_dir=None,
                 mysql_exe_path=None,
                 binlog_exe_path=None,
                 mysql_binlog_prefix=None):
        super(MasterServer, self).__init__(
            mysql_host=mysql_host,
            mysql_port=mysql_port,
            mysql_user=mysql_user,
            mysql_password=mysql_password,
            repl_user=repl_user,
            repl_password=repl_password,
            ssh_port=ssh_port,
            ssh_user=ssh_user,
            ssh_password=ssh_password,
            binlog_dir=binlog_dir,
            working_dir=working_dir,
            mysql_exe_path=mysql_exe_path,
            binlog_exe_path=binlog_exe_path,
            mysql_binlog_prefix=mysql_binlog_prefix
        )

    def get_binlog_server(self):
        return BinlogServer(mysql_host=self.mysql_host,
                            mysql_port=self.mysql_port,
                            mysql_user=self.mysql_user,
                            mysql_password=self.mysql_password,
                            ssh_port=self.ssh_port,
                            ssh_user=self.ssh_user,
                            ssh_password=self.ssh_password,
                            binlog_dir=self.binlog_dir,
                            working_dir=self.working_dir,
                            mysql_exe_path=self.mysql_exe_path,
                            binlog_exe_path=self.binlog_exe_path,
                            mysql_binlog_prefix=self.mysql_binlog_prefix)

    def check_server(self):
        if not self.get_mysql_server().ping_server():
            logger.warning("主库服务器{}:{}无法正常访问".format(self.mysql_host, self.mysql_port))
            return False
        if not self.get_binlog_server().check_server(self):
            return False
        if self.get_mysql_server().is_read_only():
            logger.warning("主库服务器{}:{}当前为只读状态".format(self.mysql_host, self.mysql_port))
            return False
        logger.info("主库服务器{}:{}检查通过".format(self.mysql_host, self.mysql_port))
        return True
