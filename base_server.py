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
from logger_helper import LoggerHelper

logger = LoggerHelper.get_logger()


class BaseServer(object):
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
        self.mysql_host = self.get_init_value(mysql_host, None)
        self.mysql_port = self.get_init_value(mysql_port, BaseConfig.MYSQL_PORT)
        self.mysql_user = self.get_init_value(mysql_user, BaseConfig.MYSQL_USER)
        self.mysql_password = self.get_init_value(mysql_password, BaseConfig.MYSQL_PASSWORD)
        self.repl_user = self.get_init_value(repl_user, BaseConfig.REPL_USER)
        self.repl_password = self.get_init_value(repl_password, BaseConfig.REPL_PASSWORD)
        self.ssh_port = self.get_init_value(ssh_port, BaseConfig.SSH_PORT)
        self.ssh_user = self.get_init_value(ssh_user, BaseConfig.SSH_USER)
        self.ssh_password = self.get_init_value(ssh_password, BaseConfig.SSH_PASSWORD)
        self.binlog_dir = self.get_init_value(binlog_dir, BaseConfig.BINLOG_DIR)
        self.working_dir = self.get_init_value(working_dir, BaseConfig.WORKING_DIR)
        self.mysql_exe_path = self.get_init_value(mysql_exe_path, BaseConfig.MYSQL_EXE_PATH)
        self.binlog_exe_path = self.get_init_value(binlog_exe_path, BaseConfig.BINLOG_EXE_PATH)
        self.mysql_binlog_prefix = self.get_init_value(mysql_binlog_prefix, BaseConfig.MYSQL_BINLOG_PREFIX)

    def get_init_value(self, init_value, base_value):
        if init_value is None:
            return base_value
        else:
            return init_value

    def get_linux_server(self):
        return LinuxServer(
            ssh_host=self.mysql_host, ssh_port=self.ssh_port,
            ssh_user=self.ssh_user, ssh_password=self.ssh_password)

    def get_mysql_server(self):
        return MySQLServer(
            mysql_host=self.mysql_host,
            mysql_user=self.mysql_user,
            mysql_password=self.mysql_password,
            mysql_port=self.mysql_port,
            database_name="information_schema",
            mysql_charset="utf8",
            connect_timeout=BaseConfig.MAX_CONNECTION_TIMEOUT
        )

    def check_file_exist(self, file_path):
        cmd = "/bin/ls '{}' |wc -l".format(file_path)
        cmd_output, cmd_error = self.get_linux_server().remote_exec_cmd(cmd=cmd)
        if cmd_output is None:
            return False
        if "".join(cmd_output).find("No such file or directory") >= 0:
            return False
        return True

    def check_dir_exist(self, dir_path):
        linux_server = self.get_linux_server()
        cmd = "/bin/mkdir -p '{}'".format(dir_path)
        linux_server.remote_exec_cmd(cmd=cmd)
        cmd = "/bin/ls '{}'".format(dir_path)
        cmd_output, cmd_error = self.get_linux_server().remote_exec_cmd(cmd=cmd)
        if cmd_output is None:
            return False
        if "".join(cmd_output).find("No such file or directory") >= 0:
            return False
        return True

    def check_ssh_connection(self):
        return self.get_linux_server().ssh_connection_check()

    def ping_server(self, server_host):
        try:
            cmd = "/bin/ping -c " + server_host
            cmd_output, cmd_error = self.get_linux_server().remote_exec_cmd(cmd=cmd)
            if str("\n".join(cmd_output)).find("time=") >= 0:
                logger.info("获取服务器{0}上ping服务器{1}正常".format(self.mysql_host, server_host))
                return True
            else:
                logger.info("获取服务器{0}上ping服务器{1}失败".format(self.mysql_host, server_host))
                return False
        except Exception as ex:
            logger.warning("获取服务器{0}上ping服务器{1}出现异常".format(self.mysql_host, server_host))
            logger.warning("Exception:{0}\nTraceBack:{1}".format(str(ex), traceback.format_exc()))
            return False

    def get_binlog_file_index(self, file_name):
        return int(str(file_name).split(".")[-1])

    def is_bigger_pos(self, base_binlog_index, base_binlog_pos, source_binlog_index, source_binlog_pos):
        if source_binlog_index > base_binlog_index:
            return True
        if base_binlog_index == source_binlog_index and source_binlog_pos > base_binlog_pos:
            return True
        return False
