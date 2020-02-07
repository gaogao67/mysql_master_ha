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

class BaseConfig(object):
    """
    常用配置文件
    """
    MYSQL_USER = "root"
    MYSQL_PASSWORD = "123@.com"
    MYSQL_PORT = 3306
    REPL_USER = "root"
    REPL_PASSWORD = "123@.com"
    SSH_USER = "root"
    SSH_PASSWORD = "calvin"
    SSH_PORT = 22
    BINLOG_DIR = "/data0/software/mysql/data/data/"
    WORKING_DIR = "/data0/software/mysql/data/log/"
    BINLOG_EXE_PATH = "/data0/software/mysql/server/bin/mysqlbinlog"
    MYSQL_EXE_PATH = "/data0/software/mysql/server/bin/mysql"
    MYSQL_BINLOG_PREFIX = "mysql-bin"
    MAX_CONNECTION_TIMEOUT = 3
    MAX_SLAVE_DELAY_SECONDS = 300
