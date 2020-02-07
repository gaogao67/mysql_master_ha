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
from master_server import MasterServer
from linux_server import LinuxServer
from logger_helper import LoggerHelper
from gtid_helper import GtidHelper

logger = LoggerHelper.get_logger()


class SlaveServer(BaseServer):
    def __init__(self,
                 mysql_host,
                 mysql_port=None,
                 mysql_user=None,
                 mysql_password=None,
                 ssh_port=None,
                 ssh_user=None,
                 repl_user=None,
                 repl_password=None,
                 ssh_password=None,
                 binlog_dir=None,
                 working_dir=None,
                 mysql_exe_path=None,
                 binlog_exe_path=None,
                 mysql_binlog_prefix=None,
                 master_server_rank=0):
        super(SlaveServer, self).__init__(
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
        self.master_server_rank = master_server_rank

    def get_slave_status(self):
        return self.get_mysql_server().get_slave_status()

    def get_retrieved_gtid_set(self):
        slave_status_list = self.get_slave_status()
        if len(slave_status_list) == 0:
            return ''
        return slave_status_list[0]["Retrieved_Gtid_Set"]

    def get_max_binlog_pos(self):
        try:
            query_result = self.get_slave_status()
            if len(query_result) == 0:
                logger.warning("获取从服务器{0}:{1}的最大位点失败，无复制信息".format(self.mysql_host, self.mysql_port))
                return None, None, None
            row_item = query_result[0]
            max_binlog_file = row_item["Master_Log_File"]
            max_binlog_pos = int(row_item["Read_Master_Log_Pos"])
            logger.warning("获取从服务器{0}:{1}的最大位点:{2},{3}".format(
                self.mysql_host, self.mysql_port,
                max_binlog_file, max_binlog_pos,
            ))
            max_binlog_index = self.get_binlog_file_index(max_binlog_file)
            return max_binlog_file, max_binlog_index, max_binlog_pos
        except Exception as ex:
            logger.warning("获取从服务器{0}:{1}的最大位点失败".format(self.mysql_host, self.mysql_port))
            logger.warning("异常信息:{0}\n堆栈信息:{1}".format(str(ex), traceback.format_exc()))
            return None, None, None

    def is_io_thread_running(self):
        try:
            query_result = self.get_slave_status()[0]
            if query_result["Slave_IO_Running"] == "Yes":
                return True
            else:
                return False
        except Exception as ex:
            logger.warning("获取从服务器{0}:{1}的IO线程状态失败".format(self.mysql_host, self.mysql_port))
            logger.warning("异常信息:{0}\n堆栈信息:{1}".format(str(ex), traceback.format_exc()))
            return None

    def start_slave(self):
        self.get_mysql_server().mysql_exec(sql_script="START SLAVE;")

    def start_slave_until(self, stop_binlog_file, stop_binlog_post, stop_gtid_set):
        """
        :param stop_binlog_file: 停止的BINLOG文件(MASTER_LOG_FILE)
        :param stop_binlog_post: 停止的BINLOG位点(MASTER_LOG_POS)
        :param stop_gtid_set: 停止的GTID集合点(SQL_AFTER_GTIDS)
        :return:
        """
        mysql_server = self.get_mysql_server()
        mysql_server.mysql_exec(sql_script="STOP SLAVE;")

        if stop_gtid_set is None:
            mysql_server.mysql_exec(
                sql_script="START SLAVE UNTIL MASTER_LOG_FILE='{0}',MASTER_LOG_POS={1};".format(
                    stop_binlog_file, stop_binlog_post
                )
            )
        else:
            mysql_server.mysql_exec(
                sql_script="START SLAVE UNTIL SQL_AFTER_GTIDS='{0}'".format(
                    stop_gtid_set
                )
            )

    def change_master_with_gtid_mode(self, master_server: BaseServer):
        mysql_server = self.get_mysql_server()
        mysql_server.mysql_exec(sql_script="STOP SLAVE;")
        sql_script = """CHANGE MASTER TO 
MASTER_HOST='{mysql_host}',
MASTER_PORT={mysql_port},
MASTER_USER='{repl_user}',
MASTER_PASSWORD='{repl_password}',
MASTER_AUTO_POSITION=1;""".format(
            mysql_host=master_server.mysql_host,
            mysql_port=master_server.mysql_port,
            repl_user=master_server.repl_user,
            repl_password=master_server.repl_password
        )
        logger.warning(sql_script)
        mysql_server.mysql_exec(sql_script=sql_script)

    def apply_all_relay_log(self):
        while True:
            slave_status = self.get_slave_status()[0]
            if slave_status["Slave_SQL_Running"] != "Yes":
                self.get_mysql_server().mysql_exec(sql_script="START SLAVE SQL_THREAD;")
            elif slave_status["Master_Log_File"] == slave_status["Relay_Master_Log_File"] \
                    and slave_status["Read_Master_Log_Pos"] == slave_status["Exec_Master_Log_Pos"]:
                logger.warning("服务器{}:{}已应用BINLOG至{},{}".format(
                    self.mysql_host, self.mysql_port,
                    slave_status["Relay_Master_Log_File"],
                    slave_status["Exec_Master_Log_Pos"]
                ))
                break
            else:
                logger.warning("正在同步服务器{}:{}上BINLOG".format(
                    self.mysql_host, self.mysql_port))
                time.sleep(0.1)

    def sync_binlog_to_new_master(self, new_master):
        master_gtid_set = new_master.get_mysql_server().get_executed_gtid_set()
        slave_gtid_set = self.get_retrieved_gtid_set()
        if GtidHelper.is_included(max_set=master_gtid_set, min_set=slave_gtid_set):
            logger.warning("新主库{}:{}已拥有从库{}:{}上所有事务".format(
                new_master.mysql_host,
                new_master.mysql_port,
                self.mysql_host,
                self.mysql_port
            ))
            return
        self.apply_all_relay_log()
        executed_gtid_set = self.get_mysql_server().get_executed_gtid_set()
        new_master.apply_all_relay_log()
        new_master.change_master_with_gtid_mode(self)
        new_master.start_slave_until(stop_binlog_file=None, stop_binlog_post=None, stop_gtid_set=executed_gtid_set)

    def is_string_equal(self, item1, item2):
        return str(item2).lower().strip() == str(item1).lower().strip()

    def check_server(self, master_server: MasterServer):
        if not self.get_mysql_server().ping_server():
            logger.warning("从库服务器{0}:{1}上无法正常连接".format(
                self.mysql_host, self.mysql_port
            ))
            return False
        slave_status_list = self.get_mysql_server().get_slave_status()
        if len(slave_status_list) > 1:
            logger.warning("从库服务器{0}:{1}上存在多源复制".format(
                self.mysql_host, self.mysql_port
            ))
            return False
        if len(slave_status_list) > 1:
            logger.warning("从库服务器{0}:{1}上无复制".format(
                self.mysql_host, self.mysql_port
            ))
            return False
        slave_status = slave_status_list[0]
        if not (self.is_string_equal(slave_status["Master_Host"], master_server.mysql_host)):
            logger.warning("从库服务器{0}:{1}上复制配置信息与主库地址不服".format(
                self.mysql_host, self.mysql_port
            ))
            return False
        if not (self.is_string_equal(slave_status["Auto_Position"], "1")):
            logger.warning("从库服务器{0}:{1}上复制未采用GTID模式".format(
                self.mysql_host, self.mysql_port
            ))
            return False
        if int(slave_status["Auto_Position"] > BaseConfig.MAX_SLAVE_DELAY_SECONDS):
            logger.warning("从库服务器{0}:{1}上复制存在严重延迟，延迟时间：{2}秒".format(
                self.mysql_host, self.mysql_port, slave_status["Auto_Position"]
            ))

        slave_gtid_set = master_server.get_mysql_server().get_executed_gtid_set()
        master_gtid_set = master_server.get_mysql_server().get_executed_gtid_set()
        if not GtidHelper.is_included(max_set=master_gtid_set, min_set=slave_gtid_set):
            logger.warning("从库服务器{0}:{1}上GTID集合与主库不兼容。".format(
                self.mysql_host, self.mysql_port, slave_status["Auto_Position"]
            ))
        logger.info("从服务器{}:{}通过检查".format(self.mysql_host, self.mysql_port))
        return True

    def reset_slave(self):
        mysql_server =self.get_mysql_server()
        mysql_server.mysql_exec("STOP SLAVE;")
        mysql_server.mysql_exec("RESET SLAVE ALL;")
