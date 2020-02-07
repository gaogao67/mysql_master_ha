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
import os, datetime, time
import pymysql
import traceback
from logger_helper import LoggerHelper

logger = LoggerHelper.get_logger()


class MySQLServer(object):
    def __init__(self, mysql_host,
                 mysql_user,
                 mysql_password,
                 mysql_port=3306,
                 database_name="mysql_asset",
                 mysql_charset="utf8",
                 connect_timeout=60):
        self.mysql_host = mysql_host
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_port = mysql_port
        self.connect_timeout = connect_timeout
        self.mysql_charset = mysql_charset
        self.database_name = database_name

    def get_connection(self, return_dict=False):
        """
        获取当前服务器的MySQL连接
        :return:
        """
        if return_dict:
            conn = pymysql.connect(
                host=self.mysql_host,
                user=self.mysql_user,
                passwd=self.mysql_password,
                port=self.mysql_port,
                connect_timeout=self.connect_timeout,
                charset=self.mysql_charset,
                db=self.database_name,
                cursorclass=pymysql.cursors.DictCursor
            )
        else:
            conn = pymysql.connect(
                host=self.mysql_host,
                user=self.mysql_user,
                passwd=self.mysql_password,
                port=self.mysql_port,
                connect_timeout=self.connect_timeout,
                charset=self.mysql_charset,
                db=self.database_name,
                cursorclass=pymysql.cursors.Cursor
            )

        return conn

    def mysql_exec(self, sql_script, sql_paras=None):
        conn = None
        cursor = None
        try:
            message = "在服务器{0}上执行脚本:{1},参数为:{2}".format(
                self.mysql_host, sql_script, str(sql_paras))
            logger.debug(message)
            conn = self.get_connection()
            cursor = conn.cursor()
            if sql_paras is not None:
                cursor.execute(sql_script, sql_paras)
            else:
                cursor.execute(sql_script)
            conn.commit()
        except Exception as ex:
            warning_message = """
            execute script:{mysql_script}
            execute paras:{mysql_paras},
            execute exception:{mysql_exception}
            execute traceback:{mysql_traceback}
            """.format(
                mysql_script=sql_script,
                mysql_paras=str(sql_paras),
                mysql_exception=str(ex),
                mysql_traceback=traceback.format_exc()
            )
            logger.warning(warning_message)
            raise Exception(str(ex))
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

    def mysql_exec_many(self, script_items):
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            for script_item in script_items:
                sql_script, sql_paras = script_item
                message = "在服务器{0}上执行脚本:{1},参数为:{2}".format(
                    self.mysql_host, sql_script, str(sql_paras))
                logger.debug(message)
                if sql_paras is not None:
                    cursor.execute(sql_script, sql_paras)
                else:
                    cursor.execute(sql_script)
            conn.commit()
        except Exception as ex:
            logger.warning("execute exception:{0} \n {1}".format(str(ex), traceback.format_exc()))
            raise Exception(str(ex))
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

    def mysql_query(self, sql_script, sql_paras=None, return_dict=False):
        conn = None
        cursor = None
        try:
            message = "在服务器{0}上执行脚本:{1},参数为:{2}".format(
                self.mysql_host, sql_script, str(sql_paras))
            logger.debug(message)
            conn = self.get_connection(return_dict=return_dict)
            cursor = conn.cursor()
            if sql_paras is not None:
                cursor.execute(sql_script, sql_paras)
            else:
                cursor.execute(sql_script)
            exec_result = cursor.fetchall()
            conn.commit()
            return exec_result
        except Exception as ex:
            warning_message = """
execute script:{mysql_script}
execute paras:{mysql_paras},
execute exception:{mysql_exception}
execute traceback:{mysql_traceback}
""".format(
                mysql_script=sql_script,
                mysql_paras=str(sql_paras),
                mysql_exception=str(ex),
                mysql_traceback=traceback.format_exc()
            )
            logger.warning(warning_message)
            raise Exception(str(ex))
        finally:
            if cursor is not None:
                cursor.close()
            if conn is not None:
                conn.close()

    def get_mysql_version(self):
        mysql_version = self.mysql_query(
            sql_script="select @@version;"
        )[0][0]
        return str(mysql_version).replace('-log', '')

    def is_mariadb(self):
        mysql_version = self.get_mysql_version()
        if str(mysql_version).lower().find("mariadb") > 0:
            return True
        else:
            return False

    def get_global_variable_value(self, variable_name):
        """
        获取指定全局变量的变量值
        :param variable_name:
        :return:
        变量值或None
        """
        sql_script = "show global variables where variable_name='{0}';".format(
            variable_name)
        query_result = self.mysql_query(sql_script=sql_script,
                                        sql_paras=None)
        if len(query_result) > 0:
            return query_result[0][1]
        else:
            return None

    def get_global_status_value(self, status_name):
        """
        获取指定全局变量的变量值
        :param status_name:
        :return:
        变量值或None
        """
        sql_script = "show global status where variable_name='{0}';".format(
            status_name)
        query_result = self.mysql_query(sql_script=sql_script,
                                        sql_paras=None)
        if len(query_result) > 0:
            return query_result[0][1]
        else:
            return None

    def get_slave_status(self):
        """
        获取该服务器上slave的状态
        :return:
        使用dict来存放每条slave status，返回所有slave的状态集合list
        """
        if self.is_mariadb():
            sql_script = "show all slave status;"
        else:
            sql_script = "show slave status;"
        return self.mysql_query(sql_script=sql_script,
                                return_dict=True)

    def get_master_status(self):
        """
        获取当前master status信息
        :return:
        """
        sql_script = "show master status;"
        return self.mysql_query(sql_script=sql_script,
                                return_dict=True)

    def is_read_only(self):
        variable_name = self.get_global_variable_value(variable_name='read_only')
        if str(variable_name).strip().upper() == 'OFF':
            return False
        else:
            return True

    def ping_server(self):
        try:
            conn = self.get_connection(return_dict=True)
            conn.ping(reconnect=False)
            return True
        except Exception as ex:
            logger.warning("服务器{}:{}无法正常ping，异常为：{}".format(
                self.mysql_host, self.mysql_port, str(ex)
            ))
            return False

    def get_executed_gtid_set(self):
        return self.get_master_status()[0]["Executed_Gtid_Set"]
