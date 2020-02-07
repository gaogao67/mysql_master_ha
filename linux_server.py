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
import paramiko
import traceback
from logger_helper import LoggerHelper

MAX_CONNECT_TIMEOUT = 3
logger = LoggerHelper.get_logger()


class LinuxServer(object):
    def __init__(self, ssh_host, ssh_port, ssh_user, ssh_password):
        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_user = ssh_user
        self.ssh_password = ssh_password

    def ssh_connection_check(self):
        echo_flag = '====ssh==check===='
        cmd_output, cmd_error = self.remote_exec_cmd(cmd="echo '{0}'".format(echo_flag))
        if cmd_output is None:
            return False
        cmd_result = "\n".join(cmd_output)
        if cmd_result.find(echo_flag) >= 0:
            return True
        else:
            return False

    def remote_exec_cmd(self, cmd):
        ssh_client = None
        try:
            logger.debug("在服务器{}上远程执行命令:\n{}".format(self.ssh_host, cmd))
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self.ssh_host,
                               port=self.ssh_port,
                               username=self.ssh_user,
                               password=self.ssh_password,
                               timeout=MAX_CONNECT_TIMEOUT)
            stdin, stdout, stderr = ssh_client.exec_command(cmd)
            cmd_output = list()
            cmd_error = list()
            for line in stdout.readlines():
                cmd_output.append(str(line))
            for line in stderr.readlines():
                cmd_error.append(str(line))
            logger.debug("在服务器{}上远程执行命令:\n{},\n执行结果：\n{},\n错误信息：\n{}\n".format(
                self.ssh_host,
                cmd,
                "\n".join(cmd_output),
                "\n".join(cmd_error)
            ))
            return cmd_output, cmd_error
        except Exception as ex:
            error_message = """
ssh_host:{ssh_host}
ssh_port:{ssh_port}
ssh_cmd:{ssh_cmd}
exception:{exception}
traceback:{traceback}
""".format(
                ssh_host=self.ssh_host,
                ssh_port=self.ssh_port,
                ssh_cmd=cmd,
                exception=str(ex),
                traceback=traceback.format_exc()
            )
            logger.warning(error_message)
            return None, None
        finally:
            if ssh_client is not None:
                ssh_client.close()

    def copy_file_to_remote(self, local_file_path, remote_file_path):
        try:
            tp = paramiko.Transport(self.ssh_host, self.ssh_port)
            tp.connect(username=self.ssh_user, password=self.ssh_password)
            sftp = paramiko.SFTPClient.from_transport(tp)
            sftp.put(local_file_path, remote_file_path)
            tp.close()
            return
        except Exception as ex:
            logger.warning("Exception:\n{0},\nTraceback:\n{1}".format(
                str(ex),
                traceback.format_exc()
            ))
            return False
