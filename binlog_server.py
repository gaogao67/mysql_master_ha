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
import codecs
from base_config import BaseConfig
from base_server import BaseServer
from linux_server import LinuxServer
from logger_helper import LoggerHelper

logger = LoggerHelper.get_logger()


class BinlogServer(BaseServer):
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
        super(BinlogServer, self).__init__(
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

    def check_server(self, master_server):
        if not self.check_ssh_connection():
            logger.warning("在服务器{}无法正常SSH".format(self.mysql_host))
            return
        if not self.check_file_exist(self.mysql_exe_path):
            logger.warning("在服务器{}上未检测到执行文件{}".format(self.mysql_host, self.mysql_exe_path))
            return False
        if not self.check_file_exist(self.binlog_exe_path):
            logger.warning("在服务器{}上未检测到执行文件{}".format(self.mysql_host, self.binlog_exe_path))
            return False
        if not self.check_dir_exist(self.working_dir):
            logger.warning("在服务器{}上未检测到目录{}".format(self.mysql_host, self.working_dir))
            return False
        if len(self.get_binlog_files()) == 0:
            logger.warning("在服务器{}上未检测到binlog文件".format(self.mysql_host))
            return False
        return True

    def get_file_length(self, file_path):
        try:
            linux_server = self.get_linux_server()
            cmd = """/bin/wc -c < '{file_path}'""".format(file_path=file_path)
            cmd_output, cmd_error = linux_server.remote_exec_cmd(cmd=cmd)
            logger.info("文件{0}的长度为:{1}".format(file_path, "\n".join(cmd_output)))
            file_length = int(str("\n".join(cmd_output)).replace("\n", ""))
            return file_length
        except Exception as ex:
            logger.warning("获取服务器{0}上文件{1}的大小失败".format(self.mysql_host, file_path))
            logger.warning("Exception:{0}\nTraceBack:{1}".format(str(ex), traceback.format_exc()))
            return None

    def get_max_binlog_pos(self):
        binlog_files = self.get_binlog_files()
        if len(binlog_files) == 0:
            return None, None
        max_binlog_file = binlog_files[-1]["binlog_file"]
        file_path = os.path.join(self.binlog_dir, max_binlog_file)
        max_binlog_pos = self.get_file_length(file_path=file_path)
        if max_binlog_pos is None:
            logger.warning("获取BINLOG服务器{0}的最大位点失败".format(
                self.mysql_host
            ))
            return None, None, None
        else:
            logger.warning("获取BINLOG服务器{0}的最大位点:{1},{2}".format(
                self.mysql_host,
                max_binlog_file,
                max_binlog_pos,
            ))
            max_binlog_index = self.get_binlog_file_index(max_binlog_file)
            return max_binlog_file, max_binlog_index, max_binlog_pos

    def get_binlog_files(self):
        cmd = """
            cd '{}'
            /bin/ls {}.*
            """.format(self.binlog_dir, self.mysql_binlog_prefix)
        cmd_output, cmd_error = self.get_linux_server().remote_exec_cmd(cmd=cmd)
        binlog_files = list()
        for cmd_line in cmd_output:
            if str(cmd_line).find(self.mysql_binlog_prefix) >= 0:
                binlog_file = str(cmd_line).strip()
                binlog_index = binlog_file.split(".")[-1]
                if binlog_index.find("index") >= 0:
                    continue
                else:
                    binlog_files.append({
                        "binlog_file": binlog_file,
                        "binlog_index": int(binlog_index),
                        "binlog_full_path": os.path.join(self.binlog_dir, binlog_file)
                    })
        return list(sorted(binlog_files, key=lambda item: item["binlog_index"]))

    def get_diff_binlog_files(self, start_binlog_file, start_binlog_post):
        start_binlog_index = self.get_binlog_file_index(start_binlog_file)
        save_binlog_files = list()
        for binlog_file in self.get_binlog_files():
            if binlog_file["binlog_index"] > start_binlog_index:
                save_binlog_files.append(
                    {
                        "binlog_file": binlog_file["binlog_file"],
                        "binlog_index": binlog_file["binlog_index"],
                        "binlog_full_path": binlog_file["binlog_full_path"],
                        "binlog_post": -1,
                    }
                )
            elif binlog_file["binlog_index"] == start_binlog_index:
                save_binlog_files.append(
                    {
                        "binlog_file": binlog_file["binlog_file"],
                        "binlog_index": binlog_file["binlog_index"],
                        "binlog_full_path": binlog_file["binlog_full_path"],
                        "binlog_post": start_binlog_post,
                    }
                )
        return list(sorted(save_binlog_files, key=lambda item: item["binlog_index"]))

    def sync_binlog_to_new_master(self, new_master):
        try:
            self.try_sync_binlog_to_new_master(new_master)
        except Exception as ex:
            logger.warning("""从BINLOG服务器{}同步数据至前新主服务器{}:{}出现异常，异常为：\n{},堆栈：\n{}""".format(
                self.mysql_host,
                new_master.mysql_host, new_master.mysql_port,
                str(ex),
                traceback.format_exc()
            ))

    def try_sync_binlog_to_new_master(self, new_master):
        start_binlog_file, start_binlog_index, start_binlog_post = new_master.get_max_binlog_pos()
        max_binlog_file, max_binlog_index, max_binlog_pos = self.get_max_binlog_pos()
        if not self.is_bigger_pos(
                base_binlog_index=start_binlog_index,
                base_binlog_pos=start_binlog_index,
                source_binlog_index=max_binlog_index,
                source_binlog_pos=max_binlog_pos
        ):
            logger.warning("""当前BINLOG服务器{}最大位点为：{},{}
当前新主服务器{}:{}最大位点为：{},{}，无需同步""".format(
                self.mysql_host, max_binlog_file, max_binlog_pos,
                new_master.mysql_host, new_master.mysql_port,
                start_binlog_index, start_binlog_index))
            return True
        remote_script_file = self.create_sync_script_file(
            new_master,
            start_binlog_file,
            start_binlog_post)
        cmd = "/bin/bash '{remote_script_file}'".format(remote_script_file=remote_script_file)
        logger.warning("在服务器{0}上远程执行脚本：{1}".format(self.mysql_host, remote_script_file))
        cmd_output, cmd_error = self.get_linux_server().remote_exec_cmd(cmd=cmd)
        for cmd_line in cmd_output:
            if str(cmd_line).find("SESSION.GTID_NEXT"):
                last_gtid = str(cmd_line).strip("'")[1]
                logger.warning("从BINLOG服务器上同步的最后GTID为：{}".format(last_gtid))

    def get_dump_binlog_script(self, start_binlog_file, start_binlog_post, ha_binlog_file, ha_log_file):
        cmd_list = []
        for binlog_file in self.get_diff_binlog_files(start_binlog_file, start_binlog_post):
            logger.warning("start sync binlog file:{binlog_file} >>'{ha_log_file}'".format(
                binlog_file=binlog_file["binlog_file"],
                ha_log_file=ha_log_file
            ))
            if binlog_file["binlog_post"] >= 0:
                cmd = """('{binlog_exe_path}' --start-position={binlog_post} """.format(
                    binlog_exe_path=self.binlog_exe_path,
                    binlog_post=binlog_file["binlog_post"])
            else:
                cmd = """('{binlog_exe_path}' """.format(binlog_exe_path=self.binlog_exe_path)
            cmd += " '{binlog_file}' >>'{ha_binlog_file}') 1>>'{ha_log_file}' 2>&1".format(
                binlog_file=binlog_file["binlog_full_path"],
                ha_binlog_file=ha_binlog_file,
                ha_log_file=ha_log_file
            )
            cmd_list.append(cmd)
        return "\n".join(cmd_list)

    def get_exec_script(self, new_master, ha_binlog_file, ha_log_file):
        return """
(
{mysql_exe_path} \
--host='{mysql_host}' \
--port={mysql_port} \
--user='{mysql_user}' \
--password='{mysql_password}' \
--binary-mode \
-vvv --unbuffered < '{ha_binlog_file}'
) 1>>'{ha_log_file}' 2>&1
""".format(
            mysql_exe_path=self.mysql_exe_path,
            mysql_host=new_master.mysql_host,
            mysql_port=new_master.mysql_port,
            mysql_user=new_master.mysql_user,
            mysql_password=new_master.mysql_password,
            ha_binlog_file=ha_binlog_file,
            ha_log_file=ha_log_file
        )

    def get_local_script_file(self, script_file_name):
        site_dir = os.path.dirname(__file__)
        script_dir = os.path.join(site_dir, 'scripts')
        if not os.path.exists(script_dir):
            os.mkdir(script_dir)
        return os.path.join(script_dir, script_file_name)

    def create_sync_script_file(self, new_master, start_binlog_file,
                                start_binlog_post):
        sync_time_str = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        script_file_name = "{0}_{1}.sh".format(self.mysql_host, sync_time_str)
        local_script_file = self.get_local_script_file(script_file_name)
        remote_script_file = os.path.join(self.working_dir, script_file_name)
        ha_binlog_file = os.path.join(self.working_dir, "ha_{dt_time}.binlog".format(dt_time=sync_time_str))
        ha_log_file = os.path.join(self.working_dir, "ha_{dt_time}.log".format(dt_time=sync_time_str))
        cmd_list = list()
        cmd_list.append("""echo "start save binlog at "`date "+%y-%m-%d %H:%M:%S"` >> '{ha_log_file}'""".format(
            ha_log_file=ha_log_file
        ))
        cmd_list.append(self.get_dump_binlog_script(start_binlog_file, start_binlog_post, ha_binlog_file, ha_log_file))
        cmd_list.append("""echo "start sync binlog at "`date "+%y-%m-%d %H:%M:%S"` >> '{ha_log_file}'""".format(
            ha_log_file=ha_log_file
        ))
        cmd_list.append(self.get_exec_script(new_master, ha_binlog_file, ha_log_file))
        cmd_list.append("""echo "try to dump last GTID at "`date "+%y-%m-%d %H:%M:%S"`   >> '{ha_log_file}'""".format(
            ha_log_file=ha_log_file
        ))
        cmd_list.append("""echo "get last GTID at "`date "+%y-%m-%d %H:%M:%S"`   >> '{ha_log_file}'""".format(
            ha_log_file=ha_log_file
        ))
        cmd_list.append(""" /bin/cat '{ha_log_file}' | grep 'GTID_NEXT' | grep -v 'AUTOMATIC' | tail -n 1 """.format(
            ha_log_file=ha_log_file
        ))
        if os.path.exists(local_script_file):
            os.remove(local_script_file)
        with codecs.open(local_script_file, 'w+', encoding='utf-8') as file_handle:
            file_handle.writelines("\n".join(cmd_list))
            file_handle.close()
        self.get_linux_server().copy_file_to_remote(
            local_script_file, remote_script_file
        )
        return remote_script_file
