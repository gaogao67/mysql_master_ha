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

class GtidHelper(object):
    @staticmethod
    def split_gtid_set(gtid_set):
        """
        将gtid set转换成gtid list,
        如将5aeb6d6a-45a1-11ea-8a7e-080027b9d8ca:1-4,e0a86c29-f20d-11e8-93c2-04b0e7954a65:10410:104934:104936-104938
        拆分为：[('5aeb6d6a-45a1-11ea-8a7e-080027b9d8ca', 1, 4),
        ('e0a86c29-f20d-11e8-93c2-04b0e7954a65', 10410, 10410),
        ('e0a86c29-f20d-11e8-93c2-04b0e7954a65', 104934, 104934),
        ('e0a86c29-f20d-11e8-93c2-04b0e7954a65', 104936, 104938)]
        :param gtid_set:
        :return:
        """
        gtid_list = list()
        for gtid_item in str(gtid_set).split(","):
            gtid_item = str(gtid_item).replace("\n", "").strip()
            tmp_list = gtid_item.split(":")
            server_id = tmp_list[0]
            for index in range(1, len(tmp_list)):
                id_range = tmp_list[index]
                if id_range.find("-") < 0:
                    min_id = id_range
                    max_id = id_range
                else:
                    min_id = id_range.split("-")[0]
                    max_id = id_range.split("-")[1]
                gtid_list.append((server_id, int(min_id), int(max_id)))
        return gtid_list

    @staticmethod
    def is_included(max_set, min_set, ignore_server_uuid=None):
        """
        比对两个gtid-set,如果gtid_set1包含gtid_set2,返回True，否则返回Falase
        如果指定ignore_server_uuid，则忽略掉指定的服务器的gtid
        :param max_set:
        :param min_set:
        :param ignore_server_uuid:
        :return:
        """
        gtid_list1 = GtidHelper.split_gtid_set(max_set)
        gtid_list2 = GtidHelper.split_gtid_set(min_set)
        for item2 in gtid_list2:
            if (ignore_server_uuid is None) and (item2[0] == ignore_server_uuid):
                continue
            find_flag = False
            for item1 in gtid_list1:
                if item2[0] == item1[0] and item2[1] >= item1[1] and item2[2] <= item1[2]:
                    find_flag = True
                    break
            if not find_flag:
                return False
        return True


# def test():
#     gtid_set1 = """
# 5aeb6d6a-45a1-11ea-8a7e-080027b9d8ca:1-4,
# e0a86c29-f20d-11e8-93c2-04b0e7954a65:10410:104934:104936-104938
# """
#     gtid_set2 = """
#     5aeb6d6a-45a1-11ea-8a7e-080027b9d8ca:1-3,
#     e0a86c29-f20d-11e8-93c2-04b0e7954a65:10410:104934:104936-104938
#     """
#     is_include = GtidHelper.is_gtid_set_included(gtid_set1, gtid_set2)
#     print(is_include)
#
#
# if __name__ == '__main__':
#     test()
