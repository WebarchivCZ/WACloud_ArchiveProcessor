#!/usr/bin/python
# coding: utf-8
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "../src"))

from pprint import pprint
from config import *
from HBase import HBase

hb = HBase(HBASE_HOST, HBASE_PORT)

# CHECK TABLES
# hb.check_tables()

# UPDATE CONFIG TABLE WITH NEW SETTINGS
# hb.put_rows_from_json_file(HBASE_CONF_TABLE, "TopicIdentification.json")
# hb.put_rows_from_json_file(HBASE_CONF_TABLE, "WebPageTypeIdentification.json")

# PRINT HBASE RECORDS:
# for key, data in hb.get_rows_by_prefix(HBASE_MAIN_TABLE, ""):
    # print(key)
    # pprint(data)
    # print()
# for key, data in hb.get_rows_by_prefix(HBASE_PROC_TABLE, ""):
    # print(key)
    # pprint(data)
    # print()
# for key, data in hb.get_rows_by_prefix(HBASE_HARV_TABLE, ""):
    # print(key)
    # pprint(data)
    # print()
for key, data in hb.get_rows_by_prefix(HBASE_CONF_TABLE, "", include_timestamp=True):
    print(key)
    pprint(data)
    print()
# for key, data in hb.get_cell_versions(HBASE_CONF_TABLE, "topics", "cf1:value", include_timestamp=True):
    # print(key)
    # pprint(data)
    # print()

# DELETE TABLES:
# hb.delete_table(HBASE_CONF_TABLE)
# hb.delete_table(HBASE_MAIN_TABLE)
# hb.delete_table(HBASE_HARV_TABLE)
# hb.delete_table(HBASE_PROC_TABLE)

hb.close()
