#!/usr/bin/python

# coding: utf-8

# Copyright 2018 AstroLab Software
# Author: Chris Arnault
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
sys.path.append('../lib')

from hbase_lib import *
import random

def main():
    hbase = HBase()

    """
    version = hbase.get_version()
    print('================== Show version')
    for a in version:
        print(a, "=", version[a])

    print('================== get_namespaces')
    namespaces = hbase.get_namespaces()
    print(', '.join(namespaces))

    print('================== create table')
    families = hbase.create_table("A", ['position', 'vitesse'], recreate=True)
    print(', '.join(families))

    print('================== get_schema')
    families = hbase.get_schema("A")
    print(', '.join(families))

    print('================== get_regions')
    regions = hbase.get_regions("A")
    print(', '.join(regions))

    print('================== get_row')
    hbase.get_row("A", "p1")

    print('================== get_rows')
    rows = hbase.get_rows("A")
    for row in rows:
        print(rows)

    print('================== create table')
    families = hbase.create_table("B", ['position'], recreate=True)
    print(', '.join(families))

    for r in range(1, 5):
        hbase.add_row('B', 'r{}'.format(r), {'position:x': r,
                                             'position:y': r,
                                             'position:z': r})

    rows = hbase.get_rows("B")
    for row in rows:
        print(str(row))

    
    print('================== get_unique_id')
    for i in range(3):
        id = hbase.get_unique_id("C", "test")
        print('id = ', id)

    """


    print('================== filter')

    f2 = column_prefix_filter("x")
    f1 = value_filter("1")
    filter = scanner_filter(list_filter([f1, f2]))

    rows = hbase.filter('B', filter)
    for row in rows:
        print(str(row))

    print('================== get_tables')
    tables = hbase.get_tables()
    print(', '.join(tables))

    exit()

    print('================== delete table')
    hbase.delete_table("A")
    hbase.delete_table("B")
    hbase.delete_table("C")

    print('================== get_tables')
    tables = hbase.get_tables()
    print(', '.join(tables))


if __name__ == "__main__":
    main()
