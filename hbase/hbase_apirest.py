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

    version = hbase.get_version()
    print('================== Show version')
    for a in version:
        print(a, "=", version[a])

    print('================== get_namespaces')
    namespaces = hbase.get_namespaces()
    print(', '.join(namespaces))

    print('================== create table')
    hbase.create_table("A", ['position', 'vitesse'])
    cols = hbase.get_schema("A")
    print(', '.join(cols))

    print('================== get_schema')
    families = hbase.get_schema("A")
    print(', '.join(families))

    print('================== get_regions')
    regions = hbase.get_regions("A")
    print(', '.join(regions))

    print('================== get_row')
    hbase.get_row("A", "p1")

    print('================== get_rows')
    hbase.get_rows("A")

    print('================== create table')
    hbase.create_table("B", ['position'])
    families = hbase.get_schema("B")
    print(', '.join(families))

    for r in range(5):
        hbase.add_row('B', 'r{}'.format(r), {'position:x': random.random(),
                                             'position:y': random.random(),
                                             'position:z': random.random()})

    hbase.get_rows("B")

    print('================== get_tables')
    tables = hbase.get_tables()
    print(', '.join(tables))

    print('================== delete table')
    hbase.delete_table("A")
    hbase.delete_table("B")

    print('================== get_tables')
    tables = hbase.get_tables()
    print(', '.join(tables))


if __name__ == "__main__":
    main()
