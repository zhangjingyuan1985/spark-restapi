#! /usr/bin/python
# -*- coding:utf-8 -*-

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
sys.path.append("../../lib")

import filter_lib
import hbase_lib

USER_OK = 1
NO_USER = 2
NO_IDENTIFIER = 3
BAD_IDENTIFIER = 4
NO_PASSWORD = 5
BAD_PASSWORD = 6

def get_users(hbase):
    users = []

    rows = hbase.get_rows('livy_users')
    for row in rows:
        name = row.cells['identity:identifier'].value
        pwd = row.cells['identity:password'].value
        users.append((name, pwd))
        # sessions = row.cells['sessions:livy'].value

    return users


def has_user(hbase, identifier):
    filter = filter_lib.scanner_filter(filter_lib.value_filter(identifier))
    rows = hbase.filter('livy_users', filter)

    return len(rows) > 0 and 'identity:name' in rows[0].cells and rows[0].cells['identity:name'] == identifier


def create_user(hbase, identifier, password):

    print("before> exiting users")
    users = get_users(hbase)
    for u in users:
        print(str(u))

    result = True

    key = hbase.get_unique_id("livy_keys", "user")
    try:
        hbase.add_row('livy_users', key, { 'identity:identifier': identifier, 'identity:password': password } )
    except:
        result = False
        print("error ???")

    print("after> exiting users")
    users = get_users(hbase)
    for u in users:
        print(str(u))

    return result

def connect_to_user(hbase, identifier, password):
    filter = filter_lib.scanner_filter(filter_lib.value_filter(identifier))
    rows = hbase.filter('livy_users', filter)

    status = USER_OK
    if len(rows) == 0:
        # print("no user")
        status = NO_USER
    else:
        row = rows[0]
        if not 'identity:identifier' in row.cells:
            # print("no identifier")
            status = NO_IDENTIFIER
        else:
            # print("cells: keys: {}".format(str(row.cells.keys())))
            cell = row.cells['identity:identifier']
            # print("identifier cell: {}".format(str(cell)))
            if cell.value != identifier:
               # print("bad identifier {}".format(cell.value))
               status = BAD_IDENTIFIER
            else:
                if not 'identity:password' in row.cells:
                    # print("no password")
                    status = NO_PASSWORD
                else:
                    cell = row.cells['identity:password']
                    # print("password cell: {}".format(str(cell)))
                    if cell.value != password:
                        # print("bad password")
                        status = BAD_PASSWORD
                    else:
                        status = USER_OK

    return status


