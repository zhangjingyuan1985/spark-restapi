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

import sys, os

sys.path.append("d:/workspace/pylivy/")
sys.path.append("../../lib")

from livy.session import *
from livy.client import *
import time
from threading import Thread, Lock

from hbase_lib import *
from gateway import *

import json

"""
Demo of using the pylivy library

https://pylivy.readthedocs.io/en/latest/index.web

"""

mutex = Lock()

def add_user(hbase, user):

    with mutex:
        print("add_user> {}".format(user))
        while True:
            filter = scanner_filter(value_filter(user))
            rows = hbase.filter('livy_users', filter)
            found = False
            for row in rows:
                print(str(row))

                for kcell in row.cells:
                    cell = row.cells[kcell]
                    value = cell.value
                    if value == user:
                        print("user {} found".format(user))
                        found = True
                        row = hbase.get_row('livy_users', row.key)
                        return row

            print("user {} was not found, create it".format(user))
            key = hbase.get_unique_id('livy_keys', 'user')
            hbase.add_row('livy_users', key, {'identity:name': user, 'sessions:livy': '[]'})
            row = hbase.get_row('livy_users', key)
            return row

class SessionThread(Thread):
#class SessionThread():
    def __init__(self, hbase, user_name, livy_client):
        super(SessionThread, self).__init__()
        self.user_name = user_name
        self.hbase = hbase
        self.session_id = None
        self.livy_client = livy_client

    def run(self):
        """
        Launch a session
        Send a asynchronous event when core action is completed
        """

        session_id = None

        all_sessions = []

        row = add_user(self.hbase, self.user_name)

        print("--- starting the thread.\n--- we start by checking the user record\n", str(row))
        print('------------------------')

        # Check if a livy session is already registered for this user
        key = row.key
        if 'sessions:livy' in row.cells:
            cell = row.cells['sessions:livy']
            print("Check livy sessions for the user row key = {} value = {}".format(key, cell.value))
            all_sessions = json.loads(cell.value)
            if len(all_sessions) > 0:
                print("There is already at least one livy session opened- We reuse it {}".format(', '.join([str(s) for s in all_sessions])))
                session_id = all_sessions[0]
            else:
                print('this user has no session registered')
        else:
            print('this user has no session registered')

        if session_id is None:
            print("Create a session for {}".format(self.user_name))
            s = self.livy_client.create_session(SessionKind.PYSPARK)
            print("=> session ", s.session_id)

            with mutex:
                print("Add the session id {} to the hbase record of {}".format(s.session_id, self.user_name))
                all_sessions.append(s.session_id)
                self.hbase.add_row('livy_users',
                                   row.key,
                                   {'identity:name': self.user_name,
                                    'sessions:livy': json.dumps(all_sessions)} )
        else:
            print("re-connect to a session for {}".format(self.user_name))
            s = self.livy_client.get_session(session_id)


        print("............... wait until idle ....................")
        while True:
            s = self.livy_client.get_session(s.session_id)
            if s.state == SessionState.IDLE:
                break

        for i in range(5):
            code = "3 + 7 + {}".format(i)

            print("{}> Execute spark statement {}".format(self.user_name, code))

            st = self.livy_client.create_statement(s.session_id, code)

            print("{}> ... wait until available ...".format(self.user_name))
            result = ""
            while True:
                st = self.livy_client.get_statement(s.session_id, st.statement_id)
                if st.state == StatementState.AVAILABLE:
                    try:
                        result = st.output.text
                        print("{}> result = {}".format(self.user_name, result))
                    except:
                        print("???")
                    break

            """
            if i == 5:
                exit()
            """

            time.sleep(1)

        print("Delete session ", s.session_id)
        self.livy_client.delete_session(s.session_id)

        with mutex:
            print("Remove the session id {} from the hbase record of {}".format(s.session_id, self.user_name))
            all_sessions.remove(s.session_id)
            self.hbase.add_row('livy_users',
                               row.key,
                               {'identity:name': self.user_name,
                                'sessions:livy': json.dumps(all_sessions)})

def main():
    url, auth = gateway_url('livy/v1')

    hbase = HBase()

    dtb_users = hbase.create_table('livy_users', ['identity', 'sessions'])

    print("show the DTB of users")
    rows = hbase.get_rows('livy_users')
    for row in rows:
        print(str(row))

    print("--------------------------------------")

    """
    print("direct execution of a statement ")
    with LivySession(url, auth=auth, verify_ssl=False) as session:
        session.run("2+3")
    """

    livy_client = LivyClient(url, auth=auth, verify_ssl=False)

    print("List all sessions")
    sessions = livy_client.list_sessions()
    for session in sessions:
        print(session.session_id, session.state)
        if session.state == SessionState.IDLE:
            for s in livy_client.list_statements(session.session_id):
                print(" statement ", s)

    print("--------------------------------------")

    threads = []

    users = ['John', 'Paul', 'Jack']

    for user in users:
        st = SessionThread(hbase, user, livy_client)
        threads.append(st)
        st.start()
        # st.run()

    for st in threads:
        st.join()

    rows = hbase.get_rows('livy_users')
    for row in rows:
        print(str(row))

    print('all done')

if __name__ == "__main__":
    main()
