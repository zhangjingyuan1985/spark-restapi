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
from threading import Thread

from hbase_lib import *
from gateway import *

"""
Demo of using the pylivy library

https://pylivy.readthedocs.io/en/latest/index.web

"""


class SessionThread(Thread):
    def __init__(self, hbase, name, livy_client):
        super(SessionThread, self).__init__()
        self.name = name
        self.hbase = hbase
        self.session_id = None
        self.livy_client = livy_client

    def run(self):
        """
        Launch a session
        Send a asynchronous event when core action is completed
        """
        print("Create a session for {}".format(self.name))
        s = self.livy_client.create_session(SessionKind.PYSPARK)
        print("=> session ", s.session_id)

        print("... wait until idle ...")
        while True:
            s = self.livy_client.get_session(s.session_id)
            if s.state == SessionState.IDLE:
                break

        for i in range(10):
            code = "3 + 7 + {}".format(i)

            print("{}> Execute spark statement {}".format(self.name, code))

            st = self.livy_client.create_statement(s.session_id, code)

            print("{}> ... wait until available ...".format(self.name))
            result = ""
            while True:
                st = self.livy_client.get_statement(s.session_id, st.statement_id)
                if st.state == StatementState.AVAILABLE:
                    result = st.output.text
                    print("{}> result = {}".format(self.name, result))
                    break

            time.sleep(1)

        print("Delete session ", s.session_id)
        self.livy_client.delete_session(s.session_id)

def main():
    url, auth = gateway_url('livy/v1')

    hbase = HBase()

    dtb_users = hbase.create_table('livy_users', ['identity'])
    dtb_sessions = hbase.create_table('livy_sessions', ['sessions'])

    print("direct execution of a statement ")
    with LivySession(url, auth=auth, verify_ssl=False) as session:
        session.run("2+3")

    livy_client = LivyClient(url, auth=auth, verify_ssl=False)

    print("List all sessions")
    sessions = livy_client.list_sessions()
    for session in sessions:
        print(session.session_id, session.state)
        if session.state == SessionState.IDLE:
            for s in livy_client.list_statements(session.session_id):
                print(" statement ", s)

    threads = []

    users = ['John', 'Paul', 'Jack']

    for user in users:
        key = hbase.get_unique_id('livy_keys', 'user')
        hbase.add_row('livy_users', key, {'name': user})

    for user in users:
        key = hbase.get_unique_id('livy_keys', 'user')
        hbase.add_row('livy_users', key, {'name': user})
        st = SessionThread(hbase, user, livy_client)
        threads.append(st)
        st.start()

    for st in threads:
        st.join()

    print('all done')

if __name__ == "__main__":
    main()
