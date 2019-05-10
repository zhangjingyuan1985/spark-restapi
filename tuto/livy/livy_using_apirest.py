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
sys.path.append('../../lib')

import requests
import json
from gateway import *

class Livy:
    def __init__(self):
        self.host, self.auth = gateway_url('livy/v1')

    def get_sessions(self):
        """
        Ask the Livy server about currently open sessions

        :return: The list of session ides

        """

        ids=dict()
        r = requests.get(self.host + '/sessions', auth = self.auth, verify = False)
        answer = r.json()
        for session in answer['sessions']:
            ids[int(session['id'])] = session['state']

        return ids

    def create_session(self):
        """
        Open a new pyspark Livy session and wait until it enters to the idle state

        :return: the new session id
        """
        headers = {'Content-Type': 'application/json'}
        data = {'kind': 'pyspark'}
        r = requests.post(self.host + '/sessions', data=json.dumps(data), headers=headers,
                          auth = self.auth, verify = False)
        answer = r.json()
        # print("JSON:", answer)

        id = int(answer['id'])

        while (True):
            session_url = self.host + r.headers['location']
            r2 = requests.get(session_url, headers=headers, auth = self.auth, verify = False)
            answer = r2.json()

            # print(answer)

            state = answer['state']

            if state == 'idle':
                break

        return id

    def execute_statement(self, id, statement):
        """
        Execute a Spark statement within the context of a Livy session.
        It then waits until the statement executes and enters the "available" state

        :param id:
        :param statement:
        :return:
        """
        headers = {'Content-Type': 'application/json'}
        data = {'code': statement}
        request = self.host + "/sessions/{}/statements".format(str(id))

        r = requests.post(request, data=json.dumps(data), headers=headers, auth = self.auth, verify = False)

        answer = r.json()

        # print("statement started ", answer)

        result = ""

        while (True):
            session_url = self.host + r.headers['location']
            r2 = requests.get(session_url, headers=headers, auth = self.auth, verify = False)
            answer = r2.json()

            # print(answer)

            state = answer['state']

            if state == 'available':
                output = answer['output']
                result = output['data']['text/plain']
                break

        return result

    def delete_session(self, id):
        """
        Delete an existing Livy session
        :param id:
        """
        r = requests.delete(self.host + '/sessions/{}'.format(id), auth = self.auth, verify = False)

def main():

    livy = new Livy()

    print('Show all session ids')
    sessions = livy.get_sessions()
    print(' - '.join(["session=({}, {})". format(str(id), str(sessions[id])) for id in sessions]))

    print('Create a new session')
    myid = livy.create_session()
    print("Session id ", myid)

    result = livy.execute_statement(myid, "1 + 2")

    print("Result = ", result)

    print('Delete it')
    print(livy.delete_session(myid))

    print('Show all session ids')
    sessions = livy.get_sessions()
    print(' - '.join(["session=({}, {})". format(str(id), str(sessions[id])) for id in sessions]))


if __name__ == "__main__":
    main()
