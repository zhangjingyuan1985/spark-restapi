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



from pylivy.session import *
from pylivy.client import *
import sys


"""
Demo of using the pylivy library

https://pylivy.readthedocs.io/en/latest/index.web

"""

LIVY_URL = "http://vm-75222.lal.in2p3.fr:21111"

client = LivyClient(LIVY_URL)

print("List all sessions")
sessions = client.list_sessions()
for session in sessions:
    print(session.session_id, session.state)
    for s in client.list_statements(session.session_id):
        print(" statement ", s)



for n, arg in enumerate(sys.argv):
    if n == 0:
        continue
    print(n, arg)
    print("Delete session ", arg)
    try:
        client.delete_session(arg)
    except:
        print("error killing session ", arg)
