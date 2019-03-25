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


"""
Demo of using the pylivy library

https://pylivy.readthedocs.io/en/latest/index.html

"""

LIVY_URL = "http://vm-75222.lal.in2p3.fr:21111"

print("direct execution of a statement ")
with LivySession(LIVY_URL) as session:
    session.run("2+3")

client = LivyClient(LIVY_URL)

print("List all sessions")
sessions = client.list_sessions()
for session in sessions:
    print(session.session_id, session.state)
    for s in client.list_statements(session.session_id):
        print(" statement ", s)

print("Create a session ")
s = client.create_session(SessionKind.PYSPARK)
print("=> session ", s.session_id)

print("... wait until idle ...")
while True:
    s = client.get_session(s.session_id)
    if s.state == SessionState.IDLE:
        break

code = "3 + 7"

print("Execute spark statement ", code)

st = client.create_statement(s.session_id, code)

print("... wait until available ...")
result = ""
while True:
    st = client.get_statement(s.session_id, st.statement_id)
    if st.state == StatementState.AVAILABLE:
        result = st.output.text
        break

print("result = ", result)

print("Delete session ", s.session_id)
client.delete_session(s.session_id)

