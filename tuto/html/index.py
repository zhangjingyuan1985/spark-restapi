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

"""

The index.py script has to be present on the <host> machine

where the minimal HTML server has been activated as

> python server.py

Then, call in a web navigator the URL

http://<host>:24701/index.py

https://python-django.dev/page-python-serveur-web-creer-rapidement
"""

# coding: utf-8

import cgi
from pylivy.session import *
from pylivy.client import *


"""
Demo of using the pylivy library

https://pylivy.readthedocs.io/en/latest/index.web

"""


# Initialize post variables
class Variable:
    def __init__(self, name, type="int"):
        self.name = name
        self.type = type
        self.reset()

    def read(self):
        try:
            if self.type == "int":
                self.value = int(form.getvalue(self.name))
            else:
                value = form.getvalue(self.name)
                if value is None:
                    value = ""
                self.value = value
                pass
        except:
            self.reset()
        pass

    def to_form(self):
        out = """<input type="hidden" name="{}" value="{}" />""".format(self.name, self.value)
        return out

    def debug(self):
        out = " {} = {}\n".format(self.name, self.value)
        return out

    def reset(self):
        if self.type == "int":
            self.value = -1
        else:
            self.value = ""
        pass

    def set(self, value):
        if self.type == "int":
            try:
                self.value = int(value)
            except:
                self.value = -1
        else:
            self.value = value

    def is_set(self):
        if self.type == "int":
            try:
                if self.value >= 0:
                    return True
            except:
                pass
        else:
            try:
                if len(self.value) > 0:
                    return True
            except:
                pass

        return False

    def incr(self):
        if self.type == "int":
            self.value += 1

    def above(self, threshold):
        if self.type == "int":
            try:
                if self.value > threshold:
                    return True
            except:
                pass

        return False


class VariableSet:
    def __init__(self, names, str_names):
        self.base = dict()

        type = "int"

        for name in names:
            if name in str_names:
                type = "str"
            else:
                type = "int"
            self.base[name] = Variable(name, type)

    def variable(self, name):
        return self.base[name]

    def read(self):
        for v in self.base:
            self.base[v].read()

    def to_form(self):
        out = ""
        for v in self.base:
            out += self.base[v].to_form()
        return out

    def debug(self):
        out = ""
        for v in self.base:
            out += self.base[v].debug()
        return out


# ======================================================
LIVY_URL = "http://vm-75222.lal.in2p3.fr:21111"

form = cgi.FieldStorage()
print("Content-type: text/html; charset=utf-8\n")

client = LivyClient(LIVY_URL)

# init data
variables = VariableSet(["start",
                         "simul",
                         "change_simul",
                         "livy_session",
                         "waiting_session",
                         "waiting_statement",
                         "livy_statement",
                         "new_statement",
                         "kill_session",
                         "result"], ["new_statement", "result"])

start = variables.base["start"]
simul = variables.base["simul"]
change_simul = variables.base["change_simul"]
livy_session = variables.base["livy_session"]
waiting_session = variables.base["waiting_session"]
waiting_statement = variables.base["waiting_statement"]
livy_statement = variables.base["livy_statement"]
kill_session = variables.base["kill_session"]
new_statement = variables.base["new_statement"]
result = variables.base["result"]

variables.read()

if not start.is_set():
    simul.set(1)
    start.set(1)


# ======================================================

html = """
<!DOCTYPE html>
<head>
    <link rel="stylesheet" type="text/css" href="css/finkstyle.css">
    <title>Mon programme test</title>
</head>
<body>
<div class="hero-image">
  <div class="hero-text">
    <h1 style="font-size:50px">Fink</h1>
    <h3>Alert dataset monitor</h3>
    <div class="topnav"> """

# manage Livy simulation

will_change_simul = change_simul.is_set()
change_simul.reset()

print("<br>change simul = {}".format(will_change_simul))

if will_change_simul:
    if simul.is_set():
        html += """
        <form action="/index.py" method="post" name="simul">
            <br> Currently using real Livy"""
        simul.reset()
        html += variables.to_form()
        html += """<button type="submit">Simul Livy</button>
        </form>
        """
    else:
        html += """
        <form action="/index.py" method="post">
            <br> Currently simulate Livy"""
        simul.set(1)
        html += variables.to_form()
        html += """<button type="submit">Use real Livy</button>
            </form>
        """
else:
    if simul.is_set():
        html += """
        <form action="/index.py" method="post">
            <br> Currently simulate Livy"""
        change_simul.set(1)
        html += variables.to_form()
        html += """<button type="submit">Use real Livy</button>
            </form>
        """
    else:
        html += """
            <form action="/index.py" method="post" name="simul">
                <br> Currently using real Livy"""
        change_simul.set(1)
        html += variables.to_form()
        html += """<button type="submit">Simul Livy</button>
            </form>
        """

change_simul.reset()

# Manage Livy session & Spark statements
html += """
    <form action="/index.py" method="post" name="operations">
        """

if simul.is_set():
    if waiting_session.above(5):
        print("<br> session is now idle")
        waiting_session.reset()
        waiting_statement.reset()
        livy_statement.reset()
        livy_session.set(1)

    if waiting_statement.above(5):
        print("<br> statement just finished")
        waiting_session.reset()
        waiting_statement.reset()
        livy_statement.incr()

# debugging
# print("<br>")
# print("Keys = [", ",".join(form.keys()), "]")
print(variables.debug())

"""
Command interface
- select Livy simulation
- open session & wait for idle
- start statement & wait for completion
"""

if kill_session.is_set():
    id = livy_session.value
    try:
        client.delete_session(id)
    except:
        print("error killing session ", id)

    livy_session.reset()
    waiting_session.reset()
    kill_session.reset()

if livy_session.is_set():
    # statement management
    if not waiting_statement.is_set():
        html += """<br>session is idle: we may start a statement<br>"""
        waiting_statement.set(0)
        html += variables.to_form()
        html += """
        Enter a Spark statement 
        <input type="text" name="new_statement" value="{}" /> 
        <input type="text" name="result" value="{}" />
        <button type="submit">Run</button>        
        """.format(new_statement.value, result.value)
    else:
        html += """<br>session is idle, we do wait a statement to complete<br>"""
        waiting_statement.incr()
        id = livy_session.value
        s = client.get_session(id)
        if not livy_statement.is_set():
            st = client.create_statement(s.session_id, new_statement.value)
            livy_statement.set(st.statement_id)
        else:
            st = client.get_statement(s.session_id, livy_statement.value)
            if st.state == StatementState.AVAILABLE:
                waiting_statement.reset()
                result.set(st.output.text)
                print("<br>", result.value)
                livy_statement.reset()

        html += variables.to_form()
        html += """<button type="submit">waiting statement to complete</button>"""
else:
    # session management
    if not waiting_session.is_set():
        html += """<br>No session<br>"""
        waiting_session.set(0)

        print(waiting_session.debug())

        waiting_statement.reset()
        html += variables.to_form()
        html += """<button type="submit">Open a session</button>"""
    else:
        # we have requested a new session thus waiting_session is set

        if simul.is_set():
            waiting_session.incr()
        else:

            if not livy_session.is_set():
                print("Create a session ")
                s = client.create_session(SessionKind.PYSPARK)
                print("<br> session {} <br>".format(s.session_id))
                livy_session.set(s.session_id)

            # we test if the session is already idle
            id = livy_session.value
            s = client.get_session(id)
            if s.state == SessionState.IDLE:
                print("<br> session is now idle")
                waiting_session.reset()
                waiting_statement.reset()
                livy_statement.reset()
                new_statement.reset()

        html += """<br>Waiting session to become idle<br>"""
        html += variables.to_form()
        html += """<button type="submit">waiting session</button>"""

html += """</form>"""

if livy_session.is_set():
    html += """
    <form action="/index.py" method="post" name="operations">"""

    kill_session.set(1)
    html += variables.to_form()
    html += """
         <button type="submit">Delete the session</button>
    </form>
    """




html += """
      </div>
    <p>&copy; AstroLab Software 2018-2019</p>
  </div>
</div>

</body>
</html>
"""


print(html)


"""

code = form.getvalue("statement")
if code != "":
    print("direct execution of a statement ", code)

    st = client.create_statement(s.session_id, code)

    print("... wait until available ...")
    result = ""
    while True:
        st = client.get_statement(session.session_id, st.statement_id)
        if st.state == StatementState.AVAILABLE:
            result = st.output.text
            break

    print("result = ", result)
"""

