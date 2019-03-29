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
#from pylivy.session import *
#from pylivy.client import *


"""
Demo of using the pylivy library

https://pylivy.readthedocs.io/en/latest/index.web

"""


# Initialize post variables
class Variable:
    def __init__(self, name):
        self.name = name
        self.value = None

    def read(self):
        try:
            self.value = int(form.getvalue(self.name))
        except:
            self.value = -1

    def to_form(self):
        out = """<input type="hidden" name="{}" value="{}" />""".format(self.name, self.value)
        return out

    def debug(self):
        out = "<br> {} = {}\n".format(self.name, self.value)
        return out

    def reset(self):
        self.value = -1

    def set(self, value):
        try:
            self.value = int(value)
        except:
            self.value = -1

    def is_set(self):
        try:
            if self.value >= 0:
                return True
        except:
            pass

        return False

    def incr(self):
        self.value += 1

    def above(self, threshold):
        try:
            if self.value > threshold:
                return True
        except:
            pass

        return False


class VariableSet:
    def __init__(self, names):
        self.base = dict()

        for name in names:
            self.base[name] = Variable(name)

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

# init data
variables = VariableSet(["simul", "change_simul", "session", "waiting_session", "waiting_statement", "statement"])
variables.read()

simul = variables.variable("simul")
simul.set(1)
change_simul = variables.variable("change_simul")
session = variables.variable("session")
waiting_session = variables.variable("waiting_session")
waiting_statement = variables.variable("waiting_statement")
statement = variables.variable("statement")

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
if simul.is_set():
    html += """
    <form action="/index.py" method="post">
        <br> Currently simulate Livy"""
    if change_simul.is_set():
        simul.set(1)
        change_simul.reset()
    html += variables.to_form()
    html += """<button type="submit">Use real Livy</button>
        </form>
    """
else:
    html += """
        <form action="/index.py" method="post" name="simul">
            <br> Currently using real Livy"""
    if change_simul.is_set():
        simul.reset()
        change_simul.reset()
    html += variables.to_form()
    html += """<button type="submit">Simul Livy</button>
        </form>
    """

# Manage Livy session & Spark statements
html += """
    <form action="/index.py" method="post" name="operations">
        """

if waiting_session.above(5):
    print("<br> session is now idle")
    waiting_session.reset()
    waiting_statement.reset()
    statement.reset()
    session.set(1)

if waiting_statement.above(5):
    print("<br> statement just finished")
    waiting_session.reset()
    waiting_statement.reset()
    statement.incr()

# debugging
print("<br>")
print("Keys = [", ",".join(form.keys()), "]")
print(variables.debug())

"""
Command interface
- select Livy simulation
- open session & wait for idle
- start statement & wait for completion
"""

if session.is_set():
    if not waiting_statement.is_set():
        html += """<br>session is idle start a staement<br>"""
        waiting_statement.set(0)
        html += variables.to_form()
        html += """Enter a Spark statement <input type="text" name="new_statement" value="code" />"""
    else:
        html += """<br>session is idle waiting a staement to complete<br>"""
        waiting_statement.incr()
        html += variables.to_form()
        html += """<button type="submit">waiting statement</button>"""
elif not waiting_session.is_set():
    html += """<br>No session<br>"""
    waiting_session.set(0)

    if not simul.is_set():
        pass

    print(waiting_session.debug())

    waiting_statement.reset()
    html += variables.to_form()
    html += """<button type="submit">Open a session</button>"""
else:
    html += """<br>Waiting session to become idle<br>"""
    waiting_session.incr()
    html += variables.to_form()
    html += """<button type="submit">waiting session</button>"""

html += """</form>"""

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
print("Create a session ")
session = client.create_session(SessionKind.PYSPARK)
print("=> session ", session.session_id)

print("... wait until idle ...")
while True:
    session = client.get_session(s.session_id)
    if session.state == SessionState.IDLE:
        break

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

