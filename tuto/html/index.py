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
                         "change_simul"], [])

start = variables.base["start"]
simul = variables.base["simul"]
change_simul = variables.base["change_simul"]

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
print(variables.debug())

html += """
      </div>
    <p>&copy; AstroLab Software 2018-2019</p>
  </div>
</div>

</body>
</html>
"""


print(html)


