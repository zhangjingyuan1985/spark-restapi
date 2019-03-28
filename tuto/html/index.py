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

LIVY_URL = "http://vm-75222.lal.in2p3.fr:21111"


form = cgi.FieldStorage()
print("Content-type: text/html; charset=utf-8\n")

name = ""
print(form.getvalue("name"))

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
    <div class="topnav">

    <form action="/index.py" method="post">
        Give your name:<input type="text" name="name" value="Votre nom" />
        <br>
        Enter a Spark statement <input type="text" name="statement" value=code />
        <br>
        Send: <input type="submit" name="send" value="Envoyer information au serveur">
    </form> 
      </div>
    <p>&copy; AstroLab Software 2018-2019</p>
  </div>
</div>

</body>
</html>
"""

print(html)
