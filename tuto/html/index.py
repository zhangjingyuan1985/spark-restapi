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

"""



import cgi 

form = cgi.FieldStorage()
print("Content-type: text/web; charset=utf-8\n")

print(form.getvalue("name"))
print(form.getvalue("age"))

html = """
<!DOCTYPE html>
<html>
<head>

<!-- Define the styles -->
<link rel="stylesheet" type="text/css" href="css/finkstyle.css">

</head>

<!-- Main start -->
<body>
  <!-- Navigation bar -->
<div class="hero-image">
  <div class="hero-text">
    <h1 style="font-size:50px">Fink</h1>
    <h3>Alert dataset monitor</h3>
    <div class="topnav">
      <a class="active" href="/index.html">Fink &#8594;</a>
      <a href="/live.html">Live</a>
      <a href="/about.html">About</a>
      </div>
    <p>&copy; AstroLab Software 2018-2019</p>
    <form action="/index.py" method="post">
        <input type="text" name="name" value="Votre nom" />
       <input type="text" name="age" value="Votre age" />

        <input type="submit" name="send" value="Envoyer information au serveur">
    </form> 
  </div>
</div>

</body>
</html>

"""

print(html)
