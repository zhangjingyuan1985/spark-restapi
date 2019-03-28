#!/usr/bin/python

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
Minimal HTML server

run in the <host> machine as

> python server.py

Then, call in a web navigator the URL

http://<host>:24701/index.py

The index.py script has to be present on the <host> machine 

https://python-django.dev/page-python-serveur-web-creer-rapidement
"""

import http.server
 
PORT = 24702
server_address = ("vm-75222.lal.in2p3.fr", PORT)

server = http.server.HTTPServer
handler = http.server.CGIHTTPRequestHandler

### handler.cgi_directories = ["/home/christian.arnault/fink_data_monitor/tuto/html/"]

handler.cgi_directories = ["/"]

print("Serveur actif sur le port :", PORT)

httpd = server(server_address, handler)
httpd.serve_forever()


