#! /usr/bin/python
# -*- coding:utf-8 -*-


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

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import netrc
import os

os.environ['HOME'] = 'c:/arnault'

def gateway_url(prefix):
    gateway_name = "gateway_spark"
    host = "vm-75109.lal.in2p3.fr"
    port = 8443
    gateway = "gateway/knox_spark_adonis"

    secrets = netrc.netrc()
    login, username, password = secrets.authenticators(gateway_name)

    return 'https://{}:{}/{}/{}'.format(host, port, gateway, prefix), (login, password)


def main():
    pass


if __name__ == "__main__":
    main()
