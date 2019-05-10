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


import xmltodict
import base64
import requests
from collections import OrderedDict
import re
from gateway import *

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class Cell(object):
    def __init__(self, name, value=None, timestamp=None):
        self.name = name
        self.value = value
        self.timestamp = timestamp

    def set(self, value=None, timestamp=None):
        self.value = value
        self.timestamp = timestamp

    def __str__(self):
        return "Cell> {} = {} T={}".format(self.name, self.value, self.timestamp)


class Row(object):
    def __init__(self):
        self.key = ""
        self.cells = []

    def set_key(self, key):
        self.key = key

    def add_cell(self, cell):
        self.cells.append(cell)

    def __str__(self):
        t = "Row> {}".format(self.key)
        for c in self.cells:
            t += "\n  {}".format(c)

        return t

class HBase(object):
    def __init__(self):
        self.host, self.auth = gateway_url('hbase')

    def get_version(self):
        """
        """

        headers = { "Accept": "text/xml"}

        r = requests.get(self.host + '/version', headers = headers, auth = self.auth, verify = False)

        xpars = xmltodict.parse(r.text)
        return xpars["Version"]

    def get_namespaces(self):
        headers = { "Accept": "text/xml"}
        r = requests.get(self.host + '/namespaces', headers = headers, auth = self.auth, verify = False)

        xpars = xmltodict.parse(r.text)
        return xpars["Namespaces"]

    def get_tables(self):
        headers = {"Accept": "text/xml"}
        r = requests.get(self.host + '/', headers=headers, auth = self.auth, verify = False)

        xpars = xmltodict.parse(r.text)

        ns = xpars['TableList']['table']

        tables = []

        if len(ns) > 1:
            for item in ns:
                tables.append(item['@name'])
        else:
            tables.append(ns['@name'])

        return tables

    def get_schema(self, table):
        headers = {"Accept": "text/xml"}
        r = requests.get(self.host + '/{}/schema'.format(table), headers=headers, auth = self.auth, verify = False)

        families = []

        if r.status_code == 404:
            return None

        xpars = xmltodict.parse(r.text)
        schema = xpars["TableSchema"]["ColumnSchema"]

        if type(schema) == list:
            for schemaItem in schema:
                families.append(table + '.' + schemaItem['@name'])
        else:
            families.append(table + '.' + schema['@name'])

        return families

    def get_regions(self, table):
        headers = {"Accept": "text/xml"}
        r = requests.get(self.host + '/{}/regions'.format(table), headers=headers, auth = self.auth, verify = False)

        xpars = xmltodict.parse(r.text)
        return xpars["TableInfo"]

    def create_table(self, table, families, recreate = False):
        """
        :param table:
        :param families:
        :param recreate: If True, forget previous definition of this table
                         If False verify if the table existed and in this case keep it unchanged
        :return:
        """
        if recreate:
            self.delete_table(table)
        else:
            f = self.get_schema(table)
            if not f is None:
                return f

        headers = {"Accept": "text/xml",
                   "Content-Type": "text/xml"}
        data = '<?xml version="1.0" encoding="UTF-8"?> <TableSchema name="{}">'.format(table)
        for family in families:
            data += '<ColumnSchema name="{}" />'.format(family)
        data += '</TableSchema>'

        r = requests.post(self.host + '/{}/schema'.format(table), headers=headers, data=data, auth = self.auth, verify = False)
        print("create_table", r.status_code)

        families = self.get_schema(table)
        return families

    def delete_table(self, table):
        headers = {"Accept": "text/xml"}

        r = requests.delete(self.host + '/{}/schema'.format(table), headers=headers, auth = self.auth, verify = False)
        return r

    def create_rowobj(self, row):

        def create_cell(cell):
            column_name = ""
            column_value = ""
            timestamp = ""
            for e in cell:
                if e in ["column", "@column"]:
                    encoded = cell[e].encode('utf-8')
                    column_name = base64.b64decode(encoded)
                elif e in ["$", "#text"]:
                    encoded = cell[e].encode('utf-8')
                    column_value = base64.b64decode(encoded)
                elif e in ["timestamp", "@timestamp"]:
                    timestamp = cell[e]
                else:
                    # print(e, cell[e])
                    pass
            cell = Cell(column_name, column_value, timestamp)
            return cell

        rowobj = Row()

        for k in row:
            if k in ["key", "@key"]:
                encoded = row[k].encode('utf-8')
                v = base64.b64decode(encoded)
                rowobj.set_key(v)
                # print('row key = {}'.format(v))
            elif k == "Cell":
                cells = row[k]

                # print("Type de cells: {}".format(type(cells)))

                if isinstance(cells, list):
                    for cell in cells:
                        c = create_cell(cell)
                        rowobj.add_cell(c)
                        # print(c)
                elif isinstance(cells, OrderedDict):
                    c = create_cell(cells)
                    rowobj.add_cell(c)
                    # print(c)
                else:
                    print("???")
            else:
                print("???", k, row[k])

        return rowobj

    def get_row(self, table, keyrow):
        headers = {"Content-Type" : "application/json", "Accept" : "application/json"}

        result = []

        r = requests.get(self.host + '/{}/{}'.format(table, keyrow), headers=headers, auth = self.auth, verify = False)

        if r.status_code == 404:
            return None

        # print('get row')

        data = r.json()

        for e1 in data:
            # print(e1)
            rows = data['Row']
            for row in rows:
                result.append(self.create_rowobj(row))

        # xpars = xmltodict.parse(r.text)
        return result

    def get_scanner(self, table, max=10):
        headers = {"Content-Type" : "text/xml", "Accept" : "text/xml"}
        data = '<Scanner batch="{}"/>'.format(max)
        r = requests.put(self.host + '/{}/scanner'.format(table), data=data, headers=headers, auth = self.auth, verify = False)

        loc = r.headers['Location']
        m = re.match('.*[/](.*)$', loc)
        scanner = m[1]

        return scanner

    def add_row(self, table, rowkey, row):
        """
curl -vi -X PUT \
         -H "Accept: text/json" \
         -H "Content-Type: text/json" \
         -d '{"Row":[{"key":"cm93NQo=", "Cell": [{"column":"Y2Y6ZQo=", "$":"dmFsdWU1Cg=="}]}]}'' \
         "example.com:20550/users/fakerow"
        :return:
        """
        headers = {"Accept" : "text/xml", "Content-Type" : "text/xml"}

        """
        as input, a row is a dict of {key, value}
        """

        def encode(value):
            return base64.b64encode(str(value).encode('utf-8')).decode('utf-8')

        """
        cells = []
        for key in row:
            cell = {"column": encode(key), "$": encode(row[key])}
            cells.append(cell)
        """

        data = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><CellSet>'

        ### data += '<Row key="cm93MQ==">'
        ### data += '<Cell column="ZmFtaWx5MTpjb2wx" >dGVzdA==</Cell>'

        data += '<Row key="{}">'.format(encode(rowkey))

        for key in row:
            data += '<Cell column="{}" >{}</Cell>'.format(encode(key), encode(row[key]))

        data += '</Row>'
        data += '</CellSet>'

        r = requests.put(self.host + '/{}/{}'.format(table, rowkey),
                         headers=headers,
                         data=data,
                         auth = self.auth,
                         verify = False)

        return r


    def delete_scanner(self, table, scanner):
        headers = {"Accept" : "text/xml"}
        r = requests.delete(self.host + '/{}/scanner/{}'.format(table, scanner), headers=headers, auth = self.auth, verify = False)
        return r

    def scan(self, table, scanner):
        headers = {"Accept" : "text/xml"}

        result = []

        while True:
            r = requests.get(self.host + '/{}/scanner/{}'.format(table, scanner), headers=headers, auth = self.auth, verify = False)
            if r.status_code == 204:
                break

            xpars = xmltodict.parse(r.text)
            rows = xpars['CellSet']

            for key in rows:
                if type(rows[key]) == list:
                    for row in rows[key]:
                        result.append(self.create_rowobj(row))
                else:
                    row = rows[key]
                    result.append(self.create_rowobj(row))

        return result

    def get_rows(self, table):
        scanner = self.get_scanner(table)
        result = self.scan(table, scanner)
        self.delete_scanner(table, scanner)
        return result

    def get_unique_id(self, table):
        families = self.get_schema(table)
        if families is None:
            r = self.create_table(table, ['unique'])
            if r is None:
                return None

        row = self.get_row(table, 'unique')
        unique = 1
        if not row is None:
            cell = row[0].cells[0]
            unique = int(cell.value) + 1

        self.add_row(table, 'unique', {'unique:unique': unique})

        return unique


def main():
    pass


if __name__ == "__main__":
    main()
