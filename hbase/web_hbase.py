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



def row(left, right):
    return """
                    <tr>
                        <td class="tdleft">{}</td>
                        <td class="tdright">{}</td>
                    </tr>
    """.format(left, right)


def main():
    print("Happybase test")
    host = "localhost:8080"

    with open("hbase.html", "r") as f:
        lines = f.readlines()

    html_head = ""
    html_tail = ""

    rows = [
        {'left': 'aaa', 'right': 'bbb'},
        {'left': 'ccc', 'right': 'ddd'},
        ]

    in_contents = False
    in_tail = False

    for line in lines:
        if not in_contents:
            if "<!-- start contents -->" in line:
                in_contents = True
                continue

            html_head += line
        elif not in_tail:
            if "<!-- end contents -->" in line:
                in_tail = True
                continue
        else:
            html_tail += line

    print(html_head)
    for r in rows:
        print(row(r['left'], r['right']))
    print(html_tail)

if __name__ == "__main__":
    main()
