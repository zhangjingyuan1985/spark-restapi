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
https://www.cloudera.com/documentation/enterprise/5-4-x/topics/admin_hbase_filtering.html

logical operators

AND OR SKIP WHILE

comparison operators

LESS (<)
LESS_OR_EQUAL (<=>
EQUAL (=)
NOT_EQUAL (!=)
GREATER_OR_EQUAL (>=)
GREATER (>)
NO_OP (no operation)

comparators

BinaryComparator
BinaryPrefixComparator
RegexStringComparator
SubStringComparator

types

KeyOnlyFilter
FirstKeyOnlyFilter
PrefixFilter
ColumnPrefixFilter
MultipleColumnPrefixFilter
ColumnCountGetFilter
PageFilter
ColumnPaginationFilter
InclusiveStopFilter
TimeStampsFilter
RowFilter
FamilyFilter
QualifierFilter
ValueFilter
DependentColumnFilter
SingleColumnValueFilter
SingleColumnValueExcludeFilter
ColumnRangeFilter



"""

import base64

def encode(value):
    return base64.b64encode(str(value).encode('utf-8')).decode('utf-8')

def comparator(value):
    data = ""
    data += '      "comparator": { '
    data += '          "type": "BinaryComparator", '
    data += '          "value": "{}" '.format(encode(value))
    data += '      } '
    return data

def row_filter(value):
    data = ""
    data += '   { '
    data += '      "type": "RowFilter", '
    data += '      "op": "EQUAL", '
    data += comparator(value)
    data += '   }, '
    return data

def family_filter(value):
    data = ""
    data += '   { '
    data += '      "type": "FamilyFilter", '
    data += '      "op": "EQUAL", '
    data += comparator(value)
    data += '   }, '
    return data

def value_filter(value):
    data = ""
    data += '   { '
    data += '      "type": "ValueFilter", '
    data += '      "op": "EQUAL", '
    data += comparator(value)
    data += '   } '
    return data

def column_prefix_filter(value):
    data = ""
    data += '   { '
    data += '     "type": "ColumnPrefixFilter", '
    data += '     "value": "{}" '.format(encode(value))
    data += '   } '

    return data

def list_filter(filter_list):
    data = ""
    data += ' { '
    data += ' "type": "FilterList", '
    data += ' "op": "MUST_PASS_ALL", '
    data += ' "filters": [ '
    data += ", ".join(filter_list)
    data += "   ] "
    data += ' } '
    return data

def column_range_filter(min, max):
    data = ""
    data += ' { '
    data += '     "type": "ColumnRangeFilter", '
    data += '     "minColumn": "{}", '.format(encode(min))
    data += '     "minColumnInclusive": true, '
    data += '     "maxColumn": "{}", '.format(encode(max))
    data += '     "maxColumnInclusive": false '
    data += ' } '
    return data

def first_key_only_filter():
    data = ""
    data += ' { '
    data += '     "type": "FirstKeyOnlyFilter" '
    data += ' } '
    return data

def inclusive_stop_filter(value):
    data = ""
    data += ' { '
    data += '     "type": "InclusiveStopFilter", '
    data += '     "value": "{}" '.format(encode(value))
    data += ' } '
    return data

def multiple_column_prefix_filter(prefixes):
    data = ""

    data += ' { '
    data += '     "type": "MultipleColumnPrefixFilter", '
    data += '     "prefixes": [ '
    data += ", ".join(['         "{}" '.format(encode(prefix)) for prefix in prefixes])
    data += '     ] '
    data += ' } '
    return data

def page_filter(value):
    data = ""
    data += ' { '
    data += '     "type": "PageFilter", '
    data += '     "value": "{}" '.format(value)
    data += ' } '
    return data

def prefix_filter(value):
    data = ""

    data += ' { '
    data += '     "type": "PrefixFilter", '
    data += '     "value": "{}" '.format(encode(value))
    data += ' } '

    return data

def qualifier_filter(value):
    data = ""

    data += ' { '
    data += '     "type": "QualifierFilter", '
    data += '     "op": "GREATER", '
    data += comparator(value)
    data += ' } '

    return data

def single_column_value_filter(family, qualifier, value):
    data = ""

    data += ' { '
    data += '     "type": "SingleColumnValueFilter", '
    data += '     "op": "EQUAL", '
    data += '     "family": "{}", '.format(encode(value))
    data += '     "qualifier": "{}", '.format(encode(value))
    data += '     "latestVersion": true, '
    data += comparator(value)
    data += ' } '

    return data

def timestamps_filter(value):
    data = ""

    data += ' { '
    data += '     "type": "TimestampsFilter", '
    data += '     "timestamps": [ '
    data += '         "{}" '.format(encode(value))
    data += '     ] '
    data += ' } '

    return data

"""
def xxx_filter():
    data = ""
    return data
"""

def scanner_filter(filter):
    data = ""
    data += """
<Scanner batch="100">
<filter>
      """
    data += filter
    data += """
</filter>
</Scanner>
        """

    return data


def main():
    pass


if __name__ == "__main__":
    main()
