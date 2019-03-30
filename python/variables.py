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

class HTMLVariable:
    """
    Handle web variables meant to exchange between client and server
    Variables have a type that can be "int" or "str"

    "int" variables are positive integers. Negative values are interpreted as "unset"
    "str" variables are considered "set" when they non empty (not "")
    """

    def __init__(self, name, type="int"):
        """
        initalizer. Default type is "int"
        :param name:
        :param type:
        """
        self.name = name
        self.type = type
        self.reset()

    def read(self):
        """
        Try and get values from the GET/POST values
        :return:
        """
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
        """
        format the variable value to be sent as a hidden HTML parameter
        :return:
        """
        out = """<input type="hidden" name="{}" value="{}" />""".format(self.name, self.value)
        return out

    def debug(self):
        """
        Format a variable value tu be displayed on a web page
        :return:
        """
        out = " {} = {}\n".format(self.name, self.value)
        return out

    def reset(self):
        """
        Apply the convention to be "unset" according to variable type
        :return:
        """
        if self.type == "int":
            self.value = -1
        else:
            self.value = ""
        pass

    def set(self, value):
        """
        Apply the convention to be "set" according to variable type, and assign a value
        :return:
        """
        if self.type == "int":
            try:
                self.value = int(value)
            except:
                self.value = -1
        else:
            self.value = value

    def is_set(self):
        """
        Ask whether the variable is considered as "set" (following the convention)
        :return:
        """
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
        """
        Increment an "int" typed variable
        :return:
        """
        if self.type == "int":
            self.value += 1

    def above(self, threshold):
        """
        Test an "int" variable if its vaue is above a threshold
        :param threshold:
        :return:
        """
        if self.type == "int":
            try:
                if self.value > threshold:
                    return True
            except:
                pass

        return False


class HTMLVariableSet:
    """
    Database of all the WEB variables of the application
    """

    def __init__(self, names, str_names):
        type = "int"

        for name in names:
            if name in str_names:
                type = "str"
            else:
                type = "int"
            self.__dict__[name] = HTMLVariable(name, type)

    def variable(self, name):
        return self.__dict__[name]

    def read(self):
        for v in self.__dict__:
            self.__dict__[v].read()

    def to_form(self):
        out = ""
        for v in self.__dict__:
            out += self.__dict__[v].to_form()
        return out

    def debug(self):
        out = ""
        for v in self.__dict__:
            out += self.__dict__[v].debug()
        return out

