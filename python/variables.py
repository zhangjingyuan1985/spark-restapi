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

    def __init__(self, name: str, type="int"):
        """
        initalizer. Default type is "int"

        Parameters
        ----------
        name: str
          Variable name
        type: str
          Variable type can be "int" or "str"

        Examples
        --------
        >>> variable = HTMLVariable("name")
        >>> print(type(variable))
        <class 'variables.HTMLVariable'>

        """

        self.name = name
        self.type = type
        self.reset()

    def read(self):
        """
        Try and get values from the GET/POST values

        Parameters
        ----------

        Returns
        -------

        Examples
        --------

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

        Parameters
        ----------
        Returns
        -------
        out: str

        Examples
        --------
        >>> variable = HTMLVariable("name")
        >>> variable.to_form()
        '<input type="hidden" name="name" value="-1" />'

        """
        out = """<input type="hidden" name="{}" value="{}" />""".format(self.name, self.value)
        return out

    def debug(self):
        """
        Format a variable value tu be displayed on a web page

        Parameters
        ----------
        Returns
        -------
        out: str

        Examples
        --------
        >>> variable = HTMLVariable("name")
        >>> print(variable.debug())
         name=-1
        <BLANKLINE>
        """
        out = " {}={}\n".format(self.name, self.value)
        return out

    def reset(self):
        """
        Apply the convention to be "unset" according to variable type
        Parameters
        ----------
        Returns
        -------
        Examples
        --------
        >>> variable = HTMLVariable("abc")
        >>> print(variable.debug())
         abc=-1
        <BLANKLINE>

        >>> variable.reset()
        >>> print(variable.debug())
         abc=-1
        <BLANKLINE>
        """
        if self.type == "int":
            self.value = -1
        else:
            self.value = ""
        pass

    def set(self, value):
        """
        Apply the convention to be "set" according to variable type, and assign a value
        Parameters
        ----------
        value: str or int

        Returns
        -------
        Examples
        --------
        >>> variable = HTMLVariable("abc")
        >>> variable.set(123)
        >>> print(variable.debug())
         abc=123
        <BLANKLINE>

        >>> variable = HTMLVariable("abc", type="str")
        >>> variable.set("xyz")
        >>> print(variable.debug())
         abc=xyz
        <BLANKLINE>

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
        Parameters
        ----------
        Returns
        -------
        status = Boolean

        Examples
        --------
        >>> variable = HTMLVariable("abc", type="str")
        >>> test = variable.is_set()
        >>> print(test)
        False

        >>> variable.set("xyz")
        >>> test = variable.is_set()
        >>> print(test)
        True

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
        Parameters
        ----------
        Returns
        -------
        Examples
        --------
        >>> variable = HTMLVariable("abc")
        >>> variable.set(123)
        >>> variable.incr()
        >>> print(variable.debug())
         abc=124
        <BLANKLINE>
        """
        if self.type == "int":
            self.value += 1

    def above(self, threshold):
        """
        Test an "int" variable if its vaue is above a threshold
        Parameters
        ----------
        threshold: int

        Returns
        -------
        status: Boolean

        Examples
        --------
        >>> variable = HTMLVariable("abc")
        >>> variable.set(123)
        >>> test = variable.above(124)
        >>> print(test)
        False

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

    def __init__(self, int_names, str_names):
        """
        constructor for a set of HTMLVariable objects
        One may provide two lists of variable names: one for int variables, and one for str variables

        Parameters
        ----------
        int_names: List<str>
        str_names: List<str>

        Returns
        -------

        Examples
        --------
        >>> variables = HTMLVariableSet(["a", "b", "c"], ["d"])
        >>> print(variables.debug())
         a=-1
         b=-1
         c=-1
         d=
        <BLANKLINE>
        """
        type = "int"

        for name in int_names:
            self.__dict__[name] = HTMLVariable(name, "int")

        for name in str_names:
            self.__dict__[name] = HTMLVariable(name, "str")

    def variable(self, name) -> HTMLVariable:
        """
        Get the HTMLVariable from its name

        Parameters
        ----------
        name: str

        Returns
        -------
        Examples
        --------
        >>> variables = HTMLVariableSet(["a", "b", "c"], ["d"])
        >>> print(type(variables.a))
        <class 'variables.HTMLVariable'>
        """
        return self.__dict__[name]

    def read(self):
        """
        Read all HTML POST/GET variables

        Parameters
        ----------

        Returns
        -------

        Examples
        --------
        """
        for v in self.__dict__:
            self.__dict__[v].read()

    def to_form(self):
        """
        format all the variable values to be sent as a hidden HTML parameter


        Parameters
        ----------
        Returns
        -------
        out: str

        Examples
        --------
        >>> variables = HTMLVariableSet(["a", "b", "c"], ["d"])
        >>> print(variables.debug())
         a=-1
         b=-1
         c=-1
         d=
        <BLANKLINE>
        """
        out = ""
        for v in self.__dict__:
            out += self.__dict__[v].to_form()
        return out

    def debug(self):
        """
        Format all the variable values tu be displayed on a web page

        Parameters
        ----------
        Returns
        -------
        out: str

        Examples
        --------
        >>> variables = HTMLVariableSet(["a", "b", "c"], ["d"])
        >>> print(variables.debug())
         a=-1
         b=-1
         c=-1
         d=
        <BLANKLINE>
        """
        out = ""
        for v in self.__dict__:
            out += self.__dict__[v].debug()
        return out

if __name__ == "__main__":
    """ Execute the test suite """
