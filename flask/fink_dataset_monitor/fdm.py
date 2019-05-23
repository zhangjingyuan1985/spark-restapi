#! /usr/bin/python
# -*- coding:utf-8 -*-

import sys
import random
from time import sleep
from threading import Thread
from flask import Flask, request, render_template
from flask_socketio import SocketIO, emit

sys.path.append("d:/workspace/pylivy/")
sys.path.append("../../lib")

# from livy.session import *
import livy
import hbase_lib
import gateway


__author__ = 'Chris Arnault'

"""
Setting up Livy and HBase
"""
url, auth = gateway.gateway_url('livy/v1')
livy_client = livy.client.LivyClient(url, auth=auth, verify_ssl=False)
hbase = hbase_lib.HBase()

dtb_users = hbase.create_table('livy_users', ['identity', 'sessions'])

print("show the DTB of existing users")
rows = hbase.get_rows('livy_users')
for row in rows:
    name = row.cells['identity:name'].value
    sessions = row.cells['sessions:livy'].value
    print("user {} sessions {}".format(name, sessions))

print("--------------------------------------")

"""
============================================================================================================================
Flask section
============================================================================================================================
"""

simulation = False

app: Flask = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
app.config['DEBUG'] = True

"""
turn the flask app into a socketio app
"""
socketio = SocketIO(app)


@app.context_processor
def inject_stage_and_region():
    """
    This Flask template variable will be used to set a version id
    to force to re-load the javascript during the development
    """
    v = random.random()*9999
    return dict(version="{}".format(v))


@app.route('/', methods=['GET', 'POST'])
def fink_dataset_monitor():

    users = []

    rows = hbase.get_rows('livy_users')
    for row in rows:
        name = row.cells['identity:name'].value
        users.append(name)
        sessions = row.cells['sessions:livy'].value

    out = render_template("user.html", len=len(users), users=users)
    return out


@socketio.on('connect', namespace='/test')
def test_connect():
    """
    Management of HTML sessions
    """
    # need visibility of the global thread object
    print('Client connected')


@socketio.on('disconnect', namespace='/test')
def test_disconnect():
    print('Client disconnected')


@app.route('/open', methods=['GET', 'POST'])
def open_session():
    """
    We are ready to open a new Livy session
    """
    print("Starting Thread")
    thread = SessionThread()
    session_id = thread.session_launch()
    out = render_template("wait.html", session_id=session_id)
    return out


@app.route('/wait', methods=['GET', 'POST'])
def wait_session():
    """
    The Livy session has been launched, wait until it becomes Idle
    """
    session_id = request.form["session_id"]
    out = render_template("session.html", session_id=session_id)
    return out


@app.route('/idle', methods=['GET', 'POST'])
def idle():
    """
    return back from a IdleStatement (handled in Javascript)
    """
    session_id = int(request.form["session_id"])
    print("Received idle form session_id={} type={}".format(session_id, type(session_id)))
    out = render_template("session.html", session_id=session_id)
    return out


@app.route('/close', methods=['GET', 'POST'])
def close_session():
    """
    Request to close the Livy session
    """
    if "session_id" in request.form:
        session_id = request.form["session_id"]

        print("trying to close session {}".format(session_id))

        if simulation:
            if session_id in session_threads:
                session_threads.pop(session_id, None)
        else:
            try:
                livy_client.delete_session(session_id)
            except:
                print("error killing session ", session_id)

        # closing the Livy session
    out = render_template("open.html")
    return out


@app.route('/open_statement', methods=['GET', 'POST'])
def open_statement():
    session_id = request.form["session_id"]
    statement = request.form["statement"]
    print("Starting Thread for statement session={} statement={}".format(session_id, statement))
    thread = StatementThread()
    statement_id = thread.statement_launch(session_id, statement)

    out = render_template("wait_statement.html",
                          session_id=session_id,
                          statement=statement,
                          statement_id=statement_id)

    return out


@app.route('/available', methods=['GET', 'POST'])
def available():
    session_id = int(request.form["session_id"])
    statement_id = int(request.form["statement_id"])
    statement = request.form["statement"]
    result = request.form["result"]
    print("Received available form session_id={} statement_id={}".format(session_id, statement_id))
    out = render_template("statement.html",
                          session_id=session_id,
                          statement_id=statement_id,
                          previous_statement=statement,
                          result=result)
    return out


"""
============================================================================================================================
End of Flask section
============================================================================================================================
"""


"""
Database of threads for asynchronous events
"""
session_threads = {}
statement_threads = {}


class SessionThread(Thread):
    """
    When a Livy session is requested, a thread is started to wait until the session becomes Idle
    Then a socketio event (IdleEvent) is triggered
    This in turn awakes up the main thread and completes the HTML page to ask for Livy statements

    If the session is pending and a close is requested, the corresponding thread must be stopped
    """

    def __init__(self):
        self.session_id = None
        self.stop = None
        super(SessionThread, self).__init__()

    def do_stop(self):
        if self.stop is None:
            return False
        return self.stop()

    def session_launch(self, stop=None):
        """
        start a Livy session
        """
        print("thread> Launching a Livy session")
        self.stop = stop
        self.start()

        return self.session_id

    def simul(self):
        """
        core action in simulation mode
        session id are randomly created
        duration time is random
        """
        global session_threads

        self.session_id = int(random.random() * 1000)

        session_threads[self.session_id] = self

        sleep(0.1)
        t_max = int(random.random() * 5 + 1)
        for t in range(t_max):
            if self.do_stop():
                break
            sleep(1)

    def livy(self):
        """
        core action in Livy mode

        session id are given by Livy
        we wait until session becomes Idle
        """
        global session_threads

        print("Create a Livy session ")

        s = livy_client.create_session(livy.session.SessionKind.PYSPARK)
        print("<br> session {} <br>".format(s.session_id))
        self.session_id = s.session_id

        session_threads[self.session_id] = self

        while not self.do_stop():
            sleep(0.1)
            s = livy_client.get_session(self.session_id)
            if s.state == livy.session.SessionState.IDLE:
                print("<br> session is now idle")
                break

    def run(self):
        """
        Launch a session
        Send a asynchronous event when core action is completed
        """
        global session_threads

        if simulation:
            self.simul()
        else:
            self.livy()

        print("thread> Session {} is now Idle".format(self.session_id))
        socketio.emit('IdleEvent', {'session_id': self.session_id}, namespace='/test')

        session_threads.pop(self.session_id, None)


class StatementThread(Thread):
    """
    When a Livy statement is attempted, a thread is started to wait until the statement becomes Available
    Then a socketio event (AvailableEvent) is triggered
    This in turn awakes up the main thread and completes the HTML page to ask for the next Livy statements

    If the statement is pending and a close is requested, the corresponding thread must be aborted
    """

    def __init__(self):
        self.session_id = None
        self.statement_id = None
        self.statement = None
        self.result = None
        self.stop = None

        super(StatementThread, self).__init__()

    def statement_launch(self, session_id, statement, stop=None):
        """
        start of a Livy statement
        """

        print("thread> Launching a Livy statement=[{}]".format(statement))
        self.session_id = session_id
        self.statement = statement

        self.stop = stop
        self.start()
        return self.statement_id

    def do_stop(self):
        if self.stop is None:
            return False
        return self.stop()

    def simul(self):
        global statement_threads

        self.statement_id = int(random.random() * 1000)

        statement_threads[self.statement_id] = self

        sleep(0.1)
        t_max = int(random.random() * 5 + 1)
        for t in range(t_max):
            if self.do_stop():
                break
            sleep(1)
        self.result = "ok {}".format(self.statement_id)

    def livy(self):
        global statement_threads

        print("Create a Livy statement ")
        s = livy_client.get_session(self.session_id)
        st = livy_client.create_statement(s.session_id, self.statement)

        print("<br> statement {} <br>".format(st.statement_id))
        self.statement_id = st.statement_id

        statement_threads[self.statement_id] = self

        while not self.do_stop():
            sleep(0.1)

            st = livy_client.get_statement(s.session_id, self.statement_id)
            if st.state == livy.session.StatementState.AVAILABLE:
                self.result = st.output.text
                print("<br> statement is now available")
                break

    def run(self):
        global statement_threads

        if simulation:
            self.simul()
        else:
            self.livy()

        print("thread> Statement {} is now Available session_id={} statement_id={}".format(self.statement,
                                                                                           self.session_id,
                                                                                           self.statement_id))

        socketio.emit('AvailableEvent', {'session_id': self.session_id,
                                         'statement_id': self.statement_id,
                                         'statement': self.statement,
                                         'result': self.result}, namespace='/test')


if __name__ == '__main__':
    # app.run(debug=True)
    import sys
    import re

    host = "127.0.0.1"
    port = 24701

    if len(sys.argv) >= 2:
        for arg in sys.argv:
            m = re.match(r"..port.([\d]+)", arg)
            if m is not None:
                port = m[1]
                continue

            m = re.match(r"..host.(.+)", arg)
            if m is not None:
                host = m[1]
                continue

    socketio.run(app, port=port, host=host)
