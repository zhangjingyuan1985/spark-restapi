#! /usr/bin/python
# -*- coding:utf-8 -*-

from pylivy.session import *
from pylivy import client
from flask_socketio import SocketIO, emit
from flask import Flask, request, render_template
from time import sleep
from threading import Thread
import random

from requests.auth import HTTPBasicAuth

__author__ = 'Chris Arnault'

# LIVY_URL = "http://vm-75222.lal.in2p3.fr:21111"
LIVY_URL = "https://134.158.75.109:8443/gateway/knox_spark/"

livy_client = client.LivyClient(url=LIVY_URL)
simulation = False

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
app.config['DEBUG'] = True

"""
This Flask template variable will be used to set a version id to force to re-load the javascript during the development
"""
@app.context_processor
def inject_stage_and_region():
    v = random.random()*9999
    return dict(version="{}".format(v))

"""
Database of threads for asynchronous events
"""
session_threads = {}
statement_threads = {}

"""
turn the flask app into a socketio app
"""
socketio = SocketIO(app)

"""
When a Livy session is requested, a thread is started to wait until the session becomes Idle
Then a socketio event (IdleEvent) is triggered 
This in turn awakes up the main thread and completes the HTML page to ask for Livy statements

If the session is pending and a close is requested, the corresponding thread must be stopped
"""
class SessionThread(Thread):
    def __init__(self):
        self.session_id = None
        self.stop = None
        super(SessionThread, self).__init__()

    def sessionLaunch(self, stop = None):
        """
        start a Livy session
        """
        print("thread> Launching a Livy session")
        self.stop = stop
        self.start()
        return self.session_id

    def do_stop(self):
        if self.stop is None:
            return False
        return self.stop()

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
        interrupt = False
        for t in range(t_max):
            if self.do_stop():
                interrupt = True
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

        auth = HTTPBasicAuth('christian.arnault', '@@@DTsa57')

        s = livy_client.create_session(SessionKind.PYSPARK, proxy_user=auth)
        print("<br> session {} <br>".format(s.session_id))
        self.session_id = s.session_id

        session_threads[self.session_id] = self

        while(not self.do_stop()):
            sleep(0.1)
            s = livy_client.get_session(self.session_id)
            if s.state == SessionState.IDLE:
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

"""
When a Livy statement is attempted, a thread is started to wait until the statement becomes Available
Then a socketio event (AvailableEvent) is triggered 
This in turn awakes up the main thread and completes the HTML page to ask for the next Livy statements

If the statement is pending and a close is requested, the corresponding thread must be aborted
"""
class StatementThread(Thread):
    def __init__(self):
        self.session_id = None
        self.statement_id = None
        self.statement = None
        self.result = None
        self.stop = None

        super(StatementThread, self).__init__()

    def statementLaunch(self, session_id, statement, stop = None):
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
        interrupt = False
        for t in range(t_max):
            if self.do_stop():
                interrupt = True
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

        while(not self.do_stop()):
            sleep(0.1)

            st = livy_client.get_statement(s.session_id, self.statement_id)
            if st.state == StatementState.AVAILABLE:
                self.result = st.output.text
                print("<br> statement is now available")
                break

    def run(self):
        global statement_threads

        if simulation:
            self.simul()
        else:
            self.livy()

        print("thread> Statement {} is now Available session_id={} statement_id={}".format(self.statement, self.session_id, self.statement_id))

        socketio.emit('AvailableEvent', {'session_id': self.session_id,
                                         'statement_id': self.statement_id,
                                         'statement': self.statement,
                                         'result': self.result}, namespace='/test')


@app.route('/', methods=['GET', 'POST'])
def fink_dataset_monitor():
    out = render_template("open.html")
    return out

"""
Management of HTML sessions
"""
@socketio.on('connect', namespace='/test')
def test_connect():
    # need visibility of the global thread object
    print('Client connected')

@socketio.on('disconnect', namespace='/test')
def test_disconnect():
    print('Client disconnected')


"""
We are ready to open a new Livy session
"""
@app.route('/open', methods=['GET', 'POST'])
def open_session():
    print("Starting Thread")
    thread = SessionThread()
    session_id = thread.sessionLaunch()
    out = render_template("wait.html", session_id=session_id)
    return out

"""
The Livy session has been launched, wait until it becomes Idle
"""
@app.route('/wait', methods=['GET', 'POST'])
def wait_session():
    session_id = request.form["session_id"]
    out = render_template("session.html", session_id=session_id)
    return out

"""
return back from a IdleStatement (handled in Javascript)
"""
@app.route('/idle', methods=['GET', 'POST'])
def idle():
    session_id = int(request.form["session_id"])
    print("Received idle form session_id={} type={}".format(session_id, type(session_id)))
    out = render_template("session.html", session_id=session_id)
    return out

"""
Request to close the Livy session
"""
@app.route('/close', methods=['GET', 'POST'])
def close_session():
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
    statement_id = thread.statementLaunch(session_id, statement)

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


if __name__ == '__main__':
    # app.run(debug=True)
    import sys
    import re

    host = "127.0.0.1"
    port = 24701

    if len(sys.argv) >= 2:
        for arg in sys.argv:
            m = re.match(r"..port.([\d]+)", arg)
            if not m is None:
                port = m[1]
                continue

            m = re.match(r"..host.(.+)", arg)
            if not m is None:
                host = m[1]
                continue

    socketio.run(app, port=port)
