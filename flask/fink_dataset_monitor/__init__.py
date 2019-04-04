#! /usr/bin/python
# -*- coding:utf-8 -*-

from flask import Flask, request, render_template
app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def fink_dataset_monitor():
    out = render_template("open.html")
    return out


@app.route('/open', methods=['GET', 'POST'])
@app.route('/wait', methods=['GET', 'POST'])
def open_wait_session():
    if "session_id" in request.form:
        session_id = request.form["session_id"]
        wait_for_idle = int(request.form["wait_for_idle"])
        wait_for_idle += 1
    else:
        session_id = 1
        wait_for_idle = 0

    if wait_for_idle < 3:
        out = render_template("wait.html", session_id=session_id, wait_for_idle=str(wait_for_idle))
    else:
        out = render_template("session.html", session_id=session_id)

    return out


@app.route('/session', methods=['GET', 'POST'])
def session():
    if "session_id" in request.form:
        session_id = request.form["session_id"]
        out = render_template("session.html", session_id=session_id)
    else:
        out = render_template("open.html")

    return out


@app.route('/close', methods=['GET', 'POST'])
def close_session():
    if "session_id" in request.form:
        session_id = request.form["session_id"]

    out = render_template("open.html")

    return out

@app.route('/statementwait', methods=['GET', 'POST'])
@app.route('/statementopen', methods=['GET', 'POST'])
def open_wait_statement():
    session_id = request.form["session_id"]

    if "statement" in request.form:
        statement = request.form["statement"]
    else:
        statement = ""

    if "wait_for_available" in request.form:
        statement_id = request.form["statement_id"]
        wait_for_available = int(request.form["wait_for_available"])
        wait_for_available += 1
    else:
        statement_id = 1
        wait_for_available = 0

    if wait_for_available < 5:
        out = render_template("wait_statement.html",
                              session_id=session_id,
                              statement=statement,
                              statement_id=statement_id,
                              wait_for_available=str(wait_for_available))
    else:
        result = "ok"
        out = render_template("statement.html",
                              session_id=session_id,
                              statement=statement,
                              result=result,
                              statement_id=statement_id)

    return out


if __name__ == '__main__':
    app.run(debug=True)
