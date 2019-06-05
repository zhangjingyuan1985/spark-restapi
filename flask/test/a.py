

from flask import Flask, render_template, redirect, url_for
from flask_socketio import SocketIO, send, emit

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'

socketio = SocketIO(app)

@app.route('/', methods=['GET', 'POST'])
def a():
    out = """
<!DOCTYPE html>
<html>
    <head>
        <meta charset="utf-8" />
        <title>Flaskr</title>
        <script src="//code.jquery.com/jquery-3.3.1.min.js"></script>
        <script type="text/javascript" src="//cdnjs.cloudflare.com/ajax/libs/socket.io/1.3.6/socket.io.min.js"></script>
        <script type="text/javascript">
            $(document).ready(function()
                {
                    var socket = io.connect('http://' + document.domain + ':' + location.port + '/');

                    socket.emit( 'my_event', {data: 'User Connected'}, namespace='/')

                    socket.on('my_event', function(msg) {console.log("Received number" + msg.number);});

                    var form = $('form').on( 'submit', function( e ) {
                        e.preventDefault()
                        let user_name = $( 'input.user_name' ).val()
                        let user_input = $( 'input.user_input' ).val()
                        socket.emit( 'my_event', {user_name : user_name, user_input : user_input}, namespace='/')
                        $( 'input.message' ).val( '' ).focus()
                    })
  
                }
            )
            
        </script>

    </head>
    <body>
        test
        <form action="" method="post">
            <input type="text" name="user_name"/>
            <input type="text" name="user_input"/>
            <button type="submit">Send a message</button>
        </form>
    </body>
</html>
    """
    return out

@app.route('/hello', methods=['GET', 'POST'])
def b():
    print("called hello")
    return redirect(url_for('a'))

@socketio.on('my_event', namespace='/')
def my_event(data, path=""):
    for k in data:
        v = data[k]
        print("{} = {}".format(k, v))
    print ("my event path={}".format(path))

@socketio.on('connect', namespace='/')
def connect():
    print ("Client connected to socketio")

@socketio.on('disconnect', namespace='/')
def test_disconnect():
    print('Client disconnected')


if __name__ == '__main__':

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

