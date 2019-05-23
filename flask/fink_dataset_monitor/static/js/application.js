function availablepost(session_id, statement_id, statement, result) {
    method = "post"; // Set method to post by default if not specified.

    var fields = {
        "session_id": session_id,
        "statement_id": statement_id,
        "statement": statement,
        "result": result
        };

    var form = document.createElement("form");
    form.setAttribute("method", method);
    form.setAttribute("action", "/available");

    for(var key in fields) {
        var value = fields[key];

        var field = document.createElement("input");
        field.setAttribute("type", "hidden");
        field.setAttribute("name", key);
        field.setAttribute("value", value);

        form.appendChild(field);
    }

    document.body.appendChild(form);

    //$('#log').html('<br>== form === <br>');

    form.submit();
}

function idlepost(session_id) {
    method = "post"; // Set method to post by default if not specified.

    var fields = {
        "session_id": session_id
        };

    var form = document.createElement("form");
    form.setAttribute("method", method);
    form.setAttribute("action", "/idle");

    for(var key in fields) {
        var value = fields[key];

        var field = document.createElement("input");
        field.setAttribute("type", "hidden");
        field.setAttribute("name", key);
        field.setAttribute("value", value);

        form.appendChild(field);
    }

    document.body.appendChild(form);

    //$('#log').html('<br> == form === <br>');

    form.submit();
}

$(document).ready(function(){
    //connect to the socket server.
    var socket = io.connect('http://' + document.domain + ':' + location.port + '/test');

    socket.on('IdleEvent', function(msg) {
        console.log("from socket.on Session id" + msg.session_id);

        $('#log').html('<br>== Idle === <br>');
        idlepost(msg.session_id);
    });

    socket.on('AvailableEvent', function(msg) {
        //console.log("from socket.on Statement id" + msg.statement_id);

        //('#log').html('<br>== Available === <br>');
        availablepost(msg.session_id, msg.statement_id, msg.statement, msg.result);
    });

});


