import socket
from form import SimpleForm
from flask import Flask, render_template, url_for, request
from pickle import dumps as pickle_dumps, loads as pickle_loads

# AWS - ELB
AWS_ELB = "YOUR_AWS_ELB.us-west-1.elb.amazonaws.com"

# AWS SERVER CONFIG
FORMAT = 'utf-8'
HEADER = 64
PORT = 5555
ADDR = (AWS_ELB, PORT)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'YOUR_SECRET_KEY'

# socket server: send user inputs to compute server and return the result
def communicate(msg, client):

    # sending
    msg_length = len(msg)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length)) # padding
    client.send(send_length) # tell the server the size of msg that is going to be sent
    client.send(msg) # send the actual msg

    # receiving
    r_msg_length = client.recv(HEADER).decode(FORMAT)
    if r_msg_length:
            r_msg_length = int(r_msg_length)
            r_msg = b''
            while len(r_msg) < r_msg_length:
                r_msg += client.recv(r_msg_length) # receive the whole main msg as we know the size of it

    return r_msg


@app.route("/", methods=['GET', 'POST'])
def home():

    form = SimpleForm()
    if form.validate_on_submit():

        # Form Inputs
        user_inputs = pickle_dumps({
            "STOCK":request.form["Asset"],
            "A":int(request.form["A_Input"]),
            "V":int(request.form["V_Input"]),
            "S":int(request.form["S_Input"]),
            "R":int(request.form["R_Input"]),
            "Res_Type":request.form["Res_Type"]
            })

        # establish new connection
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(ADDR)
            print(f"** >> GAE: going to EC2 via ELB... << **\n")
            res = communicate(user_inputs, client)
            res = pickle_loads(res)
        except (ConnectionRefusedError, UnboundLocalError):
            return render_template('home.html',form=form, error_="Server Down. Please try again in a few minutes.")


        if res=="FAILED":
            return render_template('home.html',form=form, error_="Parameters values too large.")

        else:
            return render_template('home.html',form=form, plot=res["plot"], table=res["table"], summary=res["summary"])

    return render_template('home.html', form=form)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=False)



