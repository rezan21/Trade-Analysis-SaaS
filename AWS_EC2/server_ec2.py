import socket
from threading import Thread, activeCount
import ec2_process
from pickle import dumps as pickle_dumps, loads as pickle_loads
from time import sleep, perf_counter

# AWS SERVER CONFIG
FORMAT = 'utf-8'
HEADER = 64
PORT = 5555
SERVER = socket.gethostbyname(socket.gethostname()) # Local automatically will connect to AWS-ELB
ADDR = (SERVER, PORT)

attempts = 1
while attempts < 20:
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(ADDR)
        print("[SET UP DONE] attempts: ", attempts)
        break
    except OSError:
        print("[FAILURE] failed setup, attempt: ", attempts)
        attempts += 1
        sleep(3)


def handle_client(conn, addr):

    connected = True
    while connected:

        # receiving
        msg_length = conn.recv(HEADER).decode(FORMAT)

        if not msg_length: # ELB Health checks - Disconnect after ping
            print(f"[PING] {addr}")
            connected = False

        if msg_length: # first msg sent from client telling the server the length of upcoming msg
            print(f"[MESSAGE RECEIVED] {addr}")

            msg_length = int(msg_length)
            msg = b'' # user inputs from GAE
            while len(msg) < msg_length:
                msg += conn.recv(msg_length) # receive the whole main msg as we know the size of it

            user_inputs=pickle_loads(msg)

            # process received msg
            try:
                start_time = perf_counter()
                generated_res = ec2_process.generate_results(STOCK=user_inputs["STOCK"], A=user_inputs["A"], V=user_inputs["V"], S=user_inputs["S"], R=user_inputs["R"], Res_Type=user_inputs["Res_Type"]) # returns dict of pkls
                finish_time = perf_counter()
                print(f'[DONE CALCULATION] {addr} : Res_Type: {user_inputs["Res_Type"]}, R: {user_inputs["R"]}, Duration: {finish_time - start_time}')
                status = "OK"

            except:
                print(f"[FAILED CALCULATION] {addr}")
                status = "FAILED"
                

            if status=="OK":
                # sending results back
                s_msg_length = len(generated_res)
                s_send_length = str(s_msg_length).encode(FORMAT)
                s_send_length += b' ' * (HEADER - len(s_send_length))
                conn.send(s_send_length)
                conn.send(generated_res)
                connected = False

            else:
                # sending failure msg
                fail_msg = pickle_dumps(status)
                s_msg_length = len(fail_msg)
                s_send_length = str(s_msg_length).encode(FORMAT)
                s_send_length += b' ' * (HEADER - len(s_send_length))
                conn.send(s_send_length)
                conn.send(fail_msg)
                connected = False

    conn.close()
        

def start():
    server.listen()
    print(f"[LISTENING] Server is listening on {SERVER}\n")
    while True:
        conn, addr = server.accept()
        t = Thread(target=handle_client, args=(conn, addr))
        t.start()
        print(f"[ACTIVE CONNECTIONS] {activeCount() - 1}")


print("[STARTING] server is starting...")
start()
