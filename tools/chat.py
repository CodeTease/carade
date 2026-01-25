import socket
import threading
import os
import sys

# --- CONFIG ---
HOST = '127.0.0.1'
PORT = 63790
PASSWORD = os.getenv('CARADE_PASSWORD', 'teasertopsecret')
CHANNEL = 'general'

# --- RESP PARSER (The "Better" Parser) ---
def read_resp(f):
    """
    Recursively parses RESP data. 
    This is critical for converting the server's raw ["message", "chan", "msg"] 
    response into a Python list we can format cleanly.
    """
    try:
        line = f.readline()
        if not line: return None
        
        prefix = line[:1]
        payload = line[1:].strip()

        # Simple Strings (+), Errors (-), Integers (:)
        if prefix in (b'+', b'-', b':'):
            return payload.decode()
        
        # Bulk Strings ($) - The meat of the message
        if prefix == b'$':
            length = int(payload)
            if length == -1: return None
            data = f.read(length)
            f.read(2) # Consume trailing CRLF
            return data.decode()
        
        # Arrays (*) - Recursion happens here
        if prefix == b'*':
            return [read_resp(f) for _ in range(int(payload))]
            
        return line.strip().decode() # Fallback
    except: return None

def encode_cmd(args):
    """
    Encodes a list of arguments into a RESP Array.
    This ensures 'Teaser: Hello World' is sent as one single argument,
    preventing quote issues.
    """
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        s_arg = str(arg)
        cmd += f"${len(s_arg.encode('utf-8'))}\r\n{s_arg}\r\n"
    return cmd.encode('utf-8')

# --- WORKER ---
def receive_messages(sock):
    """Listen for incoming messages from the server."""
    # Use makefile for safe line-by-line reading
    f = sock.makefile('rb') 
    try:
        while True:
            data = read_resp(f)
            if not data: break
            
            # Filter standard protocol responses
            if data == "OK" or data == "PONG": continue
            
            # Logic: If it's a PubSub list -> Print nicely
            if isinstance(data, list) and len(data) == 3 and data[0] == "message":
                # data[1] is channel, data[2] is content (e.g., "Name: Message")
                print(f"\r[{data[1]}] {data[2]}\n> ", end="")
            else:
                # System/Error messages
                print(f"\r[System] {data}\n> ", end="")
            
            sys.stdout.flush()
    except:
        print("\nDisconnected from server.")
        os._exit(0)

# --- MAIN ---
def main():
    name = input("Enter your name: ")
    print(f"Connecting to Carade at {HOST}:{PORT}...")

    # 1. Connection for PUBLISHING (Sender)
    try:
        sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sender.connect((HOST, PORT))
        sender.sendall(encode_cmd(["AUTH", PASSWORD]))
        sender.recv(1024) # Skip Auth OK
    except:
        print("❌ Could not connect (Sender).")
        return

    # 2. Connection for SUBSCRIBING (Listener)
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.connect((HOST, PORT))
        listener.sendall(encode_cmd(["AUTH", PASSWORD]))
        listener.recv(1024)
        
        # Subscribe using proper encoding
        listener.sendall(encode_cmd(["SUBSCRIBE", CHANNEL]))
        print(f"✅ Joined channel '{CHANNEL}'. Start typing!")
        
        t = threading.Thread(target=receive_messages, args=(listener,))
        t.daemon = True
        t.start()
    except:
        print("❌ Could not connect (Listener).")
        return

    # 3. Main Loop
    print("> ", end="")
    sys.stdout.flush()
    while True:
        try:
            msg = input()
            if msg.strip() == "/quit": break
            
            # Format: "Name: Message"
            full_msg = f"{name}: {msg}"
            
            # Send using RESP Encoder (Fixes the quote issue!)
            sender.sendall(encode_cmd(["PUBLISH", CHANNEL, full_msg]))
            
            # Clear sender buffer to stay in sync
            sender.recv(1024) 
            
            print("> ", end="")
            sys.stdout.flush()
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()