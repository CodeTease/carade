import socket
import os
import sys

# Configuration
HOST = "127.0.0.1"
PORT = 63790
PASSWORD = os.getenv("CARADE_PASSWORD", "teasertopsecret")

def connect(host=HOST, port=PORT, password=PASSWORD):
    """Establishes a raw socket connection to Carade and authenticates."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, port))
        
        # Authenticate
        s.sendall(encode_resp(["AUTH", password]))
        response = read_resp(s.makefile("rb"))
        
        if response != "OK":
            print(f"Authentication failed: {response}")
            s.close()
            return None
            
        return s
    except Exception as e:
        print(f"Connection failed: {e}")
        return None

def encode_resp(args):
    """Encodes a list of arguments into a RESP Array byte string."""
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        s_arg = str(arg)
        cmd += f"${len(s_arg.encode('utf-8'))}\r\n{s_arg}\r\n"
    return cmd.encode("utf-8")

def read_resp(f):
    """
    Recursively parses RESP data from a file-like object (socket.makefile).
    Returns nested Python lists for Arrays, strings for others.
    """
    try:
        line = f.readline()
        if not line:
            return None

        prefix = line[:1]
        payload = line[1:].strip()

        # Simple Strings (+), Errors (-), Integers (:)
        if prefix in (b"+", b"-", b":"):
            return payload.decode()

        # Bulk Strings ($)
        if prefix == b"$":
            length = int(payload)
            if length == -1:
                return None
            data = f.read(length)
            f.read(2)  # Consume trailing CRLF
            return data.decode()

        # Arrays (*)
        if prefix == b"*":
            count = int(payload)
            if count == -1:
                return None
            return [read_resp(f) for _ in range(count)]

        return line.strip().decode()  # Fallback
    except Exception as e:
        # print(f"Error reading RESP: {e}")
        return None
