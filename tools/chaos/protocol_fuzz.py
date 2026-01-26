import socket
import time
import sys
from utils import connect, encode_resp, read_resp, HOST, PORT, PASSWORD

def test_length_dishonesty():
    print("--- [Protocol] Testing Length Dishonesty ---")
    try:
        s = connect()
        if not s: return

        # Case 1: Fake huge array
        print("Sending *100000000 (fake array length)...")
        s.sendall(b"*100000000\r\n")
        time.sleep(1)
        # Send a valid ping inside to see if it processes or waits forever/crashes
        s.sendall(encode_resp(["PING"]))
        
        # We expect the server to likely buffer waiting for elements, or reject it if it exceeds limits.
        # Carade might not implement limit check on array length immediately.
        # We won't block forever here.
        s.settimeout(2)
        try:
            resp = read_resp(s.makefile("rb"))
            print(f"Response: {resp}")
        except socket.timeout:
            print("Server timed out (likely waiting for more data).")
        
        s.close()

        # Case 2: Fake huge bulk string
        s = connect()
        if not s: return
        print("Sending $100000000 (fake bulk length)...")
        s.sendall(b"$100000000\r\n")
        s.sendall(b"PING\r\n")
        time.sleep(1)
        s.close()
        print("Finished Length Dishonesty test.")

    except Exception as e:
        print(f"Error: {e}")

def test_recursive_depth():
    print("\n--- [Protocol] Testing Recursive Depth ---")
    try:
        s = connect()
        if not s: return

        depth = 5000
        print(f"Sending nested array of depth {depth}...")
        
        # *1\r\n *1\r\n ... $4\r\nPING\r\n
        payload = (b"*1\r\n" * depth) + b"$4\r\nPING\r\n"
        
        s.sendall(payload)
        
        s.settimeout(5)
        try:
            resp = read_resp(s.makefile("rb"))
            print(f"Response: {resp}") # Should hopefully be 'PING' or Error, not a crash.
        except socket.timeout:
            print("Server timed out.")
        except ConnectionResetError:
            print("Server closed connection (StackOverflow protection?).")
            
        s.close()
    except Exception as e:
        print(f"Error: {e}")

def test_partial_frames():
    print("\n--- [Protocol] Testing Partial Frames (Slowloris) ---")
    try:
        s = connect()
        if not s: return
        
        cmd = encode_resp(["PING"]) # *1\r\n$4\r\nPING\r\n
        print(f"Sending PING byte-by-byte with delay...")
        
        for i in range(len(cmd)):
            byte = cmd[i:i+1]
            s.sendall(byte)
            sys.stdout.write(".")
            sys.stdout.flush()
            time.sleep(0.5) # 0.5s per byte
            
        print("\nFinished sending.")
        s.settimeout(2)
        try:
            resp = read_resp(s.makefile("rb"))
            print(f"Response: {resp}")
        except socket.timeout:
            print("Server timed out.")
            
        s.close()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_length_dishonesty()
    test_recursive_depth()
    test_partial_frames()
