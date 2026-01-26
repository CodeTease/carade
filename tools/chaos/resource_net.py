import threading
import time
import socket
import sys
from utils import connect, encode_resp, read_resp

def test_pubsub_backpressure():
    print("--- [Resource] Testing Pub/Sub Backpressure ---")
    
    CHANNEL = "chaos_chan"
    stop_event = threading.Event()
    
    def publisher():
        s = connect()
        if not s: return
        i = 0
        while not stop_event.is_set():
            msg = "X" * 1000 # 1KB payload
            s.sendall(encode_resp(["PUBLISH", CHANNEL, msg]))
            # No wait, fire and forget (we might need to read response to not block buffer? 
            # Actually standard redis publish returns integer, we should read it to keep protocol sync)
            s.recv(1024) 
            i += 1
            if i % 1000 == 0:
                print(f"Published {i} messages...")
        s.close()

    def slow_subscriber():
        s = connect()
        if not s: return
        s.sendall(encode_resp(["SUBSCRIBE", CHANNEL]))
        
        # Read one byte every second
        print("Subscriber started (reading slowly)...")
        while not stop_event.is_set():
            try:
                # We just peek or read 1 byte to keep socket open but buffer full on server side
                s.recv(1)
                time.sleep(1) 
            except:
                break
        s.close()
        
    t_pub = threading.Thread(target=publisher)
    t_sub = threading.Thread(target=slow_subscriber)
    
    t_sub.start()
    time.sleep(1) # Let sub connect
    t_pub.start()
    
    print("Running backpressure test for 10 seconds...")
    time.sleep(10)
    stop_event.set()
    t_pub.join()
    t_sub.join()
    print("Backpressure test finished.")

def test_lua_exhaustion():
    print("\n--- [Resource] Testing Lua CPU Exhaustion ---")
    try:
        s = connect()
        if not s: return
        
        print("Sending Infinite Loop Script...")
        # EVAL "while true do end" 0
        s.sendall(encode_resp(["EVAL", "while true do end", "0"]))
        
        # Check if server kills it or we timeout
        s.settimeout(5)
        try:
            res = read_resp(s.makefile("rb"))
            print(f"Response: {res}") 
        except socket.timeout:
            print("Server timed out (Script is running indefinitely?)")
            
        s.close()
    except Exception as e:
        print(f"Error: {e}")

def test_rapid_reconnects():
    print("\n--- [Network] Testing Rapid Reconnects ---")
    COUNT = 1000
    start = time.time()
    errors = 0
    
    for i in range(COUNT):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(("127.0.0.1", 63790))
            s.close()
        except:
            errors += 1
            
        if i % 100 == 0:
            sys.stdout.write(".")
            sys.stdout.flush()
            
    end = time.time()
    print(f"\n{COUNT} connections in {end - start:.2f}s. Errors: {errors}")

def test_zombie_connections():
    print("\n--- [Network] Testing Zombie Connections ---")
    CONNECTIONS = 100
    sockets = []
    
    print(f"Opening {CONNECTIONS} idle connections...")
    for i in range(CONNECTIONS):
        s = connect()
        if s:
            sockets.append(s)
            
    print(f"Holding {len(sockets)} connections open for 10 seconds...")
    time.sleep(10)
    
    for s in sockets:
        s.close()
    print("Closed all connections.")

if __name__ == "__main__":
    # Optional arg to run specific test
    if len(sys.argv) > 1:
        if sys.argv[1] == "pubsub": test_pubsub_backpressure()
        elif sys.argv[1] == "lua": test_lua_exhaustion()
        elif sys.argv[1] == "reconnect": test_rapid_reconnects()
        elif sys.argv[1] == "zombie": test_zombie_connections()
    else:
        test_pubsub_backpressure()
        test_lua_exhaustion()
        test_rapid_reconnects()
        test_zombie_connections()
