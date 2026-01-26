import threading
import time
import random
from utils import connect, encode_resp, read_resp

def test_thundering_herd():
    print("--- [Concurrency] Testing Thundering Herd (WATCH/EXEC) ---")
    
    KEY = "thundering_key"
    CLIENTS = 50 # Not 10000 because Python threads are heavy, but enough to cause contention.
    success_count = 0
    lock = threading.Lock()

    # Reset key
    s = connect()
    s.sendall(encode_resp(["SET", KEY, "0"]))
    read_resp(s.makefile("rb"))
    s.close()

    def client_task(i):
        nonlocal success_count
        try:
            c = connect()
            if not c: return
            f = c.makefile("rb")
            
            # WATCH
            c.sendall(encode_resp(["WATCH", KEY]))
            read_resp(f)
            
            # GET
            c.sendall(encode_resp(["GET", KEY]))
            val = read_resp(f)
            
            # MULTI
            c.sendall(encode_resp(["MULTI"]))
            read_resp(f)
            
            # INCR
            c.sendall(encode_resp(["INCR", KEY]))
            read_resp(f) # QUEUED
            
            # Sleep tiny bit to increase collision window
            time.sleep(random.uniform(0.001, 0.01))
            
            # EXEC
            c.sendall(encode_resp(["EXEC"]))
            res = read_resp(f)
            
            if res and isinstance(res, list): # Transaction succeeded
                with lock:
                    success_count += 1
            
            c.close()
        except Exception as e:
            # print(f"Client {i} failed: {e}")
            pass

    threads = []
    print(f"Spawning {CLIENTS} clients...")
    for i in range(CLIENTS):
        t = threading.Thread(target=client_task, args=(i,))
        threads.append(t)
        t.start()
        
    for t in threads:
        t.join()
        
    print(f"Thundering Herd finished. Successful transactions: {success_count}/{CLIENTS}")

def test_eviction_race():
    print("\n--- [Concurrency] Testing Race against Eviction ---")
    
    stop_event = threading.Event()
    KEY = "race_key"
    
    def setter():
        s = connect()
        if not s: return
        f = s.makefile("rb")
        while not stop_event.is_set():
            # Set with 1ms expiry
            s.sendall(encode_resp(["SET", KEY, "val", "PX", "1"]))
            read_resp(f)
            time.sleep(0.0001)
        s.close()
        
    def getter():
        s = connect()
        if not s: return
        f = s.makefile("rb")
        hits = 0
        misses = 0
        while not stop_event.is_set():
            s.sendall(encode_resp(["GET", KEY]))
            res = read_resp(f)
            if res: hits += 1
            else: misses += 1
        s.close()
        print(f"Getter finished. Hits: {hits}, Misses: {misses}")

    t1 = threading.Thread(target=setter)
    t2 = threading.Thread(target=getter)
    
    t1.start()
    t2.start()
    
    time.sleep(3)
    stop_event.set()
    t1.join()
    t2.join()

def test_interleaved_transactions():
    print("\n--- [Concurrency] Testing Interleaved Transactions ---")
    
    try:
        # Client A
        cA = connect()
        fA = cA.makefile("rb")
        
        # Client B
        cB = connect()
        fB = cB.makefile("rb")
        
        print("Client A: MULTI")
        cA.sendall(encode_resp(["MULTI"]))
        print(f"A: {read_resp(fA)}")
        
        print("Client A: SET keyA valA")
        cA.sendall(encode_resp(["SET", "keyA", "valA"]))
        print(f"A: {read_resp(fA)}") # QUEUED
        
        print("Client B: SET keyB valB (Should execute immediately)")
        cB.sendall(encode_resp(["SET", "keyB", "valB"]))
        resB = read_resp(fB)
        print(f"B: {resB}")
        
        if resB != "OK":
            print("FAILURE: Client B blocked or failed.")
        
        print("Client A: EXEC")
        cA.sendall(encode_resp(["EXEC"]))
        resA = read_resp(fA)
        print(f"A: {resA}")
        
        # Verify
        cB.sendall(encode_resp(["GET", "keyA"]))
        valA = read_resp(fB)
        cB.sendall(encode_resp(["GET", "keyB"]))
        valB = read_resp(fB)
        
        print(f"Verification: keyA={valA}, keyB={valB}")
        
        cA.close()
        cB.close()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_thundering_herd()
    test_eviction_race()
    test_interleaved_transactions()
