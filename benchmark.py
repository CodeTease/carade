import socket
import time
import threading
import random
import os

# --- CONFIGURATION (Feel free to modify) ---
HOST = '127.0.0.1'
PORT = 63790
# Load password from ENV or use default (Insecure default for local dev)
PASSWORD = os.getenv('CARADE_PASSWORD', 'teasertopsecret') 

# Stress Test Configuration
CONCURRENT_CLIENTS = 50     # Number of concurrent threads (simulated users)
REQUESTS_PER_CLIENT = 1000  # Requests per thread
# Total requests = 50 * 1000 = 50,000

# --- UTILITIES ---
def get_connection():
    """Establishes a connection to the Carade server and performs authentication."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5) # 5s timeout avoids hanging indefinitely
        s.connect((HOST, PORT))
        
        # Authenticate immediately
        s.sendall(f"AUTH {PASSWORD}\n".encode())
        resp = s.recv(1024).decode()
        
        if "OK" not in resp:
            print(f"❌ Auth Failed: {resp.strip()}")
            return None
        return s
    except Exception as e:
        # Silently fail usually, or uncomment next line for debug
        # print(f"Connection failed: {e}") 
        return None

def send_cmd(sock, cmd):
    """Sends a command and returns the raw response string."""
    try:
        sock.sendall(f"{cmd}\n".encode())
        return sock.recv(4096).decode().strip()
    except:
        return None

# --- FEATURE VERIFICATION ---
def test_new_features():
    """Checks if the server supports v0.1.0 'Universe' features (Quotes, TTL)."""
    print("\n🕵️  FEATURE INSPECTION")
    print("-" * 50)
    
    s = get_connection()
    if not s:
        print("❌ Cannot connect to Server. Is Carade running?")
        return False

    # 1. Test Quotes Parser (Handling spaces in strings)
    print("🔹 Testing Quotes Parser...", end=" ")
    quote_val = "CodeTease Platform Inc"
    send_cmd(s, f'SET test:quote "{quote_val}"')
    res = send_cmd(s, "GET test:quote")
    
    if f'"{quote_val}"' in res or quote_val in res:
        print("✅ PASSED")
    else:
        print(f"❌ FAILED (Got: {res})")

    # 2. Test TTL (Time-To-Live / Expiration)
    print("🔹 Testing TTL (Expire)...  ", end=" ")
    send_cmd(s, "SET test:ttl 'I_will_die' EX 1") # Should expire in 1s
    val_immediate = send_cmd(s, "GET test:ttl")
    
    if "nil" in val_immediate:
         print("❌ FAILED (Key was not set correctly)")
    else:
        time.sleep(1.2) # Wait for death
        val_after = send_cmd(s, "GET test:ttl")
        if "(nil)" in val_after:
            print("✅ PASSED (Key expired as expected)")
        else:
            print(f"❌ FAILED (Key is still alive: {val_after})")
            
    s.close()
    print("-" * 50)
    return True

# --- WORKER THREAD ---
def stress_worker(thread_id, success_counter):
    """Worker function to simulate a single active user."""
    s = get_connection()
    if not s: return

    for i in range(REQUESTS_PER_CLIENT):
        key = f"usr:{thread_id}:{i}"
        val = f"data_{random.randint(1000,9999)}"
        
        # Randomly inject TTL commands to stress the expiration logic
        if i % 10 == 0:
            cmd = f"SET {key} {val} EX 60"
        else:
            cmd = f"SET {key} {val}"
            
        res = send_cmd(s, cmd)
        if res and "OK" in res:
            success_counter[0] += 1
            
        # Occasionally read back data to simulate real-world read/write mix
        if i % 5 == 0:
            send_cmd(s, f"GET {key}")

    s.close()

# --- MAIN ENTRY POINT ---
def run_benchmark():
    print(f"\n🚀 CARADE STRESS TESTER (International Edition)")
    print(f"Target: {HOST}:{PORT}")
    print(f"Auth:   {'Env Configured' if os.getenv('CARADE_PASSWORD') else 'Default (Insecure)'}")
    
    # Verify features first
    if not test_new_features():
        return

    print(f"\n🏋️  STARTING STRESS TEST")
    print(f"Threads: {CONCURRENT_CLIENTS} | Reqs/Thread: {REQUESTS_PER_CLIENT}")
    print(f"Total Requests: {CONCURRENT_CLIENTS * REQUESTS_PER_CLIENT:,}")
    print("Sending payload... Please wait...")

    start_time = time.time()
    
    threads = []
    success_counter = [0] # List used for pass-by-reference mutability
    
    # Spawn Threads (Simulate concurrent users)
    for i in range(CONCURRENT_CLIENTS):
        t = threading.Thread(target=stress_worker, args=(i, success_counter))
        threads.append(t)
        t.start()
        
    # Wait for all threads to finish
    for t in threads:
        t.join()

    end_time = time.time()
    duration = end_time - start_time
    # Estimate total operations (SETs + interleaved GETs)
    total_reqs = CONCURRENT_CLIENTS * REQUESTS_PER_CLIENT 
    rps = total_reqs / duration

    print("\n" + "="*30)
    print(f"🔥 FINAL RESULTS")
    print(f"✅ Successful SETs: {success_counter[0]:,}")
    print(f"⏱️ Time Taken:      {duration:.2f}s")
    print(f"🚀 Throughput:      {rps:,.0f} req/s")
    print("="*30)
    print("Note: This metric is client-bound (Python GIL limitation), not Server-bound.")

if __name__ == "__main__":
    run_benchmark()