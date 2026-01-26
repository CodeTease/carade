import socket
import time
import threading
import os

# --- CONFIGURATION ---
HOST = "127.0.0.1"
PORT = 63790
PASSWORD = os.getenv("CARADE_PASSWORD", "teasertopsecret")

# Stress Test Configuration
CONCURRENT_CLIENTS = 50
REQUESTS_PER_CLIENT = 1000


# --- RESP PARSER (Improved) ---
def read_resp(f):
    try:
        line = f.readline()
        if not line:
            return None
        p = line[:1]

        if p in (b"+", b"-", b":"):
            return line[1:].strip().decode()

        if p == b"$":
            l = int(line[1:].strip())
            if l == -1:
                return None
            data = f.read(l)
            f.read(2)  # Skip CRLF
            return data.decode()

        if p == b"*":
            return [read_resp(f) for _ in range(int(line[1:].strip()))]

        return None
    except:
        return None


# --- UTILITIES ---
def get_connection():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((HOST, PORT))
        f = s.makefile("rb")

        s.sendall(f"AUTH {PASSWORD}\r\n".encode())
        resp = read_resp(f)
        if resp == "OK":
            return s, f
        return None, None
    except:
        return None, None


def send_cmd(sock, f, cmd):
    try:
        sock.sendall(f"{cmd}\r\n".encode())
        return read_resp(f)
    except:
        return None


# --- FEATURE TESTS (Restored Logic) ---
def test_new_features():
    print("🧪 Verifying system features (TTL & Expiry)...")
    conn = get_connection()
    if not conn[0]:
        return False
    s, f = conn

    # 1. Basic Check
    send_cmd(s, f, "SET test_key benchmark_val")
    val = send_cmd(s, f, "GET test_key")
    if val != "benchmark_val":
        print(f"❌ Basic GET failed. Got: {val}")
        return False

    # 2. TTL Test (Your original logic)
    send_cmd(s, f, "EXPIRE test_key 1")
    ttl = send_cmd(s, f, "TTL test_key")
    print(f"   Initial TTL: {ttl}s")

    print("   Waiting for expiry (1.5s)...")
    time.sleep(1.5)

    expired_val = send_cmd(s, f, "GET test_key")
    if expired_val is not None:
        print(f"❌ Expiry failed: Key still exists -> {expired_val}")
        return False

    print("✅ Feature tests passed.")
    s.close()
    return True


# --- WORKER ---
def stress_worker(thread_id, success_counter):
    conn = get_connection()
    if not conn[0]:
        return
    s, f = conn

    for i in range(REQUESTS_PER_CLIENT):
        key = f"bench:{thread_id}:{i}"

        # SET
        s.sendall(f"SET {key} val_{i}\r\n".encode())
        read_resp(f)

        # GET
        s.sendall(f"GET {key}\r\n".encode())
        read_resp(f)

        success_counter[0] += 1

    s.close()


# --- MAIN ---
def run_benchmark():
    print("\n🏋️  CARADE BENCHMARK")
    print(f"Target: {HOST}:{PORT}")

    if not test_new_features():
        return

    print("\n🚀 Starting stress test...")
    start_time = time.time()

    threads = []
    success_counter = [0]

    for i in range(CONCURRENT_CLIENTS):
        t = threading.Thread(target=stress_worker, args=(i, success_counter))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    duration = time.time() - start_time
    total = success_counter[0] * 2

    print("\n" + "=" * 30)
    print("🔥 FINAL RESULTS")
    print(f"✅ Total Ops:    {total:,}")
    print(f"⏱️  Duration:     {duration:.2f}s")
    print(f"🚀 Throughput:   {total / duration:,.0f} ops/sec")
    print("=" * 30 + "\n")


if __name__ == "__main__":
    run_benchmark()
