import socket
import time

# CONFIG
HOST = '127.0.0.1'
PORT = 63790
PASSWORD = 'teasertopsecret'
NUM_REQUESTS = 50000 
BATCH_SIZE = 500    # Pipeline batch size

def run_benchmark():
    print(f"⚖️  STARTING THE 'HONEST' BENCHMARK")
    print(f"Target: {HOST}:{PORT} | Reqs: {NUM_REQUESTS}")

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))
        
        # AUTH
        s.sendall(f"AUTH {PASSWORD}\n".encode())
        s.recv(1024) # Clear buffer

        # PREPARE PAYLOAD
        payload = ""
        for i in range(BATCH_SIZE):
            payload += f"SET key_{i} val_{i}\n"
        payload_bytes = payload.encode()
        
        # Số lần cần gửi batch
        batches = NUM_REQUESTS // BATCH_SIZE

        print("🚀 Sending and WAITING for all responses...")
        start_time = time.time()
        
        total_received_lines = 0
        
        # Chúng ta gửi từng batch và đợi nhận đủ phản hồi của batch đó
        # (Để tránh tràn buffer TCP nếu gửi 1 cục 50k)
        for _ in range(batches):
            s.sendall(payload_bytes)
            
            # Đọc phản hồi cho đến khi đủ số dòng của batch
            received_in_batch = 0
            while received_in_batch < BATCH_SIZE:
                chunk = s.recv(65536) # Đọc chunk lớn
                if not chunk: break
                # Đếm số dòng (số chữ OK)
                received_in_batch += chunk.count(b'\n')
            
            total_received_lines += received_in_batch

        end_time = time.time()
        duration = end_time - start_time
        rps = NUM_REQUESTS / duration

        print("\n" + "="*30)
        print(f"✅ VERIFIED RESULT (Full Round-Trip)")
        print(f"✅ Processed: {total_received_lines} responses")
        print(f"✅ Time:      {duration:.4f}s")
        print(f"✅ True RPS:  {rps:,.2f} req/s")
        print("="*30)
        
        s.close()

    except Exception as e:
        print(f"💥 Error: {e}")

if __name__ == "__main__":
    run_benchmark()