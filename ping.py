import socket
import os

# --- CONFIGURATION ---
HOST = '127.0.0.1'
PORT = 63790
PASSWORD = os.getenv('CARADE_PASSWORD', 'teasertopsecret')

# --- UTILITIES ---
def get_connection():
    """Establishes a connection to the Carade server and performs authentication."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((HOST, PORT))
        
        # Authenticate immediately
        s.sendall(f"AUTH {PASSWORD}\n".encode())
        resp = s.recv(1024).decode()
        
        if "OK" not in resp:
            print(f"❌ Auth Failed: {resp.strip()}")
            return None
        return s
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None

def send_cmd(sock, cmd):
    """Sends a command and returns the response string."""
    try:
        sock.sendall(f"{cmd}\n".encode())
        return sock.recv(1024).decode().strip()
    except Exception as e:
        print(f"❌ Command failed: {e}")
        return None

# --- MAIN ENTRY POINT ---
def ping_server():
    print(f"\n🏓 CARADE PING")
    print(f"Target: {HOST}:{PORT}")
    
    s = get_connection()
    if not s:
        print("❌ Cannot connect to Server. Is Carade running?")
        return
    
    print("Sending PING...", end=" ")
    response = send_cmd(s, "PING")
    
    if response and "PONG" in response:
        print(f"✅ {response}")
    else:
        print(f"❌ Unexpected response: {response}")
    
    s.close()

if __name__ == "__main__":
    ping_server()