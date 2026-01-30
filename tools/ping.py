import os
import socket

# --- CONFIGURATION ---
HOST = "127.0.0.1"
PORT = 63790
PASSWORD = os.getenv("CARADE_PASSWORD", "teasertopsecret")


def read_resp(f):
    """
    Parses RESP responses properly.
    """
    try:
        line = f.readline()
        if not line:
            return None
        p = line[:1]
        payload = line[1:].strip()

        if p in (b"+", b"-", b":"):
            return payload.decode()
        if p == b"$":
            l = int(payload)
            if l == -1:
                return None
            data = f.read(l)
            f.read(2)  # Skip CRLF
            return data.decode()
        if p == b"*":
            return [read_resp(f) for _ in range(int(payload))]
        return line.strip().decode()
    except:
        return None


def get_connection():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        s.connect((HOST, PORT))
        f = s.makefile("rb")

        # Auth with CRLF
        s.sendall(f"AUTH {PASSWORD}\r\n".encode())
        resp = read_resp(f)

        if resp != "OK":
            print(f"❌ Auth Failed: {resp}")
            return None, None
        return s, f
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return None, None


def send_cmd(sock, f, cmd):
    try:
        sock.sendall(f"{cmd}\r\n".encode())
        return read_resp(f)
    except Exception as e:
        print(f"❌ Command failed: {e}")
        return None


def ping_server():
    print("\n🏓 CARADE PING")
    print(f"Target: {HOST}:{PORT}")

    conn = get_connection()
    if not conn[0]:
        print("❌ Cannot connect to Server. Is Carade running?")
        return exit(1)

    s, f = conn
    print("Sending PING...", end=" ")
    response = send_cmd(s, f, "PING")

    if response == "PONG":
        print(f"✅ {response}")
    else:
        print(f"❌ Unexpected response: {response}")

    s.close()


if __name__ == "__main__":
    ping_server()
