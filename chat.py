import socket
import threading
import sys
import os

# --- CONFIG ---
HOST = '127.0.0.1'
PORT = 63790
PASSWORD = os.getenv('CARADE_PASSWORD', 'teasertopsecret')
CHANNEL = 'general' # Default channel

def receive_messages(sock):
    """Listen for incoming messages from the server."""
    try:
        while True:
            data = sock.recv(4096).decode()
            if not data: break
            # Filter out PONG or OK if they slip in
            if data.strip() == "OK" or data.strip() == "PONG": continue
            print(f"\r{data.strip()}\n> ", end="")
    except:
        print("\nDisconnected from server.")
        os._exit(0)

def main():
    name = input("Enter your name: ")
    print(f"Connecting to Carade at {HOST}:{PORT}...")

    # 1. Connection for PUBLISHING (Sending)
    try:
        sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sender.connect((HOST, PORT))
        sender.sendall(f"AUTH {PASSWORD}\n".encode())
        sender.recv(1024) # Eat Auth OK
    except:
        print("❌ Could not connect (Sender). Check server.")
        return

    # 2. Connection for SUBSCRIBING (Listening)
    try:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.connect((HOST, PORT))
        listener.sendall(f"AUTH {PASSWORD}\n".encode())
        listener.recv(1024) # Eat Auth OK
        
        # Subscribe
        listener.sendall(f"SUBSCRIBE {CHANNEL}\n".encode())
        print(f"✅ Joined channel '{CHANNEL}'. Start typing!")
        
        # Start listening thread
        t = threading.Thread(target=receive_messages, args=(listener,))
        t.daemon = True
        t.start()

    except:
        print("❌ Could not connect (Listener).")
        return

    # 3. Main Loop (Sending messages)
    print("> ", end="")
    while True:
        try:
            msg = input()
            if msg.strip() == "/quit": break
            # Publish format: PUBLISH channel "Name: Message"
            safe_msg = f"{name}: {msg}"
            sender.sendall(f'PUBLISH {CHANNEL} "{safe_msg}"\n'.encode())
            # We don't read response from sender to keep UI clean, or we could.
            sender.recv(1024) # Clear buffer (expected integer)
            print("> ", end="")
        except KeyboardInterrupt:
            break

    sender.close()
    listener.close()
    print("\nBye!")

if __name__ == "__main__":
    main()