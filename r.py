import redis

# Config

HOST = 'localhost'
PORT = 63790
PASSWORD = 'teasertopsecret'
DB = 0 # Default DB

# Test Connection
r = redis.Redis(host=HOST, port=PORT, password=PASSWORD, db=DB)
r.ping()
print("Connected to Carade server successfully!")

# Simple REPL
while True:
    cmd = input("Enter Redis command (or 'exit' to quit): ")
    if cmd.lower() == 'exit':
        break
    try:
        parts = cmd.split()
        command = parts[0].upper()
        args = parts[1:]
        method = getattr(r, command.lower())
        result = method(*args)
        print(f"Result: {result}")
    except Exception as e:
        print(f"Error executing command: {e}")