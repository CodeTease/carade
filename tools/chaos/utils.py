import socket
import os
import time

# Configuration
HOST = "127.0.0.1"
PORT = 63790
PASSWORD = os.getenv("CARADE_PASSWORD", "teasertopsecret")


def connect(host=HOST, port=PORT, password=PASSWORD, retries=3, timeout=2.0):
    """Establishes a raw socket connection to Carade and authenticates with retries."""
    for attempt in range(retries):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((host, port))

            # Authenticate
            s.sendall(encode_resp(["AUTH", password]))
            response = read_resp(s.makefile("rb"))

            if response != "OK":
                print(f"Authentication failed: {response}")
                s.close()
                return None

            s.settimeout(None)  # Reset timeout for normal usage, caller can override
            return s
        except (ConnectionRefusedError, socket.timeout, OSError) as e:
            if attempt < retries - 1:
                time.sleep(1)
            else:
                print(f"Connection failed after {retries} attempts: {e}")
                return None


def encode_resp(args):
    """Encodes a list of arguments into a RESP Array byte string."""
    cmd = f"*{len(args)}\r\n"
    for arg in args:
        s_arg = str(arg)
        cmd += f"${len(s_arg.encode('utf-8'))}\r\n{s_arg}\r\n"
    return cmd.encode("utf-8")


def read_resp(f):
    """
    Recursively parses RESP data from a file-like object (socket.makefile).
    Returns nested Python lists for Arrays, strings for others.
    """
    try:
        line = f.readline()
        if not line:
            return None

        prefix = line[:1]
        payload = line[1:].strip()

        # Simple Strings (+), Errors (-), Integers (:)
        if prefix in (b"+", b"-", b":"):
            return payload.decode("utf-8", errors="replace")

        # Bulk Strings ($)
        if prefix == b"$":
            try:
                length = int(payload)
            except ValueError:
                return None

            if length == -1:
                return None
            data = f.read(length)
            f.read(2)  # Consume trailing CRLF
            return data.decode("utf-8", errors="replace")

        # Arrays (*)
        if prefix == b"*":
            try:
                count = int(payload)
            except ValueError:
                return None

            if count == -1:
                return None
            return [read_resp(f) for _ in range(count)]

        return line.strip().decode("utf-8", errors="replace")  # Fallback
    except Exception:
        # Debugging hook: enable if needed
        # sys.stderr.write(f"Error reading RESP: {e}\n")
        return None
