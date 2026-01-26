import socket
import time
import sys
from utils import connect, encode_resp, read_resp


def test_length_dishonesty():
    print("--- [Protocol] Testing Length Dishonesty ---")

    # Case 1: Fake huge array
    try:
        s = connect()
        if s:
            print("Sending *100000000 (fake array length)...")
            s.sendall(b"*100000000\r\n")
            time.sleep(1)
            # Send a valid ping inside to see if it processes or waits forever/crashes
            s.sendall(encode_resp(["PING"]))

            s.settimeout(2)
            try:
                resp = read_resp(s.makefile("rb"))
                print(f"Response: {resp}")
            except socket.timeout:
                print("Server timed out (expected behavior).")
            except Exception as e:
                print(f"Exception during read: {e}")
            s.close()
    except Exception as e:
        print(f"Case 1 Error: {e}")

    # Case 2: Fake huge bulk string
    try:
        s = connect()
        if s:
            print("Sending $100000000 (fake bulk length)...")
            s.sendall(b"$100000000\r\n")
            s.sendall(b"PING\r\n")
            time.sleep(1)
            s.close()
            print("Finished Length Dishonesty test.")
    except Exception as e:
        print(f"Case 2 Error: {e}")


def test_recursive_depth():
    print("\n--- [Protocol] Testing Recursive Depth ---")
    try:
        s = connect()
        if not s:
            return

        depth = 5000
        print(f"Sending nested array of depth {depth}...")

        # *1\r\n *1\r\n ... $4\r\nPING\r\n
        payload = (b"*1\r\n" * depth) + b"$4\r\nPING\r\n"

        s.sendall(payload)

        s.settimeout(5)
        try:
            resp = read_resp(s.makefile("rb"))
            print(f"Response: {resp}")
        except socket.timeout:
            print("Server timed out.")
        except ConnectionResetError:
            print("Server closed connection (StackOverflow protection?).")
        except Exception as e:
            print(f"Read error: {e}")

        s.close()
    except Exception as e:
        print(f"Error: {e}")


def test_partial_frames():
    print("\n--- [Protocol] Testing Partial Frames (Slowloris) ---")
    try:
        s = connect()
        if not s:
            return

        cmd = encode_resp(["PING"])
        print("Sending PING byte-by-byte with delay...")

        for i in range(len(cmd)):
            byte = cmd[i : i + 1]
            try:
                s.sendall(byte)
                sys.stdout.write(".")
                sys.stdout.flush()
                time.sleep(0.5)
            except OSError as e:
                print(f"\nSend failed (Connection closed?): {e}")
                break

        print("\nFinished sending loop.")
        s.settimeout(2)
        try:
            resp = read_resp(s.makefile("rb"))
            print(f"Response: {resp}")
        except socket.timeout:
            print("Server timed out.")
        except Exception as e:
            print(f"Read error: {e}")

        s.close()
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    test_length_dishonesty()
    test_recursive_depth()
    test_partial_frames()
