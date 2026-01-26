import os
import random
import argparse


def sabotage_aof(data_dir):
    path = os.path.join(data_dir, "appendonly.aof")
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return

    size = os.path.getsize(path)
    if size < 10:
        print("AOF file too small to truncate.")
        return

    # Truncate last few bytes (e.g., cut halfway through a command)
    # This simulates a crash during write.
    print(f"Truncating AOF: {path} (Size: {size})")
    with open(path, "rb+") as f:
        f.seek(-5, os.SEEK_END)
        f.truncate()

    print("AOF Truncated. Start server to verify recovery.")


def sabotage_rdb(data_dir):
    path = os.path.join(data_dir, "dump.rdb")
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return

    size = os.path.getsize(path)
    if size < 10:
        print("RDB file too small.")
        return

    print(f"Corrupting RDB: {path}")
    with open(path, "r+b") as f:
        # Flip some bytes in the middle
        pos = random.randint(10, size - 10)
        f.seek(pos)
        byte = f.read(1)
        # Flip bits
        corrupted = bytes([byte[0] ^ 0xFF])
        f.seek(pos)
        f.write(corrupted)
        print(f"Flipped byte at position {pos}.")


def simulate_disk_full(data_dir):
    print("Warning: This test attempts to create a large file 'disk_full_filler.dat'.")
    print("It respects available disk space but use with caution.")
    confirm = input("Type 'yes' to proceed: ")
    if confirm != "yes":
        return

    path = os.path.join(data_dir, "disk_full_filler.dat")
    try:
        with open(path, "wb") as f:
            while True:
                # Write 10MB chunks until error or stopped
                f.write(os.urandom(10 * 1024 * 1024))
    except OSError as e:
        print(f"Stopped filling: {e}")
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        if os.path.exists(path):
            print(f"Created {path}. Delete it to free space.")


def main():
    parser = argparse.ArgumentParser(description="Chaos tools for Storage")
    parser.add_argument(
        "--dir", default=".", help="Directory containing .aof and .rdb files"
    )
    parser.add_argument(
        "--action", choices=["aof-trunc", "rdb-flip", "disk-full"], required=True
    )

    args = parser.parse_args()

    print("⚠️  Ensure Carade server is STOPPED before running storage sabotage! ⚠️")

    if args.action == "aof-trunc":
        sabotage_aof(args.dir)
    elif args.action == "rdb-flip":
        sabotage_rdb(args.dir)
    elif args.action == "disk-full":
        simulate_disk_full(args.dir)


if __name__ == "__main__":
    main()
