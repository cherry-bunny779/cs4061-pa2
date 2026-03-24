import os
import shutil
import subprocess
import hashlib
import sys

# --- Configuration ---
MGIT_EXEC = "./mgit"
TEST_DIR = "test_env"
VALGRIND_LOG_SEND = "valgrind_send.log"
VALGRIND_LOG_RECV = "valgrind_recv.log"
TIMEOUT = 10

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    CYAN = '\033[96m'

def log(msg): print(f"{Colors.OKBLUE}[TEST] {msg}{Colors.ENDC}")
def error(msg): print(f"{Colors.FAIL}[FAIL] {msg}{Colors.ENDC}")
def success(msg): print(f"{Colors.OKGREEN}[PASS] {msg}{Colors.ENDC}")

# Global state to track commands for debugging output
class DebugContext:
    history = []

    @classmethod
    def reset(cls):
        cls.history = []

    @classmethod
    def print_trail(cls, cwd):
        print(f"\n{Colors.WARNING}--- DEBUGGING TRACE ---{Colors.ENDC}")
        print("Commands executed right before failure:")
        for cmd in cls.history:
            print(f"  {Colors.CYAN}{cmd}{Colors.ENDC}")
        print(f"\n👉 {Colors.WARNING}HOW TO DEBUG MANUALLY:{Colors.ENDC}")
        print(f"  1. Run: cd {cwd}")
        print(f"  2. Inspect the files, or run the last command above through gdb/valgrind.")
        print("-" * 50)

def calculate_checksum(filepath):
    sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256.update(byte_block)
    return sha256.hexdigest()

def clean_env():
    for d in [TEST_DIR, "sender_env", "receiver_env", "restore_test", "valgrind_restore"]:
        if os.path.exists(d): shutil.rmtree(d)
    for f in [VALGRIND_LOG_SEND, VALGRIND_LOG_RECV]:
        if os.path.exists(f): os.remove(f)

def setup_env(directory=TEST_DIR):
    if os.path.exists(directory): shutil.rmtree(directory)
    os.makedirs(directory)
    DebugContext.reset()

def run_command(cmd, cwd=TEST_DIR, enforce_silence=True, shell=False, expect_fail=False):
    cmd_str = cmd if isinstance(cmd, str) else " ".join(cmd)
    DebugContext.history.append(f"[{cwd}]$ {cmd_str}")

    try:
        result = subprocess.run(
            cmd, cwd=cwd, shell=shell, capture_output=True, timeout=TIMEOUT
        )

        if expect_fail:
            if result.returncode == 0:
                raise RuntimeError("Expected command to fail, but it succeeded.\n💡 HINT: Your program should exit with a non-zero code on bad input.")
            if len(result.stderr) == 0:
                raise RuntimeError("Failed command must print diagnostic to stderr.\n💡 HINT: Check the 'Silence Rule'. Errors MUST go to stderr.")
            return result

        if result.returncode != 0:
            raise RuntimeError(f"Program Crashed or returned Error Code {result.returncode}.\n💡 STDERR OUTPUT:\n{result.stderr.decode()}")

        if enforce_silence:
            if len(result.stdout) > 0 or len(result.stderr) > 0:
                msg = f"Violation of The Silence Rule. Output detected on successful operation.\n"
                msg += f"💡 HINT: Remove stray printf() calls. Good UNIX tools are silent on success.\n"
                if result.stdout: msg += f"STDOUT: {result.stdout.decode()}\n"
                if result.stderr: msg += f"STDERR: {result.stderr.decode()}\n"
                raise RuntimeError(msg)

        return result
    except subprocess.TimeoutExpired:
        raise RuntimeError("Process timed out.\n💡 HINT: You likely have an infinite loop or a deadlocked pipe blocking `read()`.")

# --- STRICT TEST CASES ---

def test_compilation():
    log("1. Compilation, Strict Warnings, & Init Verification")
    DebugContext.reset()
    subprocess.run(["make", "clean"], capture_output=True)
    result = subprocess.run(["make"], capture_output=True)

    if b"warning" in result.stderr.lower():
        error("Compilation produced warnings. -Werror strict check failed.\n💡 HINT: Look closely at your compiler output and fix all warnings.")
        return False
    if not os.path.exists(MGIT_EXEC):
        error(f"Executable '{MGIT_EXEC}' not found.\n💡 HINT: Does your Makefile produce a binary named 'mgit'?")
        return False

    # Verify init behavior
    setup_env()
    subprocess.run(["../mgit", "init"], cwd=TEST_DIR)
    if not os.path.isdir(f"{TEST_DIR}/.mgit") or not os.path.isfile(f"{TEST_DIR}/.mgit/data.bin"):
        error("mgit init did not create the .mgit/ directory or data.bin vault.")
        return False

    # Ensure calling init twice doesn't crash or truncate existing data
    res_double_init = subprocess.run(["../mgit", "init"], cwd=TEST_DIR, capture_output=True)
    if res_double_init.returncode != 0:
         error("Calling mgit init twice caused a crash/error. It should safely do nothing if .mgit exists.")
         return False

    success("Compiled clean and init verified.")
    return True

def test_deep_nesting_metadata():
    log("2. Structural Integrity (Deep Nesting & BFS Crawler)")
    setup_env()

    expected_paths = []
    current_path = os.path.join(TEST_DIR, "lvl1")

    for i in range(1, 13):
        os.makedirs(current_path, exist_ok=True)
        file_path = os.path.join(current_path, f"data{i}.txt")
        with open(file_path, "w") as f: f.write(f"Payload {i}")
        expected_paths.append(f"data{i}.txt".encode())
        if i < 12: current_path = os.path.join(current_path, f"lvl{i+1}")

    hidden_dir = os.path.join(current_path, ".secret_folder")
    os.makedirs(hidden_dir)
    with open(os.path.join(hidden_dir, ".hidden"), "w") as f: f.write("Hidden")
    expected_paths.extend([b".secret_folder", b".hidden"])

    try:
        run_command(["../mgit", "init"], cwd=TEST_DIR)
        run_command(["../mgit", "snapshot", "deep"], cwd=TEST_DIR)

        manifest = f"{TEST_DIR}/.mgit/snapshots/snap_001.bin"
        if not os.path.exists(manifest):
            raise RuntimeError("Manifest not saved.\n💡 HINT: Check if `mgit snapshot` is writing to `.mgit/snapshots/snap_001.bin`.")

        with open(manifest, "rb") as f: data = f.read()
        for p in expected_paths:
            if p not in data:
                raise RuntimeError(f"Crawler failed to index path: {p.decode()}\n💡 HINT: Is your BFS queue too small? Are you skipping hidden folders?")

        shutil.rmtree(os.path.join(TEST_DIR, "lvl1"))
        run_command(["../mgit", "restore", "1"], cwd=TEST_DIR)

        if not os.path.exists(os.path.join(hidden_dir, ".hidden")):
            raise RuntimeError("Deep hidden file was not restored.\n💡 HINT: Check how `restore` recreates parent directories for deeply nested files.")

        success("BFS deep nesting and metadata serialization verified.")
        return True
    except Exception as e:
        error(str(e))
        DebugContext.print_trail(TEST_DIR)
        return False

def test_mgit_show():
    log("3. Command: mgit show (Live and Saved States)")
    setup_env()
    try:
        run_command(["../mgit", "init"], cwd=TEST_DIR)
        with open(f"{TEST_DIR}/hello.txt", "w") as f: f.write("World")

        # Test Live View
        res_live = run_command(["../mgit", "show"], cwd=TEST_DIR, enforce_silence=False)
        if b"hello.txt" not in res_live.stdout or b"LIVE VIEW" not in res_live.stdout:
            raise RuntimeError("mgit show (live) failed to print correct metadata.")

        # Test Saved View
        run_command(["../mgit", "snapshot", "Initial commit"], cwd=TEST_DIR)
        res_saved = run_command(["../mgit", "show", "1"], cwd=TEST_DIR, enforce_silence=False)
        if b"SNAPSHOT 1" not in res_saved.stdout or b"Initial commit" not in res_saved.stdout:
            raise RuntimeError("mgit show <id> failed to print snapshot message and ID.")

        success("mgit show correctly outputs both live and saved metadata.")
        return True
    except Exception as e:
        error(str(e))
        DebugContext.print_trail(TEST_DIR)
        return False


def test_storage_efficiency():
    log("4. Storage Efficiency (CoW & Garbage Collection)")
    setup_env()
    try:
        payload_a = b"X" * (1024 * 1024)
        with open(f"{TEST_DIR}/A.bin", "wb") as f: f.write(payload_a)
        os.link(f"{TEST_DIR}/A.bin", f"{TEST_DIR}/B.bin")

        run_command(["../mgit", "init"], cwd=TEST_DIR)

        unique_payload = b"UNIQUE_CHUNK_DATA_FOR_RECYCLING_TEST"
        with open(f"{TEST_DIR}/unique.bin", "wb") as f: f.write(unique_payload)

        run_command(["../mgit", "snapshot", "snap 1 with unique data"], cwd=TEST_DIR)

        vault_path = f"{TEST_DIR}/.mgit/data.bin"
        vault_size = os.path.getsize(vault_path)
        if vault_size > 1500000:
            raise RuntimeError(f"Deduplication failed. Vault size: {vault_size}")

        os.remove(f"{TEST_DIR}/unique.bin")
        log("   -> Filling history to trigger vacuum...")
        for i in range(2, 7):
            with open(f"{TEST_DIR}/noise_{i}.txt", "w") as f: f.write(f"Snapshot {i}")
            run_command(["../mgit", "snapshot", f"snap {i}"], cwd=TEST_DIR)

        if os.path.exists(f"{TEST_DIR}/.mgit/snapshots/snap_001.bin"):
            raise RuntimeError("Trimming failed: snap_001.bin still exists after 6th snapshot.")

        with open(vault_path, "rb") as f:
            vault_content = f.read()
            if unique_payload in vault_content:
                 raise RuntimeError(
                    "Chunk Recycling failed! Found data from Snapshot 1 still in the vault.\n"
                    "💡 HINT: In mgit_snapshot, are you calling chunks_recycle() before remove()?"
                )

        success("CoW deduplication and Chunk Recycling verified.")
        return True
    except Exception as e:
        error(str(e))
        DebugContext.print_trail(TEST_DIR)
        return False

def test_offset_relocation():
    log("5. Advanced Streaming (Offset Relocation & Forward-Only)")
    setup_env("sender_env")
    setup_env("receiver_env")

    try:
        run_command(["../mgit", "init"], cwd="sender_env")
        with open("sender_env/A.txt", "w") as f: f.write("SENDER_A")
        run_command(["../mgit", "snapshot", "snap1"], cwd="sender_env")
        with open("sender_env/B.txt", "w") as f: f.write("SENDER_B")
        run_command(["../mgit", "snapshot", "snap2"], cwd="sender_env")

        run_command(["../mgit", "init"], cwd="receiver_env")
        with open("receiver_env/R.txt", "w") as f: f.write("RECEIVER_NOISE_DATA_PADDING")
        run_command(["../mgit", "snapshot", "local_snap"], cwd="receiver_env")

        cmd = "../mgit send 2 | ../mgit receive ../receiver_env"
        run_command(cmd, cwd="sender_env", shell=True, enforce_silence=False)
        run_command(["../mgit", "restore", "2"], cwd="receiver_env")

        with open("receiver_env/B.txt", "r") as f:
            if f.read() != "SENDER_B":
                raise RuntimeError("Offset relocation failed! Restored wrong data.\n💡 HINT: In `mgit receive`, you MUST update the `physical_offset` in the manifest to match where you appended the file in the receiver's data.bin.")

        success("Physical offsets correctly relocated on receiver side.")
        return True
    except Exception as e:
        error(str(e))
        DebugContext.print_trail("receiver_env")
        return False

def test_time_travel_and_corruption():
    log("7. Time Travel & Corruption Detection")
    setup_env()
    try:
        with open(f"{TEST_DIR}/file.txt", "w") as f: f.write("V1")
        run_command(["../mgit", "init"], cwd=TEST_DIR)
        run_command(["../mgit", "snapshot", "s1"], cwd=TEST_DIR)

        # --- THE FIX: Change the size to break the mtime/size collision ---
        with open(f"{TEST_DIR}/file.txt", "w") as f: f.write("VERSION_2_MODIFIED")
        with open(f"{TEST_DIR}/added.txt", "w") as f: f.write("NEW")
        run_command(["../mgit", "snapshot", "s2"], cwd=TEST_DIR)

        run_command(["../mgit", "restore", "1"], cwd=TEST_DIR)

        with open(f"{TEST_DIR}/file.txt", "r") as f:
            if f.read() != "V1": raise RuntimeError("Failed to overwrite modified file.\n💡 HINT: `mgit restore` must completely overwrite existing files with the snapshot data.")
        if os.path.exists(f"{TEST_DIR}/added.txt"):
            raise RuntimeError("Sanitization failed: added.txt was not deleted.\n💡 HINT: `mgit restore` must delete files that exist in the workspace but NOT in the snapshot.")

        with open(f"{TEST_DIR}/.mgit/data.bin", "r+b") as f:
            f.seek(5)
            f.write(b'\xFF\xFF\xFF')

        res = run_command(["../mgit", "restore", "2"], cwd=TEST_DIR, expect_fail=True)
        if b"error" not in res.stderr.lower() and b"corrupt" not in res.stderr.lower():
            raise RuntimeError("Failed to print diagnostic error on corruption.\n💡 HINT: When restoring, calculate the hash of the chunk and compare it to the manifest. If it doesn't match, abort and print to stderr.")

        success("Perfect tree sanitization and graceful corruption handling passed.")
        return True
    except Exception as e:
        error(str(e))
        DebugContext.print_trail(TEST_DIR)
        return False


def main():
    clean_env()
    print(f"\n{Colors.HEADER}=== PA2: GRADING & DEBUG SUITE ==={Colors.ENDC}")
    tests = [
        test_compilation,
        test_deep_nesting_metadata,
        test_mgit_show,
        test_storage_efficiency,
        test_offset_relocation,
        test_time_travel_and_corruption,
    ]
    passed = 0
    for test in tests:
        print("-" * 65)
        if test(): passed += 1
        else:
            print(f"\n{Colors.FAIL}>> CRITICAL FAILURE. SUITE HALTED TO PRESERVE DEBUG STATE. <<{Colors.ENDC}")
            break

    print("=" * 65)
    print(f"{Colors.HEADER}FINAL SCORE: {passed}/{len(tests)} Tests Passed{Colors.ENDC}")
    sys.exit(0 if passed == len(tests) else 1)

if __name__ == "__main__":
    main()
