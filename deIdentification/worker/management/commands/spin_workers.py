import os
import time
import signal
import json
import platform
import subprocess
import shutil
from pathlib import Path

import psutil
from django.core.management.base import BaseCommand
from deIdentification.nd_logger import nd_logger

# ──────────────────────────────────────────────
# ⚙️ Configuration
# ──────────────────────────────────────────────
_CPU_COUNT = os.cpu_count() or 8
MAX_WORKERS = _CPU_COUNT
CPU_THRESHOLD = 70.0
RAM_THRESHOLD = 70.0
CHECK_INTERVAL = 10       # seconds between health checks
SPAWN_DELAY = 10          # seconds between new workers
IDLE_GRACE_PERIOD = 60    # seconds with no tasks before killing workers
RECENT_ACTIVITY_WINDOW = 30  # seconds

WORKER_STATE_FILE = Path(os.environ.get("TEMP", "/tmp")) / "spin_workers.json"
WORKER_HEARTBEAT_DIR = Path(os.environ.get("TEMP", "/tmp")) / "worker_heartbeats"
WORKER_HEARTBEAT_DIR.mkdir(exist_ok=True)

worker_processes = []  # [{pid, system, cmd, started_at}]
last_task_seen_at = time.time()


# ──────────────────────────────────────────────
# 📁 Project & System Utilities
# ──────────────────────────────────────────────
def get_project_dir():
    try:
        base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        manage_py = os.path.join(base, "manage.py")
        if os.path.exists(manage_py):
            return os.path.dirname(manage_py)
    except Exception:
        pass
    return os.getcwd()


def has_ready_tasks():
    try:
        nd_logger.debug("Polling for ready tasks")
        from worker.models import Task
        from worker.models.helper import ComputationStatus
        exists = Task.objects.filter(status=ComputationStatus.NOT_STARTED).exists()
        if not exists:
            nd_logger.info("Found no task to pick up")
        return exists
    except Exception as e:
        nd_logger.error(f"[Manager] Error checking ready tasks: {e}", exc_info=True)
        return False


def get_system_usage(samples=3):
    try:
        cpu_samples = [psutil.cpu_percent(interval=1) for _ in range(samples)]
        cpu_usage = sum(cpu_samples) / len(cpu_samples)
        ram_usage = psutil.virtual_memory().percent
        return cpu_usage, ram_usage
    except Exception as e:
        nd_logger.error(f"[Manager] Failed to get system usage: {e}")
        return 0.0, 0.0


# ──────────────────────────────────────────────
# 🧱 Persistence Helpers
# ──────────────────────────────────────────────
def _load_worker_state():
    global worker_processes
    if WORKER_STATE_FILE.exists():
        try:
            with open(WORKER_STATE_FILE, "r") as f:
                worker_processes = json.load(f)
                nd_logger.info(f"[Manager] Loaded {len(worker_processes)} persisted workers")
        except Exception:
            worker_processes = []


def _save_worker_state():
    with open(WORKER_STATE_FILE, "w") as f:
        json.dump(worker_processes, f, indent=2)

def mark_worker_active(pid):
    heartbeat_file = WORKER_HEARTBEAT_DIR / f"{pid}.json"
    heartbeat_file.write_text(json.dumps({
        "last_active_at": time.time()
    }))

def _clear_worker_state():
    if WORKER_STATE_FILE.exists():
        WORKER_STATE_FILE.unlink()


# ──────────────────────────────────────────────
# 🚀 Start Worker
# ──────────────────────────────────────────────
def start_worker(project_dir, conda_env=None):
    global worker_processes
    activate_cmd = f"conda activate {conda_env}" if conda_env else ""
    cmd = f"{activate_cmd} && cd '{project_dir}' && python manage.py start_worker"
    system = platform.system().lower()
    pid = None

    if "darwin" in system:
        applescript = f'''
        tell application "Terminal"
            do script "{cmd}; exit"
        end tell
        '''
        p = subprocess.Popen(["osascript", "-e", applescript])
        pid = p.pid

    elif "linux" in system:
        full_cmd = f"{cmd}; exit"
        if shutil.which("gnome-terminal"):
            p = subprocess.Popen(["gnome-terminal", "--", "bash", "-c", full_cmd])
            pid = p.pid
        else:
            p = subprocess.Popen(["bash", "-c", full_cmd], preexec_fn=os.setpgrp)
            pid = p.pid

    elif "windows" in system:
        p = subprocess.Popen(
            ["start", "cmd", "/c", f"conda activate {conda_env} && cd /d {project_dir} && python manage.py start_worker"],
            shell=True,
        )
        pid = p.pid

    if pid:
        worker_processes.append(
            {"pid": pid, "system": system, "cmd": cmd, "started_at": time.time()}
        )
        _save_worker_state()


# ──────────────────────────────────────────────
# 🧹 Cleanup
# ──────────────────────────────────────────────
def cleanup_workers():
    global worker_processes
    _load_worker_state()

    nd_logger.info(f"[Manager] Cleaning {len(worker_processes)} idle worker(s)...")
    for proc in worker_processes[:]:
        try:
            os.kill(proc["pid"], signal.SIGTERM)
            time.sleep(0.5)
            os.kill(proc["pid"], signal.SIGKILL)
        except Exception:
            pass
        finally:
            worker_processes.remove(proc)
            _save_worker_state()

    _clear_worker_state()


# ──────────────────────────────────────────────
# 🧠 Main Command
# ──────────────────────────────────────────────
class Command(BaseCommand):
    help = "Dynamic worker manager with idle timeout + CPU/RAM throttling"

    def add_arguments(self, parser):
        parser.add_argument("--conda_env", type=str)

    def handle(self, *args, **options):
        global worker_processes, last_task_seen_at

        conda_env = options.get("conda_env")
        project_dir = get_project_dir()
        _load_worker_state()

        def signal_handler(sig, frame):
            cleanup_workers()
            exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        while True:
            now = time.time()

            # ──────────────────────────────────────────
            # 1️⃣ Remove dead processes (OS-level)
            # ──────────────────────────────────────────
            worker_processes[:] = [
                w for w in worker_processes
                if psutil.pid_exists(w["pid"])
            ]
            _save_worker_state()

            cpu, ram = get_system_usage()
            nd_logger.info(
                f"[Manager] CPU={cpu:.1f}% | RAM={ram:.1f}% | Active={len(worker_processes)}"
            )

            # ──────────────────────────────────────────
            # 2️⃣ Kill PER-WORKER idle workers (even if tasks exist)
            # ──────────────────────────────────────────
            for worker in worker_processes[:]:
                pid = worker["pid"]
                heartbeat_file = WORKER_HEARTBEAT_DIR / f"{pid}.json"

                # Worker NEVER picked a task
                if not heartbeat_file.exists():
                    nd_logger.info(
                        f"[Manager] Worker PID={pid} never picked a task. Killing idle worker."
                    )
                    try:
                        os.kill(pid, signal.SIGTERM)
                        time.sleep(0.5)
                        os.kill(pid, signal.SIGKILL)
                    except Exception:
                        pass

                    worker_processes.remove(worker)
                    _save_worker_state()
                    continue

                # Worker picked tasks before, but not recently
                try:
                    data = json.loads(heartbeat_file.read_text())
                    last_active = data.get("last_active_at", 0)
                except Exception:
                    last_active = 0

                if now - last_active > CHECK_INTERVAL:
                    nd_logger.info(
                        f"[Manager] Worker PID={pid} not picking tasks anymore. Killing idle worker."
                    )
                    try:
                        os.kill(pid, signal.SIGTERM)
                        time.sleep(0.5)
                        os.kill(pid, signal.SIGKILL)
                    except Exception:
                        pass

                    worker_processes.remove(worker)
                    _save_worker_state()
                    try:
                        heartbeat_file.unlink()
                    except Exception:
                        pass

            # ──────────────────────────────────────────
            # 3️⃣ Check if tasks exist (GLOBAL signal)
            # ──────────────────────────────────────────
            tasks_present = has_ready_tasks()

            if tasks_present:
                last_task_seen_at = now

                # 🔑 STRICT RULE:
                # Spawn ONLY if there are ZERO workers
                if not worker_processes:
                    cpu, ram = get_system_usage()
                    if cpu < CPU_THRESHOLD and ram < RAM_THRESHOLD:
                        nd_logger.info(
                            "[Manager] Tasks exist and no active workers. Spawning ONE worker."
                        )
                        start_worker(project_dir, conda_env)
                        time.sleep(SPAWN_DELAY)
                else:
                    nd_logger.info(
                        "[Manager] Tasks exist but existing workers are idle. "
                        "Waiting for idle workers to exit before spawning."
                    )
            else:
                # ──────────────────────────────────────
                # 4️⃣ GLOBAL idle shutdown fallback
                # ──────────────────────────────────────
                idle_time = now - last_task_seen_at
                if worker_processes and idle_time >= IDLE_GRACE_PERIOD:
                    nd_logger.info(
                        f"[Manager] No tasks for {int(idle_time)}s ≥ {IDLE_GRACE_PERIOD}s. "
                        "Shutting down all remaining workers."
                    )
                    cleanup_workers()
                else:
                    nd_logger.debug(
                        f"[Manager] No tasks. Global idle for {int(idle_time)}s (waiting)."
                    )

            time.sleep(CHECK_INTERVAL)
