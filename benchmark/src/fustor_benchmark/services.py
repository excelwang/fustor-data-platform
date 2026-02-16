import os
import shutil
import click
import sys
import yaml
import subprocess
import time
import requests


class ServiceManager:
    def __init__(self, run_dir: str, base_port: int = 18100):
        self.run_dir = os.path.abspath(run_dir)
        # 监控目标数据目录
        self.data_dir = os.path.join(self.run_dir, "data")
        # 系统环境主目录 (FUSTOR_HOME)
        self.env_dir = os.path.join(self.run_dir, ".fustor")
        
        self.fustord_port = base_port + 2 # Management API
        self.ingest_port = base_port + 3 # Data Receiver
        self.sensord_port = base_port
        
        self.fustord_process = None
        self.sensord_process = None
        self.processes = [] # Backwards compatibility for stop_all fallback

    def setup_env(self):
        # Safety Check: Only allow operations in directories ending with 'fustor-benchmark-run'
        if not self.run_dir.endswith("fustor-benchmark-run"):
            click.echo(click.style(f"FATAL: Environment setup denied. Target run-dir '{self.run_dir}' must end with 'fustor-benchmark-run' for safety.", fg="red", bold=True))
            sys.exit(1)

        if os.path.exists(self.env_dir):
            shutil.rmtree(self.env_dir)
        os.makedirs(self.env_dir, exist_ok=True)
        
        # Generate a random token for internal communication
        import secrets
        self.client_token = secrets.token_urlsafe(32)
        
        # Environment config
        with open(os.path.join(self.env_dir, ".env"), "w") as f:
            f.write(f"FUSTOR_HOME={self.env_dir}\n")
            f.write(f"FUSTOR_LOG_LEVEL=DEBUG\n")
        
        # V2: Unified Config for fustord
        os.makedirs(os.path.join(self.env_dir, "fustord-config"), exist_ok=True)
        self.api_key = "bench-api-key-123456"
        fustord_config = {
            "receivers": {
                "bench-http": {
                    "driver": "http",
                    "port": self.ingest_port,
                    "host": "0.0.0.0",
                    "api_keys": [
                        {"key": self.api_key, "pipe_id": "bench-pipe"}
                    ]
                }
            },
            "views": {
                "bench-view": {
                    "driver": "fs",
                    "driver_params": {"hot_file_threshold": 30.0}
                }
            },
            "pipes": {
                "bench-pipe": {
                    "receiver": "bench-http",
                    "views": ["bench-view"]
                }
            }
        }
        with open(os.path.join(self.env_dir, "fustord-config/default.yaml"), "w") as f:
            yaml.dump(fustord_config, f)

    def _wait_for_service(self, url: str, name: str, timeout: int = 30):
        click.echo(f"Waiting for {name} at {url}...")
        start = time.time()
        while time.time() - start < timeout:
            try:
                requests.get(url, timeout=1)
                click.echo(f"{name} is up.")
                return True
            except Exception:
                time.sleep(0.5)
        click.echo(f"Error: {name} failed to start.")
        return False


    def configure_system(self):
        click.echo("System configured via static YAML.")
        return self.api_key

    def start_fustord(self):
        cmd = [
            "fustord", "start",
            "-p", str(self.fustord_port)
        ]
        log_file = open(os.path.join(self.env_dir, "fustord.log"), "a")
        env = os.environ.copy()
        env["FUSTOR_HOME"] = self.env_dir
        
        p = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        log_file.close()
        self.fustord_process = p
        self.processes.append(p)
        
        click.echo(f"Waiting for fustord at http://localhost:{self.fustord_port}...")
        start = time.time()
        while time.time() - start < 30:
            try:
                requests.get(f"http://localhost:{self.fustord_port}/", timeout=1)
                click.echo("fustord is up.")
                return
            except requests.ConnectionError:
                time.sleep(0.5)
        raise RuntimeError("fustord start failed")

    def start_sensord(self, api_key: str, **kwargs):
        # Clean up stale PID file to allow restart
        sensord_pid = os.path.join(self.env_dir, "sensord.pid")
        if os.path.exists(sensord_pid):
            os.remove(sensord_pid)

        # V2: Unified Config for sensord
        os.makedirs(os.path.join(self.env_dir, "sensord-config"), exist_ok=True)
        sensord_config = {
            "sources": {
                "bench-fs": {
                    "driver": "fs",
                    "uri": self.data_dir,
                    "driver_params": {
                        "max_queue_size": 100000,
                        "min_monitoring_window_days": 1
                    }
                }
            },
            "senders": {
                "bench-fustord": {
                    "driver": "fustord",
                    "uri": f"http://127.0.0.1:{self.ingest_port}",
                    "credential": {"key": api_key}
                }
            },
            "pipes": {
                "bench-pipe": {
                    "source": "bench-fs",
                    "sender": "bench-fustord",
                    "audit_interval_sec": kwargs.get("audit_interval", 0),
                    "sentinel_interval_sec": kwargs.get("sentinel_interval", 0)
                }
            }
        }
        with open(os.path.join(self.env_dir, "sensord-config/default.yaml"), "w") as f:
            yaml.dump(sensord_config, f)
            
        cmd = [
            "sensord", "start"
        ]
        log_file = open(os.path.join(self.env_dir, "sensord.log"), "a")
        env = os.environ.copy()
        env["FUSTOR_HOME"] = self.env_dir
        
        p = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
        log_file.close() # Close in parent
        self.sensord_process = p
        self.processes.append(p)
        
        # sensord has no HTTP management API in V2, so we just wait a bit or check logs
        time.sleep(2)

    def check_sensord_logs(self, lines=100):
        log_path = os.path.join(self.env_dir, "sensord.log")
        if not os.path.exists(log_path):
            return False, "Log file not found yet"
        
        try:
            with open(log_path, "r") as f:
                content = f.readlines()[-lines:]
            
            error_keywords = ["ERROR", "Exception", "Traceback", "404 -", "failed to start", "ConfigurationError", "崩溃"]
            success_keywords = ["initiated successfully", "Uvicorn running", "Application startup complete"]
            
            has_error = False
            error_msg = ""
            has_success = False

            for line in content:
                if any(kw in line for kw in error_keywords):
                    has_error = True
                    error_msg = line.strip()
                if any(kw in line for kw in success_keywords):
                    has_success = True

            if has_error:
                return False, f"Detected Error: {error_msg}"
            
            if not has_success:
                return True, "Starting up... (no success signal yet)"
                
            return True, "OK (Success signals detected)"
        except Exception as e:
            return True, f"Could not read log: {e}"

    def get_sensord_log_path(self):
        return os.path.join(self.env_dir, "sensord.log")

    def get_fustord_log_path(self):
        return os.path.join(self.env_dir, "fustord.log")

    def get_log_size(self, log_path):
        if not os.path.exists(log_path): return 0
        return os.path.getsize(log_path)

    def grep_log(self, log_path, pattern, start_offset=0):
        """Search for a regex pattern in log file starting from offset."""
        if not os.path.exists(log_path): return None
        import re
        regex = re.compile(pattern)
        with open(log_path, "r") as f:
            f.seek(start_offset)
            for line in f:
                match = regex.search(line)
                if match:
                    return match
        return None
        
    def wait_for_log(self, log_path, pattern, start_offset=0, timeout=30):
        """Wait for a pattern to appear in log."""
        start = time.time()
        while time.time() - start < timeout:
            match = self.grep_log(log_path, pattern, start_offset)
            if match: return match
            time.sleep(0.5)
        return None

    def trigger_sensord_audit(self, pipe_id="bench-pipe"):
        """Triggers audit for a view via fustord API."""
        url = f"http://localhost:{self.fustord_port}/api/v1/pipe/consistency/audit/start"
        headers = {"X-API-Key": self.api_key}
        res = requests.post(url, headers=headers)
        res.raise_for_status()
        return res.json()

    def trigger_sensord_sentinel(self, pipe_id="bench-pipe"):
        """Sentinel check is passive in V2, but we can check tasks."""
        url = f"http://localhost:{self.fustord_port}/api/v1/pipe/consistency/sentinel/tasks"
        headers = {"X-API-Key": self.api_key}
        res = requests.get(url, headers=headers)
        res.raise_for_status()
        return res.json()

    def wait_for_leader(self, pipe_id="bench-pipe", timeout=30, start_offset=0):
        click.echo(f"Waiting for {pipe_id} to become LEADER...")
        pattern = rf"Assigned LEADER role for {pipe_id}"
        return self.wait_for_log(self.get_sensord_log_path(), pattern, start_offset=start_offset, timeout=timeout)

    def stop_sensord(self):
        """Safely stop only the benchmark sensord process."""
        if self.sensord_process:
            click.echo("Stopping benchmark sensord...")
            try:
                self.sensord_process.terminate()
                self.sensord_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.sensord_process.kill()
            
            # Remove from tracking list to prevent double-kill in stop_all
            if self.sensord_process in self.processes:
                self.processes.remove(self.sensord_process)
                
            self.sensord_process = None
        
        # Remove PID file
        sensord_pid = os.path.join(self.env_dir, "sensord.pid")
        if os.path.exists(sensord_pid):
            os.remove(sensord_pid)
        time.sleep(1)

    def stop_all(self):
        click.echo("Stopping all benchmark services...")
        # No more global pkill!
        
        for p in self.processes:
            try:
                p.terminate()
                p.wait(timeout=2)
            except Exception:
                p.kill()
        self.processes = []

