# sensord/src/sensord/config/unified.py
"""
Unified Configuration Loader for sensord.

Config directory: $FUSTOR_HOME/sensord-config/
All YAML files in the directory share the same namespace.
Components (sources, senders, pipes) can reference each other across files.

Example:
  sensord-config/default.yaml:
    sources:
      research-fs:
        driver: fs
        paths: [/data/research]
    senders:
      fustord-main:
        driver: fustord
        uri: http://fustord:8102
    pipes:
      research-sync:
        source: research-fs
        sender: fustord-main
"""
import yaml
import logging
import socket
import hashlib
import json
from pathlib import Path
from typing import Dict, Optional, List, Any, Set
from pydantic import BaseModel, field_validator, Field

from sensord_core.common import get_fustor_home_dir
from sensord_core.models.config import SourceConfig, SenderConfig, GlobalLoggingConfig, FieldMapping

logger = logging.getLogger(__name__)


def get_outbound_ip(target_host: str = "8.8.8.8", target_port: int = 80) -> str:
    """
    Detect the local IP address used to reach a target host.
    Does not actually establish a connection.
    """
    try:
        # We use UDP to avoid actual handshake, just gets the routing decision
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Handle cases where target_host might be a URL
        if "://" in target_host:
            from urllib.parse import urlparse
            target_host = urlparse(target_host).hostname or target_host
        
        s.connect((target_host, target_port))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        # Fallback to hostname if IP detection fails
        return socket.gethostname()


class SensordPipeConfig(BaseModel):
    """Configuration for a single sensord pipe."""
    source: str  # Reference to source ID
    sender: str  # Reference to sender ID
    
    # Sync intervals
    audit_interval_sec: float = 43200.0  # 12 hours
    sentinel_interval_sec: float = 300.0   # 5 minutes
    session_timeout_seconds: Optional[float] = None  # Session timeout hint for the server
    
    # Reliability configuration
    error_retry_interval: float = 5.0
    max_consecutive_errors: int = 5
    backoff_multiplier: float = 2.0
    max_backoff_seconds: float = 60.0
    
    disabled: bool = False
    
    # Field mapping
    fields_mapping: List[FieldMapping] = []


class UnifiedsensordConfig(BaseModel):
    """Unified configuration containing all components."""
    logging: GlobalLoggingConfig = Field(default_factory=GlobalLoggingConfig)

    sources: Dict[str, Dict[str, Any]] = {}
    senders: Dict[str, Dict[str, Any]] = {}
    pipes: Dict[str, SensordPipeConfig] = {}
    
    # Tuning parameters
    fs_scan_workers: int = Field(default=4, description="Default concurrency for FS scans")


class sensordConfigLoader:
    """
    Loads and merges all config files from sensord-config/ directory.
    
    All files share the same namespace, allowing cross-file references.
    """
    
    def __init__(self, config_dir: Optional[Path] = None):
        if config_dir is None:
            home = get_fustor_home_dir()
            config_dir = home / "sensord-config"
        
        self.dir = Path(config_dir)
        
        # Global settings
        self.logging = GlobalLoggingConfig()
        self.fs_scan_workers: int = 4
        self.sensord_id: Optional[str] = None

        # Merged namespace
        self._sources: Dict[str, SourceConfig] = {}
        self._senders: Dict[str, SenderConfig] = {}
        self._pipes: Dict[str, SensordPipeConfig] = {}
        
        # Track which file defines which pipes (for selective startup)
        self._pipes_by_file: Dict[str, Set[str]] = {}
        
        self._loaded = False
    
    def load_all(self) -> None:
        """Load and merge all YAML files from config directory."""
        self.logging = GlobalLoggingConfig()
        self.fs_scan_workers: int = 4
        self.sensord_id = None

        self._sources.clear()
        self._senders.clear()
        self._pipes.clear()
        self._pipes_by_file.clear()
        
        if not self.dir.exists():
            logger.info(f"Config directory not found: {self.dir}. Creating it.")
            self.dir.mkdir(parents=True, exist_ok=True)
            self._loaded = True
            return
        
        # Load all YAML files
        for yaml_file in sorted(self.dir.glob("*.yaml")):
            self._load_file(yaml_file)
        
        self._loaded = True
        logger.info(
            f"Loaded config: {len(self._sources)} sources, "
            f"{len(self._senders)} senders, {len(self._pipes)} pipes"
        )
    
    def _load_file(self, path: Path) -> None:
        """Load a single config file and merge into namespace."""
        try:
            with open(path) as f:
                data = yaml.safe_load(f) or {}
            
            file_key = path.name
            
            # 1. Global settings
            if "logging" in data:
                 # GlobalLoggingConfig validator handles string vs dict
                self.logging = GlobalLoggingConfig.model_validate(data["logging"])
            
            if "fs_scan_workers" in data:
                self.fs_scan_workers = int(data["fs_scan_workers"])
            
            if "sensord_id" in data:
                aid = str(data["sensord_id"]).strip()
                if self.sensord_id and self.sensord_id != aid:
                    logger.warning(f"sensord ID redefined in {path}: was '{self.sensord_id}', now '{aid}'")
                self.sensord_id = aid
            
            # Default to hostname if not set after loading all (logic moved to load_all end, 
            # but to ensure validator sees it, we can set it if still None? 
            # No, load_all calls _load_file multiple times. 
            # Better to set default in load_all AFTER loop.)
            


            # Merge sources
            for src_id, src_data in data.get("sources", {}).items():
                if src_id in self._sources:
                    logger.warning(f"Source '{src_id}' redefined in {path}")
                print(f"DEBUG: Loading source {src_id} from {path}")
                self._sources[src_id] = SourceConfig(**src_data)
            
            # Merge senders
            for sender_id, sender_data in data.get("senders", {}).items():
                if sender_id in self._senders:
                    logger.warning(f"Sender '{sender_id}' redefined in {path}")
                self._senders[sender_id] = SenderConfig(**sender_data)
            
            # Merge pipes
            pipe_ids = set()
            for pipe_id, pipe_data in data.get("pipes", {}).items():
                if pipe_id in self._pipes:
                    logger.warning(f"Pipe '{pipe_id}' redefined in {path}")
                self._pipes[pipe_id] = SensordPipeConfig(**pipe_data)
                pipe_ids.add(pipe_id)
            
            self._pipes_by_file[file_key] = pipe_ids
            
            # Load global settings if this is default.yaml
            if file_key == "default.yaml":
                if "logging" in data:
                    self.logging = GlobalLoggingConfig.model_validate(data["logging"])

            
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {path}")
        except yaml.YAMLError as e:
            logger.error(f"YAML syntax error in configuration file {path}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error loading configuration from {path}: {e}", exc_info=True)
    
    def ensure_loaded(self) -> None:
        if not self._loaded:
            self.load_all()
    
    def get_source(self, source_id: str) -> Optional[SourceConfig]:
        self.ensure_loaded()
        return self._sources.get(source_id)
    
    def get_sender(self, sender_id: str) -> Optional[SenderConfig]:
        self.ensure_loaded()
        return self._senders.get(sender_id)
    
    def get_pipe(self, pipe_id: str) -> Optional[SensordPipeConfig]:
        self.ensure_loaded()
        return self._pipes.get(pipe_id)
    
    def get_all_sources(self) -> Dict[str, SourceConfig]:
        self.ensure_loaded()
        return self._sources.copy()
    
    def get_all_senders(self) -> Dict[str, SenderConfig]:
        self.ensure_loaded()
        return self._senders.copy()
    
    def get_all_pipes(self) -> Dict[str, SensordPipeConfig]:
        self.ensure_loaded()
        return self._pipes.copy()
    
    def get_pipes_from_file(self, filename: str) -> Dict[str, SensordPipeConfig]:
        """Get pipes defined in a specific file."""
        self.ensure_loaded()
        pipe_ids = self._pipes_by_file.get(filename, set())
        return {pid: self._pipes[pid] for pid in pipe_ids if pid in self._pipes}
    
    def get_default_pipes(self) -> Dict[str, SensordPipeConfig]:
        """Get pipes from default.yaml."""
        return self.get_pipes_from_file("default.yaml")
    
    def get_enabled_pipes(self) -> Dict[str, SensordPipeConfig]:
        """
        Get all enabled pipes.
        In sensord, a pipe is enabled if its source is enabled.
        """
        self.ensure_loaded()
        enabled = {}
        for pid, pcfg in self._pipes.items():
            if pcfg.disabled:
                continue
                
            source = self.get_source(pcfg.source)
            if not source or source.disabled:
                continue
                
            sender = self.get_sender(pcfg.sender)
            if not sender or sender.disabled:
                continue
                
            enabled[pid] = pcfg
        return enabled
    
    def resolve_pipe_refs(self, pipe_id: str) -> Optional[Dict[str, Any]]:
        """Resolve a pipe's source and sender references to actual configs."""
        pipe = self.get_pipe(pipe_id)
        if not pipe:
            return None
        
        source = self.get_source(pipe.source)
        sender = self.get_sender(pipe.sender)
        
        if not source:
            logger.error(f"Pipe '{pipe_id}' references unknown source '{pipe.source}'")
            return None
        if not sender:
            logger.error(f"Pipe '{pipe_id}' references unknown sender '{pipe.sender}'")
            return None
        
        return {
            "pipe": pipe,
            "source": source,
            "sender": sender,
        }
    
    def get_diff(self, old_pipe_ids: Set[str]) -> Dict[str, Set[str]]:
        """
        Compare current enabled pipes with a set of old pipe IDs.
        Returns:
            dict: {
                "added": set of new pipe IDs that should be started,
                "removed": set of old pipe IDs that should be stopped
            }
        """
        current_enabled = set(self.get_enabled_pipes().keys())
        added = current_enabled - old_pipe_ids
        removed = old_pipe_ids - current_enabled
        return {"added": added, "removed": removed}

    def get_config_signature(self) -> str:
        """
        Calculate a SHA256 signature of the current configuration state.
        This allows for efficient change detection (hot reload).
        """
        self.ensure_loaded()
        
        # Collect all config components into a stable structure
        state = {
            "logging": self.logging.model_dump(),
            "fs_scan_workers": self.fs_scan_workers,
            "sensord_id": self.sensord_id,
            "sources": {k: v.model_dump() for k, v in sorted(self._sources.items())},
            "senders": {k: v.model_dump() for k, v in sorted(self._senders.items())},
            "pipes": {k: v.model_dump() for k, v in sorted(self._pipes.items())},
        }
        
        # Serialize to JSON with sorted keys for stability
        serialized = json.dumps(state, sort_keys=True)
        return hashlib.sha256(serialized.encode('utf-8')).hexdigest()

    def reload(self) -> None:
        """Force reload all configurations."""
        self._loaded = False
        self.load_all()

    def add_source(self, id: str, config: SourceConfig) -> None:
        self.ensure_loaded()
        self._sources[id] = config

    def delete_source(self, id: str) -> None:
        self.ensure_loaded()
        if id in self._sources:
            del self._sources[id]

    def update_source(self, id: str, updates: Dict[str, Any]) -> None:
        self.ensure_loaded()
        if id in self._sources:
            conf = self._sources[id]
            for key, value in updates.items():
                setattr(conf, key, value)
    
    def add_sender(self, id: str, config: SenderConfig) -> None:
        self.ensure_loaded()
        self._senders[id] = config

    def delete_sender(self, id: str) -> None:
        self.ensure_loaded()
        if id in self._senders:
            del self._senders[id]

    def update_sender(self, id: str, updates: Dict[str, Any]) -> None:
        self.ensure_loaded()
        if id in self._senders:
            conf = self._senders[id]
            for key, value in updates.items():
                setattr(conf, key, value)


# Global instance
sensord_config = sensordConfigLoader()
