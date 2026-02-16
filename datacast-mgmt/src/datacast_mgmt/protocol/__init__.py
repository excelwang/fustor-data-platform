from enum import Enum
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field

class CommandType(str, Enum):
    SCAN = "scan"
    RELOAD_CONFIG = "reload_config"
    STOP_PIPE = "stop_pipe"
    UPDATE_CONFIG = "update_config"
    REPORT_CONFIG = "report_config"
    UPGRADE = "upgrade"

class CommandBase(BaseModel):
    type: CommandType
    job_id: Optional[str] = None

class ScanCommand(CommandBase):
    type: CommandType = CommandType.SCAN
    path: str = "/"
    recursive: bool = True
    max_depth: int = 10
    limit: int = 1000
    pattern: str = "*"

class StopPipeCommand(CommandBase):
    type: CommandType = CommandType.STOP_PIPE
    pipe_id: str

class UpdateConfigCommand(CommandBase):
    type: CommandType = CommandType.UPDATE_CONFIG
    config_yaml: str
    filename: str = "default.yaml"

class ReportConfigCommand(CommandBase):
    type: CommandType = CommandType.REPORT_CONFIG
    filename: str = "default.yaml"

class UpgradeCommand(CommandBase):
    type: CommandType = CommandType.UPGRADE
    version: str
