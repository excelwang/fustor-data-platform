from pydantic import BaseModel, Field
from typing import Optional

class fustordGlobalConfig(BaseModel):
    """
    Global configuration for the fustord service.
    Relocated from datacastst_core.
    """
    host: str = Field(default="0.0.0.0", description="管理 API 监听地址")
    port: int = Field(default=8101, description="管理 API 监听端口")
    session_cleanup_interval: float = Field(default=5.0, description="会话清理间隔(秒)")
    management_api_key: Optional[str] = Field(default=None, description="管理 API 密钥，为空则不鉴权")
    
    # GAP-P0 Implementation settings
    audit_timeout_multiplier: float = Field(default=2.0, description="审计超时倍数 (x audit_interval)")
    on_command_fallback_timeout: float = Field(default=10.0, description="On-Command Find 回退超时(秒)")
    on_command_concurrency_limit: int = Field(default=50, description="On-Command Find 并发限制")
