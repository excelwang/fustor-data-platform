
import pytest
import yaml
from sensord.config.unified import SensordPipeConfig

def test_pipe_config_accepts_floats():
    config_yaml = """
    source: "local-fs"
    sender: "fustord-cloud"
    audit_interval_sec: 0.5
    sentinel_interval_sec: 1.2
    """
    config_dict = yaml.safe_load(config_yaml)
    config = SensordPipeConfig(**config_dict)
    
    assert isinstance(config.audit_interval_sec, float)
    assert config.audit_interval_sec == 0.5
    assert config.sentinel_interval_sec == 1.2
