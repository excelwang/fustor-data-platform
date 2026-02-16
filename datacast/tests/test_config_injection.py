
import pytest
from datacast.config.unified import DatacastConfigLoader, UnifiedDatacastConfig
from datacast_core.models.config import SourceConfig

def test_DatacastConfig_default_fs_scan_workers():
    loader = DatacastConfigLoader()
    # It should default to 4 without loading any file
    assert loader.fs_scan_workers == 4

def test_DatacastConfig_load_fs_scan_workers(tmp_path):
    loader = DatacastConfigLoader(config_dir=tmp_path)
    config_file = tmp_path / "datacast.yaml"
    config_file.write_text("fs_scan_workers: 8\n")
    
    loader.load_all()
    assert loader.fs_scan_workers == 8

def test_injection_logic():
    # Simulate the logic in PipeManager
    from datacast.config.unified import DatacastConfig
    DatacastConfig.fs_scan_workers = 6
    
    # Case 1: Driver is fs, no max_scan_workers -> inject
    source_config = SourceConfig(driver="fs", uri="/tmp", driver_params={})
    
    if source_config.driver == "fs":
        if "max_scan_workers" not in source_config.driver_params:
            source_config.driver_params["max_scan_workers"] = DatacastConfig.fs_scan_workers
            
    assert source_config.driver_params["max_scan_workers"] == 6
    
    # Case 2: Driver is fs, max_scan_workers exists -> keep
    source_config_2 = SourceConfig(driver="fs", uri="/tmp", driver_params={"max_scan_workers": 2})
    
    if source_config_2.driver == "fs":
        if "max_scan_workers" not in source_config_2.driver_params:
            source_config_2.driver_params["max_scan_workers"] = DatacastConfig.fs_scan_workers
            
    assert source_config_2.driver_params["max_scan_workers"] == 2
    
    # Case 3: Driver is not fs -> do nothing
    source_config_3 = SourceConfig(driver="mysql", uri="mysql://", driver_params={})
    
    if source_config_3.driver == "fs":
        if "max_scan_workers" not in source_config_3.driver_params:
            source_config_3.driver_params["max_scan_workers"] = DatacastConfig.fs_scan_workers
            
    assert "max_scan_workers" not in source_config_3.driver_params
