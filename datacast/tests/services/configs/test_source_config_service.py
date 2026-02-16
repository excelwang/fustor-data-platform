import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from datacast.domain.configs.source import SourceConfigService
from datacast_core.models.config import AppConfig, SourceConfig, PipeConfig, PasswdCredential
from datacast_core.exceptions import NotFoundError

@pytest.fixture
def mock_app_config():
    app_config = MagicMock(spec=AppConfig)
    app_config.get_sources.return_value = {}
    app_config.get_pipes.return_value = {}
    return app_config

@pytest.fixture
def mock_pipe_instance_service():
    mock = MagicMock()
    mock.mark_dependent_pipes_outdated = AsyncMock()
    return mock

@pytest.fixture
def source_config_service(mock_app_config, mock_pipe_instance_service):
    service = SourceConfigService(mock_app_config)
    service.set_dependencies(mock_pipe_instance_service)
    return service

@pytest.fixture
def sample_source_config():
    return SourceConfig(driver="mysql", uri="mysql://host", credential=PasswdCredential(user="u"), disabled=False)

@pytest.fixture
def sample_pipe_config(sample_source_config):
    return PipeConfig(source="source1", sender="sender1", disabled=False)

class TestSourceConfigService:
    def test_set_dependencies(self, source_config_service):
        mock_pipe_service = MagicMock()
        source_config_service.set_dependencies(mock_pipe_service)
        assert source_config_service.pipe_instance_service == mock_pipe_service

    @pytest.mark.asyncio
    @patch('datacast.domain.configs.source.config_lock')
    async def test_add_config(self, mock_config_lock, source_config_service, mock_app_config, sample_source_config):
        mock_app_config.get_sources.return_value = {}
        source_config_service._add_config_to_app = MagicMock()

        mock_config_lock.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_config_lock.return_value.__aexit__ = AsyncMock(return_value=None)

        result = await source_config_service.add_config("test_source", sample_source_config)

        source_config_service._add_config_to_app.assert_called_once_with("test_source", sample_source_config)
        assert result == sample_source_config

    @pytest.mark.asyncio
    @patch('datacast.domain.configs.source.config_lock')
    @patch('datacast.domain.schema_cache.is_schema_valid', return_value=True)
    async def test_update_config_enable_with_schema(self, mock_schema_exists, mock_config_lock, source_config_service, mock_app_config, sample_source_config):
        sample_source_config.disabled = True # Start disabled
        # mock_app_config.get_sources is ignored now

        mock_config_lock.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_config_lock.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("datacast.config.unified.DatacastConfig") as mock_DatacastConfig:
            mock_DatacastConfig.get_source.return_value = sample_source_config
            
            # BaseConfigService.update_config modifies object in place.
            # But wait, BaseConfigService calls self.get_config(id).
            # SourceConfigService overrides get_config to use DatacastConfig.
            # So it gets sample_source_config from mock.
            # Then it modifies it.
            
            updated_config = await source_config_service.update_config("test_source", {"disabled": False})
            assert updated_config.disabled is False
            mock_schema_exists.assert_called_once_with("test_source")

    @pytest.mark.asyncio
    @patch('datacast.domain.configs.source.config_lock')
    @patch('datacast.domain.schema_cache.is_schema_valid', return_value=False)
    async def test_update_config_enable_without_schema(self, mock_schema_exists, mock_config_lock, source_config_service, mock_app_config, sample_source_config):
        sample_source_config.disabled = True # Start disabled

        mock_config_lock.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_config_lock.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("datacast.config.unified.DatacastConfig") as mock_DatacastConfig:
            mock_DatacastConfig.get_source.return_value = sample_source_config
            
            with pytest.raises(ValueError, match="Cannot enable source 'test_source': Schema cache is not validated. Please run 'discover-schema' for this source first."):
                await source_config_service.update_config("test_source", {"disabled": False})
        mock_schema_exists.assert_called_once_with("test_source")

    @pytest.mark.asyncio
    @patch('datacast.domain.configs.source.config_lock')
    async def test_cleanup_obsolete_configs(self, mock_config_lock, source_config_service, mock_app_config):
        sources = {
            "src1": SourceConfig(driver="d", uri="u", credential=PasswdCredential(user="u"), disabled=True), # Obsolete
            "src2": SourceConfig(driver="d", uri="u", credential=PasswdCredential(user="u"), disabled=False), # Not obsolete (enabled)
            "src3": SourceConfig(driver="d", uri="u", credential=PasswdCredential(user="u"), disabled=True), # Obsolete
            "src4": SourceConfig(driver="d", uri="u", credential=PasswdCredential(user="u"), disabled=False), # Not obsolete (enabled)
        }
        
        mock_app_config.get_pipes.return_value = {
            "p1": PipeConfig(source="src2", sender="r1", disabled=False),
            "p2": PipeConfig(source="src4", sender="r1", disabled=False),
        }
        
        mock_config_lock.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_config_lock.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("datacast.config.unified.DatacastConfig") as mock_DatacastConfig:
            mock_DatacastConfig.get_all_sources.return_value = sources
            # delete_config calls get_config which calls get_source
            def get_source_side_effect(id):
                return sources.get(id)
            mock_DatacastConfig.get_source.side_effect = get_source_side_effect
            
            mock_DatacastConfig.get_all_pipes.return_value = {
                "p1": PipeConfig(source="src2", sender="r1", disabled=False),
                "p2": PipeConfig(source="src4", sender="r1", disabled=False),
            }
            
            # We also need to mock valid schema check if clean up does it? No.
            
            # cleanup calls _delete_config_from_app which calls DatacastConfig.delete_source
            
            # AND schema_cache.delete_schema
            with patch('datacast.domain.schema_cache.delete_schema') as mock_delete_schema:
                deleted_ids = await source_config_service.cleanup_obsolete_configs()

                assert sorted(deleted_ids) == sorted(["src1", "src3"])
                # We check DatacastConfig.delete_source
                assert mock_DatacastConfig.delete_source.call_count == 2
                mock_DatacastConfig.delete_source.assert_any_call("src1")
                mock_DatacastConfig.delete_source.assert_any_call("src3")

    @pytest.mark.asyncio
    @patch('datacast.domain.configs.source.config_lock')
    @patch('datacast.domain.schema_cache.is_schema_valid')
    async def test_check_and_disable_missing_schema_sources(self, mock_schema_exists, mock_config_lock, source_config_service, mock_app_config):
        # Setup initial state: one enabled source with no schema, one enabled with schema, one disabled with no schema
        source_no_schema = SourceConfig(driver="d", uri="u", credential=PasswdCredential(user="u"), disabled=False, schema_cached=None)
        source_with_schema = SourceConfig(driver="d", uri="u", credential=PasswdCredential(user="u"), disabled=False, schema_cached=True)
        source_disabled_no_schema = SourceConfig(driver="d", uri="u", credential=PasswdCredential(user="u"), disabled=True, schema_cached=None)

        sources_map = {
            "src_no_schema": source_no_schema,
            "src_with_schema": source_with_schema,
            "src_disabled_no_schema": source_disabled_no_schema,
        }

        mock_schema_exists.side_effect = {
            "src_no_schema": False,
            "src_with_schema": True,
            "src_disabled_no_schema": False,
        }.get

        mock_config_lock.return_value.__aenter__ = AsyncMock(return_value=None)
        mock_config_lock.return_value.__aexit__ = AsyncMock(return_value=None)

        with patch("datacast.config.unified.DatacastConfig") as mock_DatacastConfig:
            mock_DatacastConfig.get_all_sources.return_value = sources_map
            
            # Setup get_source side effect BEFORE calling the method
            def get_source_side_effect(id):
                return sources_map.get(id)
            mock_DatacastConfig.get_source.side_effect = get_source_side_effect
            
            # Do not mock disable, let it call the actual method
            disabled_sources = await source_config_service.check_and_disable_missing_schema_sources()

            assert disabled_sources == ["src_no_schema"]
            # To assert disabled was called, we need to check if update_config was called with disabled: True
            # But update_config calls get_config which calls DatacastConfig.get_source
            # So we need to ensure get_source returns the correct config object
            # BUT wait, update_config uses DatacastConfig.update_config? No.
            # SourceConfigService.update_config calls super().update_config.
            # BaseConfigService.update_config modifies the config object IN PLACE.
            
            # So enable DatacastConfig.get_source too
            def get_source_side_effect(id):
                return sources_map.get(id)
            mock_DatacastConfig.get_source.side_effect = get_source_side_effect
            
            assert source_disabled_no_schema.disabled is True  # Wait, logic above says src_no_schema is disabled
            assert source_no_schema.disabled is True # Correct

    

    @pytest.mark.asyncio
    async def test_discover_and_cache_fields_source_not_found(self, source_config_service):
        with patch("datacast.config.unified.DatacastConfig") as mock_DatacastConfig:
            mock_DatacastConfig.get_source.return_value = None
            with pytest.raises(ValueError, match="Source config 'non_existent' not found."):
                await source_config_service.discover_and_cache_fields("non_existent", "admin", "pass")

    @pytest.mark.asyncio
    @patch('datacast_core.models.config.PasswdCredential')
    @patch('datacast.domain.drivers.source_driver.SourceDriverService')
    @patch('datacast.domain.schema_cache.save_source_schema')
    async def test_discover_and_cache_fields_success(self, mock_save_schema, mock_source_driver_service, mock_passwd_credential, source_config_service, mock_app_config, sample_source_config):
        with patch("datacast.config.unified.DatacastConfig") as mock_DatacastConfig:
            mock_DatacastConfig.get_source.return_value = sample_source_config
            mock_source_driver_instance = MagicMock()
            mock_source_driver_instance.get_available_fields = AsyncMock(return_value={"field1": "type1"})
            mock_source_driver_service.return_value = mock_source_driver_instance
    
            await source_config_service.discover_and_cache_fields("test_source", "admin", "pass")
    
            mock_passwd_credential.assert_called_once_with(user="admin", passwd="pass")
            mock_source_driver_instance.get_available_fields.assert_called_once()
            mock_save_schema.assert_called_once_with("test_source", {"field1": "type1"})

    @pytest.mark.asyncio
    @patch('datacast_core.models.config.PasswdCredential')
    @patch('datacast.domain.drivers.source_driver.SourceDriverService')
    @patch('datacast.domain.schema_cache.save_source_schema')
    async def test_discover_and_cache_fields_driver_error(self, mock_save_schema, mock_source_driver_service, mock_passwd_credential, source_config_service, mock_app_config, sample_source_config):
        with patch("datacast.config.unified.DatacastConfig") as mock_DatacastConfig:
            mock_DatacastConfig.get_source.return_value = sample_source_config
            mock_source_driver_instance = MagicMock()
            mock_source_driver_instance.get_available_fields = AsyncMock(side_effect=ValueError("Driver connection failed"))
            mock_source_driver_service.return_value = mock_source_driver_instance
    
            with pytest.raises(ValueError, match="Driver connection failed"):
                await source_config_service.discover_and_cache_fields("test_source", "admin", "pass")
    
            mock_save_schema.assert_not_called()
