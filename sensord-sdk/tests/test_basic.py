from sensord_sdk import __version__

def test_version_exists():
    assert __version__ is not None
