"""
Common utilities for Sensord Core.
"""

def verify_spec(spec_id: str):
    """
    Decorator to mark a function or class as verifying a specific requirement.
    This is used by the vibespec validator to track implementation coverage.
    """
    def decorator(func):
        # Attach the spec ID to the function metadata
        func._verify_spec = spec_id
        return func
    return decorator
