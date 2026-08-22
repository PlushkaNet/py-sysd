from typing import Any

class SysDException(Exception):
    """Base exception for all SysD exceptions"""

class ValidationError(SysDException):
    """Raised if some expected field in config invalid or missing"""
    def __init__(self, what: str):
        self._what = what
    
    def what(self):
        return self._what

class NoServiceError(SysDException):
    """Raised if service send signal() to unknown service"""

def validate(value, _type):
    """
    Validates `value` with expected `_type`
    Does nothing on success
    Raises ValidationError() on failure
    """
    if not isinstance(value, _type):
        raise ValidationError(f"{value!r} is not {_type.__name__!r} type")

def important_missing(data: dict[str, Any], *args) -> bool:
    """
    Checks if the required key is missing from the dictionary
    Returns True if the key is absent
    Returns False if all keys are present
    """
    for i in args:
        if data.get(i, None) is None:
            return True

    return False
