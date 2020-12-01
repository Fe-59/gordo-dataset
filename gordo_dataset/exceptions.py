from typing import Dict, Optional


class ConfigException(ValueError):
    """
    Exception in case if configuration is broken. Could be used for validation errors

    """
    def __init__(self, msg: str, fields: Optional[Dict] = None):
        if fields is None:
            fields = {}
        self.fields = fields
        super().__init__(msg)
