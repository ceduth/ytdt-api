import json
from datetime import datetime
from collections import defaultdict


__all__ = ("DateTimeEncoder", )


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, defaultdict):
            return self._convert(dict(obj))
        if isinstance(obj, (list, tuple, dict)):
            return self._convert(obj)
        return super().default(obj)

    def _convert(self, obj):
        if isinstance(obj, dict):
            return {k: self._convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert(v) for v in obj]
        elif isinstance(obj, tuple):
            return tuple(self._convert(v) for v in obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, defaultdict):
            return self._convert(dict(obj))
        else:
            return obj
