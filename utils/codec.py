import base64
import json
import typing as t
import zlib

"""
Converts any Python object to bytes
"""


def encode(obj: t.Any) -> str:
    return base64.b64encode(zlib.compress(str.encode(json.dumps(obj), 'utf-8'), 6))
