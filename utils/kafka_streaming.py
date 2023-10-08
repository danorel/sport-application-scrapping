import msgpack
import typing as t


def serialize(obj: t.Any) -> str:
    return msgpack.dumps(obj)


def deserialize(b: bytes) -> t.Any:
    return msgpack.unpackb(b)
