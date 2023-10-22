import typing as t


class GpxPoint(t.TypedDict):
    elevation: t.Optional[float]
    latitude: t.Optional[float]
    longitude: t.Optional[float]
    ISOString: t.Optional[str]
    speed: t.Optional[float]


class Gpx(t.TypedDict):
    id: str
    name: t.Optional[str]
    data: t.Optional[list[GpxPoint]]


class User(t.TypedDict):
    id: str
    name: str
    tracesCount: float
    createdAt: str


class ReadyToExtractFormat(t.TypedDict):
    gpxURL: str
    userURL: str


class ReadyToTransformLoadFormat(t.TypedDict):
    gpx: Gpx
    user: User
