import typing as t


class Gpx(t.TypedDict):
    id: str
    name: t.Optional[str]
    data: t.Optional[str]


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
