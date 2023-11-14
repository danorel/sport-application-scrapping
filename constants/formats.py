import typing as t


class ActivityMeasurement(t.TypedDict):
    elevation: t.Optional[float]
    latitude: t.Optional[float]
    longitude: t.Optional[float]
    ISOString: t.Optional[str]
    speed: t.Optional[float]


class Activity(t.TypedDict):
    id: str
    data: t.Optional[list[ActivityMeasurement]]


class Athlete(t.TypedDict):
    name: str
    createdAt: str


class ReadyToExtractFormat(t.TypedDict):
    activityURL: str
    athleteURL: str


class ReadyToTransformLoadFormat(t.TypedDict):
    activity: Activity
    athlete: Athlete
