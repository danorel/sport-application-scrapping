import typing as t


class ScrappingFormat(t.TypedDict):
    gpxURL: str
    userURL: str


class ProcessingFormat(t.TypedDict):
    user: t.TypedDict('User', {
        'name': str
        'traces': float,
        'createdAt': str
    })
    gpx: t.TypedDict('GPX', {
        'name': str,
        'data': str
    })
