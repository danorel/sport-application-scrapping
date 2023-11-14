import datetime as dt


def datetime2isostring(datetime: dt.datetime, timespec="milliseconds"):
    return datetime.isoformat(timespec=timespec)


def isostringvalid(iso: str):
    try:
        dt.datetime.fromisoformat(iso.replace('Z', '+00:00'))
    except:
        return False
    return True


def isostring2datetime(iso: str, default_value=None, utc=True):
    if not isostringvalid(iso):
        return default_value
    datetime = dt.datetime.fromisoformat(iso)
    if utc:
        datetime = datetime.astimezone(dt.timezone.utc)
    return datetime
