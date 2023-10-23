import datetime as dt
import json
import gpxpy
import gpxpy.gpx
import numpy as np
import pandas as pd
import requests
import traceback

from bs4 import BeautifulSoup
from kafka import KafkaConsumer, KafkaProducer

from constants.domain import (
    MAX_CYCLING_LIMIT,
    MAX_RUNNING_LIMIT,
    MAX_WALKING_LIMIT,
)
from constants.formats import Activity, ActivityMeasurement, Athlete, ReadyToExtractFormat, ReadyToTransformLoadFormat
from constants.scrapping import OPENSTREETMAP_BASE_URL
from constants.kafka import (
    READY_TO_EXTRACT_TOPIC,
    READY_TO_TRANSFORM_LOAD_TOPIC,
)
from utils.dict import flatten
from utils.kafka import deserialize, serialize
from utils.logging import logger

kafka_consumer = KafkaConsumer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_deserializer=deserialize
)
kafka_consumer.subscribe([READY_TO_EXTRACT_TOPIC])

kafka_producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=serialize,
    security_protocol="PLAINTEXT"
)


def filter_from_none(point: dict):
    return {k: v for k, v in point.items() if v is not None}


def mean_attr_or_none(activity_data: list[ActivityMeasurement], attr: str):
    sequence = [
        activity_measurement[attr]
        for activity_measurement in activity_data
        if attr in activity_measurement
    ]
    if not len(sequence):
        return None
    return np.array(sequence).mean()


def classify_by_activity_data(activity_data: list[ActivityMeasurement]):
    mean_speed_in_meters_per_second = mean_attr_or_none(
        activity_data, attr='speed')
    if not mean_speed_in_meters_per_second:
        return "other"
    else:
        if mean_speed_in_meters_per_second < MAX_WALKING_LIMIT:
            return "walking"
        elif mean_speed_in_meters_per_second < MAX_RUNNING_LIMIT:
            return "running"
        elif mean_speed_in_meters_per_second < MAX_CYCLING_LIMIT:
            return "cycling"
        else:
            return "other"


def isostring_no_microseconds(datetime: dt.datetime):
    if not datetime:
        return None
    return datetime.replace(microsecond=0).isoformat(timespec="milliseconds")


def interpolate_points(points, attrs: list[str] = ['elevation', 'latitude', 'longitude', 'time', 'speed']):
    for point in points:
        timestamp_no_microseconds = isostring_no_microseconds(
            getattr(point, "time"))
        if timestamp_no_microseconds:
            setattr(point, "time", timestamp_no_microseconds)
    samples = []
    for point in points:
        sample = {}
        for attr in attrs:
            sample[attr] = getattr(point, attr, None)
        samples.append(sample)
    grouped_mean_df = (
        pd.DataFrame(samples)
        .groupby("time", as_index=False)
        .mean(numeric_only=True)
        .drop_duplicates()
    )
    datetime_data = pd.to_datetime(
        grouped_mean_df["time"], utc=True, errors="coerce"
    )
    if not len(datetime_data):
        return []
    interpolated_df = (
        grouped_mean_df.set_index(pd.DatetimeIndex(datetime_data))
        .drop(
            columns=["time"],
            axis=1,
            errors="ignore",
        )
        .resample("1s")
        .interpolate(method="time")
        .reset_index()
        .rename(columns={"index": "time"})
    )
    if interpolated_df.isnull().values.any():
        return []
    interpolated_df["ISOString"] = interpolated_df["time"].map(
        lambda datetime: datetime.isoformat(timespec="milliseconds")
    )
    interpolated_df.drop("time", inplace=True, axis=1)
    return interpolated_df.to_dict(orient="records")


def extract_activity_data(gpx):
    tracks = gpx.tracks
    segments = flatten([track.segments for track in tracks])
    points = flatten([segment.points for segment in segments])
    activity_data = [
        filter_from_none(ActivityMeasurement(**point))
        for point in interpolate_points(points)
    ]
    return activity_data


def extract_activity(html_url: str):
    html_doc = requests.get(html_url)
    soup = BeautifulSoup(html_doc.content, "html.parser")
    file_tr, *rest_tr = (
        soup
        .find("table")
        .find_all("tr")
    )
    try:
        gpx_href = file_tr.find("a").get("href")
        gpx_link = f"{OPENSTREETMAP_BASE_URL}/{gpx_href}"
        gpx_file = requests.get(gpx_link, allow_redirects=True)
        gpx_data = gpxpy.parse(gpx_file.content)
        gpx_id = gpx_href.split("/").pop(-2)
        activity_data = extract_activity_data(gpx_data)
        activity = Activity(
            id=gpx_id,
            name=gpx_data.name,
            classification=classify_by_activity_data(activity_data),
            data=activity_data
        )
        return filter_from_none(activity)
    except Exception:
        logger.info(f"Couldn't parse page: {gpx_link}")
        traceback.print_exc()
        return None


def extract_athlete(html_url: str):
    html_doc = requests.get(html_url)
    soup = BeautifulSoup(html_doc.content, "html.parser")
    content_div = (
        soup
        .find("div", attrs={"class": "content-heading"})
        .find("div", attrs={"class": "content-inner"})
    )
    created_at_dd = (
        content_div.find("div", attrs={"class": "text-muted"})
        .find("dl")
        .find("dd")
    )
    name_h1 = content_div.find("h1")
    _, _, traces_li, *rest_li = (
        content_div.find("nav", attrs={"class": "secondary-actions"})
        .find_all("li")
    )
    athlete = Athlete(
        name=str(name_h1.getText()),
        createdAt=str(created_at_dd.getText())
    )
    return filter_from_none(athlete)


def extract_data(ready_to_extract_format: ReadyToExtractFormat) -> ReadyToTransformLoadFormat:
    activity_html_url, athlete_html_url = (
        ready_to_extract_format["activityURL"],
        ready_to_extract_format["athleteURL"],
    )
    ready_to_transform_load_format = ReadyToTransformLoadFormat(
        activity=extract_activity(html_url=activity_html_url),
        athlete=extract_athlete(html_url=athlete_html_url)
    )
    return ready_to_transform_load_format


for kafka_message in kafka_consumer:
    logger.info("%s:%d:%d: key=%s value=%s" % (
        kafka_message.topic,
        kafka_message.partition,
        kafka_message.offset,
        kafka_message.key,
        json.dumps(kafka_message.value)
    ))
    ready_to_transform_load_format = extract_data(
        ReadyToExtractFormat(**kafka_message.value))
    kafka_producer.send(READY_TO_TRANSFORM_LOAD_TOPIC,
                        ready_to_transform_load_format)
