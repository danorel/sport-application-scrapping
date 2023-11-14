import json
import gpxpy
import gpxpy.gpx
import numpy as np
import pandas as pd
import requests
import traceback

from bs4 import BeautifulSoup
from copy import deepcopy
from geopy.distance import geodesic
from kafka import KafkaConsumer, KafkaProducer

from constants.formats import Activity, ActivityMeasurement, Athlete, ReadyToExtractFormat, ReadyToTransformLoadFormat
from constants.scrapping import OPENSTREETMAP_BASE_URL
from constants.kafka import (
    READY_TO_EXTRACT_TOPIC,
    READY_TO_TRANSFORM_LOAD_TOPIC,
)
from utils.date import isostring2datetime
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


def interpolate_points(points, attrs: list[str] = ['elevation', 'latitude', 'longitude', 'time']):
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


def calculate_speed_samples(coords, timestamps):
    """
    Calculate speed samples based on a sequence of coordinates and timestamps.

    Parameters:
    - coords: List of tuples containing (latitude, longitude) coordinates.
    - timestamps: List of datetime objects representing timestamps for each coordinate.

    Returns:
    - List of speed samples (in km/h) between consecutive points.
    """

    # Ensure there are at least two points to calculate speed
    if len(coords) < 2 or len(timestamps) < 2:
        raise ValueError("Insufficient data points for speed calculation")

    # Initialize an empty list to store speed samples
    speed_samples = []

    # Iterate through the coordinates and timestamps
    for i in range(1, len(coords)):
        # Extract information for the current and previous points
        coord1 = coords[i - 1]
        coord2 = coords[i]
        time1 = timestamps[i - 1]
        time2 = timestamps[i]

        # Calculate distance using Haversine formula
        distance = geodesic(coord1, coord2).kilometers

        # Calculate time difference in hours
        delta_time = (time2 - time1).total_seconds() / 3600.0

        # Calculate speed in km/h and append to the list
        speed = distance / delta_time
        speed_samples.append(speed)

    return speed_samples


def enrich_activity_data(activity_data):
    enriched_data = deepcopy(activity_data)

    coordinate_samples, time_samples = (
        [(x['latitude'], x['longitude']) for x in activity_data],
        [isostring2datetime(x['ISOString']) for x in activity_data]
    )

    speed_samples = calculate_speed_samples(coordinate_samples, time_samples)

    for sample, speed in zip(enriched_data, speed_samples):
        sample['speed'] = speed

    return enriched_data


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
        activity_data = enrich_activity_data(activity_data)
        activity = Activity(
            id=gpx_id,
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
    try:
        ready_to_transform_load_format = ReadyToTransformLoadFormat(
            activity=extract_activity(html_url=activity_html_url),
            athlete=extract_athlete(html_url=athlete_html_url)
        )
        return ready_to_transform_load_format
    except Exception as exception:
        logger.error(
            f"Skipping {activity_html_url} due to error {str(exception)}")
        return None


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
    if ready_to_transform_load_format is not None:
        kafka_producer.send(READY_TO_TRANSFORM_LOAD_TOPIC,
                            ready_to_transform_load_format)
