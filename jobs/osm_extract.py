import json
import gpxpy
import gpxpy.gpx
import requests
import uuid

from bs4 import BeautifulSoup
from kafka import KafkaConsumer, KafkaProducer

from constants.formats import Gpx, ReadyToExtractFormat, User
from constants.scrapping import OPENSTREETMAP_BASE_URL
from constants.kafka import (
    READY_TO_EXTRACT_TOPIC,
    READY_TO_TRANSFORM_LOAD_GPX_TOPIC,
    READY_TO_TRANSFORM_LOAD_USER_TOPIC
)
from utils.kafka_streaming import deserialize, serialize
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


def extract_gpx(html_url: str):
    html_doc = requests.get(html_url)
    soup = BeautifulSoup(html_doc.content, "html.parser")
    file_tr, *rest_tr = (
        soup
        .find("table")
        .find_all("tr")
    )
    gpx_file_href = file_tr.find("a").get("href")
    gpx_url = f"{OPENSTREETMAP_BASE_URL}/{gpx_file_href}"
    gpx_file = requests.get(gpx_url, allow_redirects=True)
    gpx = gpxpy.parse(gpx_file.content)
    return Gpx(
        id=str(uuid.uuid4()),
        name=gpx.name,
        data=[
            dict(
                elevation=route.elevation,
                latitude=route.latitude,
                longitude=route.longitude,
                time=route.time,
                speed=route.speed,
            )
            for route in gpx.routes
        ]
    )


def extract_user(html_url: str):
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
    return User(
        id=str(uuid.uuid4()),
        name=str(name_h1.getText()),
        tracesCount=int(traces_li.find("span").getText().replace(',', '')),
        createdAt=str(created_at_dd.getText())
    )


def extract_data(ready_to_extract_format: ReadyToExtractFormat):
    gpx_html_url, user_html_url = (
        ready_to_extract_format.get("gpxURL"),
        ready_to_extract_format.get("userURL"),
    )
    data = (
        extract_gpx(html_url=gpx_html_url),
        extract_user(html_url=user_html_url),
    )
    return data


if __name__ == "__main__":
    for kafka_message in kafka_consumer:
        logger.info("%s:%d:%d: key=%s value=%s" % (
            kafka_message.topic,
            kafka_message.partition,
            kafka_message.offset,
            kafka_message.key,
            json.dumps(kafka_message.value)
        ))
        gpx, user = extract_data(ReadyToExtractFormat(**kafka_message.value))
        kafka_producer.send(READY_TO_TRANSFORM_LOAD_GPX_TOPIC, gpx)
        kafka_producer.send(READY_TO_TRANSFORM_LOAD_USER_TOPIC, user)
