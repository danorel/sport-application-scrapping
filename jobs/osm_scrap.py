import argparse
import requests

from bs4 import BeautifulSoup
from kafka import KafkaProducer
from tqdm import tqdm

from constants.formats import ReadyToExtractFormat
from constants.kafka import READY_TO_EXTRACT_TOPIC
from constants.scrapping import (
    OPENSTREETMAP_BASE_URL,
    OPENSTREETMAP_TRACES_URL,
)
from utils.kafka_streaming import serialize

parser = argparse.ArgumentParser(
    prog='OpenStreetMapScrapper',
    description='Scrapping OpenStreetMap data'
)
parser.add_argument('-hp', '--html_pages', type=int, default=10)
args = parser.parse_args()

kafka_producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=serialize,
    security_protocol="PLAINTEXT"
)


def scrap_pagination(soup: BeautifulSoup):
    return (
        soup.find("ul", attrs={"class": "pagination"})
        .find("a", attrs={"class": "page-link"})
        .get("href")
    )


def scrap_data(soup: BeautifulSoup):
    for tr in soup.find_all("tr"):
        head_td, body_td, *rest_td = tr.find_all("td")
        gpx_a, user_a, *rest_a = body_td.find_all("a")
        yield ReadyToExtractFormat(
            gpxURL=OPENSTREETMAP_BASE_URL + gpx_a.get("href"),
            userURL=OPENSTREETMAP_BASE_URL + user_a.get("href")
        )


html_url = OPENSTREETMAP_TRACES_URL
for _ in tqdm(range(args.html_pages)):
    html_doc = requests.get(html_url)
    soup = BeautifulSoup(html_doc.content, "html.parser")
    for data in scrap_data(soup):
        kafka_producer.send(READY_TO_EXTRACT_TOPIC, data)
    html_href = scrap_pagination(soup)
    if not html_href:
        raise ValueError("Not found navigation element while scrapping")
    html_url = f"{OPENSTREETMAP_BASE_URL}/{html_href}"
