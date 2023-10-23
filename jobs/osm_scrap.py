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
from utils.kafka import serialize

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
        activity_a, athlete_a, *rest_a = body_td.find_all("a")
        read_to_extract_format = ReadyToExtractFormat(
            activityURL=OPENSTREETMAP_BASE_URL + activity_a.get("href"),
            athleteURL=OPENSTREETMAP_BASE_URL + athlete_a.get("href")
        )
        yield read_to_extract_format


html_url = OPENSTREETMAP_TRACES_URL
for _ in tqdm(range(args.html_pages + 1)):
    html_doc = requests.get(html_url)
    soup = BeautifulSoup(html_doc.content, "html.parser")
    for read_to_extract_format in scrap_data(soup):
        kafka_producer.send(READY_TO_EXTRACT_TOPIC, read_to_extract_format)
    html_href = scrap_pagination(soup)
    if not html_href:
        raise ValueError("Not found navigation element while scrapping")
    html_url = f"{OPENSTREETMAP_BASE_URL}/{html_href}"
