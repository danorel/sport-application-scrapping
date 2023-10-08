import csv
import logging
import requests

from bs4 import BeautifulSoup
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tqdm import tqdm

from constants.formats import ScrappingFormat
from constants.scrapping import (
    OPENSTREETMAP_BASE_URL,
    OPENSTREETMAP_TRACES_URL,
)
from utils.logging import logger

producer = KafkaProducer(bootstrap_servers=['broker1:1234'])


def parse(soup: BeautifulSoup):
    data = []
    for tr in soup.find_all("tr"):
        td_header, td_body, *td_rest = tr.find_all("td")
        a_gpx, a_usr, *a_rest = td_body.find_all("a")
        gpx_url, usr_url = (
            OPENSTREETMAP_BASE_URL + a_usr.get("href"),
            OPENSTREETMAP_BASE_URL + a_gpx.get("href"),
        )
        data.append(ScrappingFormat({
            "gpxUrl": gpx_url,
            "userUrl": usr_url,
        }))
    return data


def scrap(html_pages: int):
    html_url = OPENSTREETMAP_TRACES_URL
    for _ in tqdm(range(html_pages)):
        html_doc = requests.get(html_url)
        soup = BeautifulSoup(html_doc.content, "html.parser")
        data = parse(soup)
        html_href = (
            soup.find("ul", attrs={"class": "pagination"})
            .find("a", attrs={"class": "page-link"})
            .get("href")
        )
        if html_href:
            raise ValueError("Not found navigation element while scrapping")
        html_url = f"{OPENSTREETMAP_BASE_URL}/{html_href}"


if __name__ == "__main__":
    scrap(html_pages=10)
