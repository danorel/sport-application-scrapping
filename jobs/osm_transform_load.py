import json

from kafka import KafkaConsumer
from neo4j import GraphDatabase

from constants.kafka import READY_TO_TRANSFORM_LOAD_TOPIC
from constants.formats import ReadyToTransformLoadFormat
from utils.logging import logger
from utils.kafka import deserialize

kafka_consumer = KafkaConsumer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_deserializer=deserialize
)
kafka_consumer.subscribe([READY_TO_TRANSFORM_LOAD_TOPIC])

neo4j_driver = GraphDatabase.driver(
    "neo4j://127.0.0.1:7687", auth=("neo4j", "password"))


def insert_data(tx, ready_to_transform_load_format: ReadyToTransformLoadFormat):
    gpx, user = (
        ready_to_transform_load_format["gpx"],
        ready_to_transform_load_format["user"]
    )
    if gpx:
        create_relation_query = (
            "CREATE"
            "(u:User {id: randomUUID(), name: $user_name, tracesCount: $user_traces_count, createdAt: $user_created_at})"
            "-[:HAS]->"
            "(g:Gpx {id: randomUUID(), name: $gpx_name})"
            "RETURN u.id as user_id, g.id as gpx_id"
        )
        query_result = tx.run(
            create_relation_query,
            user_name=user["name"],
            user_traces_count=user["tracesCount"],
            user_created_at=user["createdAt"],
            gpx_name=gpx["name"],
            gpx_data=gpx["data"]
        )
        response = query_result.single()
        return response["user_id"], response["gpx_id"]
    else:
        create_query = (
            "CREATE"
            "(u:User {id: randomUUID(), name: $user_name, tracesCount: $user_traces_count, createdAt: $user_created_at})"
            "RETURN u.id as user_id"
        )
        query_result = tx.run(
            create_query,
            user_name=user["name"],
            user_traces_count=user["tracesCount"],
            user_created_at=user["createdAt"]
        )
        response = query_result.single()
        return response["user_id"], None


def transform_load_data(ready_to_transform_load_format: ReadyToTransformLoadFormat):
    print(ready_to_transform_load_format)
    with neo4j_driver.session(database="neo4j") as session:
        session.execute_write(insert_data, ready_to_transform_load_format)


for kafka_message in kafka_consumer:
    logger.info("%s:%d:%d: key=%s value=%s" % (
        kafka_message.topic,
        kafka_message.partition,
        kafka_message.offset,
        kafka_message.key,
        json.dumps(kafka_message.value)
    ))
    transform_load_data(ReadyToTransformLoadFormat(**kafka_message.value))
