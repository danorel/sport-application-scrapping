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

CYPHER_ATHLETES_WITH_ONE_ACTIVITY = """
MATCH (at:Athlete)-[:TRACKS]->(ac:Activity) 
WITH at, count(ac) as rels
WHERE rels = 1
RETURN at
"""


def insert_data(tx, ready_to_transform_load_format: ReadyToTransformLoadFormat):
    gpx, user = (
        ready_to_transform_load_format["gpx"],
        ready_to_transform_load_format["user"]
    )
    if gpx:
        create_gpx_point_query = (
            "CREATE"
            "(am:ActivityMeasurement {elevation: $elevation, latitude: $latitude, longitude: $longitude, ISOString: $ISOString, speed: $speed})"
            "RETURN ID(am) as activityMeasurementId"
        )
        activity_measurement_ids = []
        for activity_measurement in gpx["data"]:
            query_result = tx.run(
                create_gpx_point_query,
                elevation=activity_measurement["elevation"],
                latitude=activity_measurement["latitude"],
                longitude=activity_measurement["longitude"],
                ISOString=activity_measurement["ISOString"],
                speed=activity_measurement["speed"],
            )
            response = query_result.single()
            activity_measurement_ids.append(response["activityMeasurementId"])

        create_activity_query = (
            "CREATE"
            "(ac:Activity {externalId: $id, name: $name, HAS_DATA_IN: $activity_measurement_ids})"
            "RETURN ID(ac) as activityId"
        )
        query_result = tx.run(
            create_activity_query,
            id=gpx["id"],
            name=gpx["name"],
            activity_measurement_ids=activity_measurement_ids
        )
        response = query_result.single()
        activity_id = response["activityId"]

        create_athlete_query = (
            "CREATE"
            "(at:Athlete {externalId: $id, name: $name, createdAt: $created_at, TRACKS: $activity_id})"
            "RETURN ID(at) as athleteId"
        )
        query_result = tx.run(
            create_athlete_query,
            id=user["id"],
            name=user["name"],
            created_at=user["createdAt"],
            activity_id=activity_id
        )
        response = query_result.single()
        athlete_id = response["athleteId"]

        activity_data_relationship_query = (
            "MATCH (ac:Activity)"
            "MATCH (am:ActivityMeasurement)"
            "WHERE (ID(ac) = $activity_id AND ID(am) IN ac.HAS_DATA_IN)"
            "MERGE (ac)-[:HAS_DATA_IN]->(am);"
        )
        tx.run(activity_data_relationship_query, activity_id=activity_id)

        athlete_activity_relationship_query = (
            "MATCH (at:Athlete)"
            "MATCH (ac:Activity)"
            "WHERE (ID(at) = $athlete_id AND ID(ac) = at.TRACKS)"
            "MERGE (at)-[:TRACKS]->(ac);"
        )
        tx.run(athlete_activity_relationship_query, athlete_id=athlete_id)
    else:
        create_athlete_query = (
            "CREATE"
            "(at:Athlete {externalId: $id, name: $name, createdAt: $created_at})"
            "RETURN ID(at) as athleteId"
        )
        query_result = tx.run(
            create_athlete_query,
            id=user["id"],
            name=user["name"],
            created_at=user["createdAt"]
        )


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
