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


def filter_keys(d: dict, unwanted_keys: list[str]):
    f = d.copy()
    for unwanted_key in unwanted_keys:
        del f[unwanted_key]
    return f


def make_dynamic_statement(d: dict, key_mapping: dict = None):
    dynamic_prompts = []
    for k, v in d.items():
        if key_mapping is not None and k in key_mapping:
            renamed_k = key_mapping[k]
            dynamic_prompts.append(f"{renamed_k}: ${k}")
        else:
            dynamic_prompts.append(f"{k}: ${k}")
    return "{" + ', '.join(dynamic_prompts) + "}"


def insert_data(tx, ready_to_transform_load_format: ReadyToTransformLoadFormat):
    activity, athlete = (
        ready_to_transform_load_format["activity"],
        ready_to_transform_load_format["athlete"]
    )
    if activity:
        activity_measurement_ids = []
        for activity_measurement in activity["data"]:
            activity_measurement_params = activity_measurement
            activity_measurement_stmt = make_dynamic_statement(
                d=activity_measurement_params,
                key_mapping={
                    "id": "externalId",
                }
            )
            create_activity_measurement_query = f"""
                CREATE (am:ActivityMeasurement {activity_measurement_stmt})
                RETURN ID(am) as activityMeasurementId;
            """
            query_result = tx.run(
                create_activity_measurement_query, **activity_measurement_params)
            response = query_result.single()
            activity_measurement_ids.append(response["activityMeasurementId"])

        activity_params = filter_keys(activity, unwanted_keys=['id', 'data'])
        activity_stmt = make_dynamic_statement(activity_params)
        merge_activity_query = f"""
            MERGE (ac:Activity {activity_stmt})
            ON CREATE 
                SET ac.HAS_DATA_IN = $activityMeasurementIds
            RETURN ID(ac) as activityId;
        """
        query_result = tx.run(
            merge_activity_query,
            **{
                **activity_params,
                "activityMeasurementIds": activity_measurement_ids
            }
        )
        response = query_result.single()
        activity_id = response["activityId"]

        athlete_params = athlete
        athlete_stmt = make_dynamic_statement(athlete)
        merge_athlete_query = f"""
            MERGE (at:Athlete {athlete_stmt})
            ON CREATE 
                SET at.TRACKS = $activityId
            RETURN ID(at) as athleteId;
        """
        query_result = tx.run(
            merge_athlete_query,
            **{
                **athlete_params,
                "activityId": activity_id
            }
        )
        response = query_result.single()
        athlete_id = response["athleteId"]

        activity_data_relationship_query = """
            MATCH (ac:Activity)
            MATCH (am:ActivityMeasurement)
            WHERE (ID(ac) = $activityId AND ID(am) IN ac.HAS_DATA_IN)
            MERGE (ac)-[:HAS_DATA_IN]->(am);
        """
        tx.run(activity_data_relationship_query, activityId=activity_id)

        athlete_activity_relationship_query = """
            MATCH (at:Athlete)
            MATCH (ac:Activity)
            WHERE (ID(at) = $athleteId AND ID(ac) = at.TRACKS)
            MERGE (at)-[:TRACKS]->(ac);
        """
        tx.run(athlete_activity_relationship_query, athleteId=athlete_id)
    else:
        athlete_params = athlete
        athlete_stmt = make_dynamic_statement(athlete)
        merge_athlete_query = f"""
            MERGE (at:Athlete {athlete_stmt})
            RETURN ID(at) as athleteId;
        """
        query_result = tx.run(
            merge_athlete_query,
            **{
                **athlete_params,
            }
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
