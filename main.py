import logging
import sys

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from user import User


def user_to_dict(user_object: User, ctx):
    # User._address must not be serialized; omit from dict
    return dict(
        name=user.name,
        favorite_number=user.favorite_number,
        favorite_color=user.favorite_color,
        address=user.address,
    )


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

p = Producer(
    {
        "bootstrap.servers": "192.168.1.69:19092",
        "socket.timeout.ms": 1000,
        "request.timeout.ms": 1000,
        "message.timeout.ms": 1000,  # important in case the broker cannot be reached
        "retries": 1,
    }
)
with open("user.avsc") as f:
    schema_str = f.read()

schema_registry_client = SchemaRegistryClient({"url": "http://192.168.1.69:8081"})
# schema = Schema(schema_str=schema_str, schema_type="AVRO")
# schema_registry_client.register_schema(schema=schema, subject="test_python_producer")
# schema_registry_client.set_compatibility()
avro_serializer = AvroSerializer(
    schema_registry_client, schema_str=schema_str, to_dict=user_to_dict
)

TOPIC = "test_python_producer"


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        log.info("Message delivery failed: {}".format(err))
    else:
        log.info("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))


l = [
    User(
        name="foo_name",
        address="foo_address",
        favorite_color="blaue",
        favorite_number=42,
    )
]

log.info("Sending data to kafka")
for user in l:
    log.info(f"Sending data: {user.__dict__}")
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    p.produce(
        TOPIC,
        value=avro_serializer(user, SerializationContext(TOPIC, MessageField.VALUE)),
        callback=delivery_report,
    )

log.info("Flushing data to kafka")

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()

log.info("Done")
