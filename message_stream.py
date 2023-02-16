from confluent_kafka import Producer
from nipyapi import canvas, nifi
import time
import json

def produce_kafka_message(producer, topic, message):
    """
    Produce a message to a Kafka topic using a Kafka producer.

    :param producer: The Kafka producer to use.
    :param topic: The Kafka topic to send the message to.
    :param message: The message to send.
    """
    producer.produce(topic, message.encode('utf-8'))
    producer.flush()

def run_nifi_workflow(process_group_name, input_port_name, output_port_name, kafka_topic):
    """
    Run a NiFi workflow that reads data from an input port, sends it to a Kafka topic, and then writes it to an output port.

    :param process_group_name: The name of the NiFi process group to use.
    :param input_port_name: The name of the NiFi input port to use.
    :param output_port_name: The name of the NiFi output port to use.
    :param kafka_topic: The name of the Kafka topic to use.
    """
    # Get the input port and output port by name
    input_port = nifi.ProcessorApi().get_processor(nifi.ProcessorTypes.INPUT_PORT, input_port_name, True)
    output_port = nifi.ProcessorApi().get_processor(nifi.ProcessorTypes.OUTPUT_PORT, output_port_name, True)

    # Create a connection between the input port and a Kafka producer
    connection1 = canvas.create_connection(
        input_port, 
        nifi.ProcessorApi().get_processor(nifi.ProcessorTypes.KAFKA_PRODUCER, "Kafka Producer", True), 
        "success")

    # Create a connection between the Kafka producer and the output port
    connection2 = canvas.create_connection(
        nifi.ProcessorApi().get_processor(nifi.ProcessorTypes.KAFKA_PRODUCER, "Kafka Producer", True), 
        output_port, 
        "success")

    # Set the Kafka topic property on the Kafka producer
    canvas.update_processor(
        nifi.ProcessorApi().get_processor(nifi.ProcessorTypes.KAFKA_PRODUCER, "Kafka Producer", True),
        properties={"topic": kafka_topic}
    )

    # Start the NiFi process group
    canvas.schedule_process_group(canvas.get_process_group(process_group_name), True)

if __name__ == '__main__':
    # Set up the Kafka producer
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    # Produce some messages to the Kafka topic
    for i in range(10):
        message = {'id': i, 'name': 'Message ' + str(i)}
        produce_kafka_message(producer, 'my-topic', json.dumps(message))
        time.sleep(1)

    # Run the NiFi workflow
    run_nifi_workflow('My Process Group', 'My Input Port', 'My Output Port', 'my-topic')
