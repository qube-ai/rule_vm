import json

from google.cloud import pubsub_v1
from loguru import logger

from vm import VM

# Start the VM and add rules from DB
rule_vm = VM()
rule_vm.load_rules_from_db()


def door_sensor_dev_callback(message):
    # Parse the incoming message
    try:
        raw_string = message.data.decode("utf-8")
        logger.debug(f"raw message -> {raw_string}")
        data_packet = json.loads(raw_string)
    except json.JSONDecodeError:
        logger.error("Unable to decode JSON")
        return

    conditions = ["state" in data_packet]

    if all(conditions):
        # Parsed data packet
        parsed_packet = {
            # General information about the message
            "message_id": message.message_id,
            "deviceId": message.attributes["deviceId"],
            "deviceNumId": message.attributes["deviceNumId"],
            "datetime": message.publish_time,
            # Unpacking data from the original packet
            "state": data_packet["state"],
        }

        # Log the prepared packet
        logger.debug(f"Parsed door sensor device packet -> {parsed_packet}")

        # Send the reading to rule engine
        # If the packet goes through without any errors, ack
        # If there is some error while executing it's rule, dont ack the msg
        # main(last_reading)
        # engine.execute_rule(parsed_packet)
        # TODO raise the correct rule to execute here depending on the message

        # Acknowledge Cloud PubSub message
        message.ack()

    else:
        logger.error(
            "Some keys were not found in the incoming packet. Discarding message."
        )
        logger.debug(f"Packet with incorrect keys -> {data_packet}")


def energy_meter_dev_callback(message):
    # Parse the incoming message
    try:
        raw_string = message.data.decode("utf-8")
        logger.debug(f"raw message -> {raw_string}")
        data_packet = json.loads(raw_string)

    except json.JSONDecodeError:
        logger.error("Unable to decode JSON")
        return

    conditions = [
        "voltage" in data_packet,
        "current" in data_packet,
        "frequency" in data_packet,
        "pf" in data_packet,
        "power" in data_packet,
        "energy" in data_packet,
    ]

    if all(conditions):

        # Parsed data packet
        parsed_packet = {
            # General information about the message
            "message_id": message.message_id,
            "deviceId": message.attributes["deviceId"],
            "deviceNumId": message.attributes["deviceNumId"],
            "datetime": message.publish_time,
            # Unpacking data from the original packet
            "voltage": data_packet["voltage"],
            "current": data_packet["current"],
            "frequency": data_packet["frequency"],
            "pf": data_packet["pf"],
            "power": data_packet["power"],
            "energy": data_packet["energy"],
        }

        # Log the prepared packet
        logger.debug(f"Parsed energy meter device packet -> {parsed_packet}")

        # Send the reading to rule engine
        # If the packet goes through without any errors, ack
        # If there is some error while executing it's rule, dont ack the msg
        # engine.execute_rule(parsed_packet)
        # TODO raise the correct rule to execute here depending on the message

        # Acknowledge Cloud PubSub message
        message.ack()

    else:
        logger.error(
            "Some keys were not found in the incoming packet. Discarding message."
        )
        logger.debug(f"Packet with incorrect keys -> {data_packet}")


def occupancy_dev_callback(message):
    # Parse the incoming message
    try:
        raw_string = message.data.decode("utf-8")
        logger.debug(f"raw message -> {raw_string}")
        data_packet = json.loads(raw_string)
    except json.JSONDecodeError:
        logger.error("Unable to decode JSON")
        return

    conditions = [True]

    if all(conditions):
        # Parsed data packet
        parsed_packet = {
            # General information about the message
            "message_id": message.message_id,
            "deviceId": message.attributes["deviceId"],
            "deviceNumId": message.attributes["deviceNumId"],
            "datetime": message.publish_time,
        }

        # Log the prepared packet
        logger.debug(f"Parsed door sensor device packet -> {parsed_packet}")

        # Send the reading to rule engine
        # If the packet goes through without any errors, ack
        # If there is some error while executing it's rule, dont ack the msg
        # engine.execute_rule(parsed_packet)
        # TODO raise the correct rule to execute here depending on the message

        # Acknowledge Cloud PubSub message
        message.ack()

    else:
        logger.error(
            "Some keys were not found in the incoming packet. Discarding message."
        )
        logger.debug(f"Packet with incorrect keys -> {data_packet}")


def switch_device_callback(message):
    # Parse the incoming message
    try:
        raw_string = message.data.decode("utf-8")
        logger.debug(f"raw message -> {raw_string}")
        data_packet = json.loads(raw_string)
    except json.JSONDecodeError:
        logger.error("Unable to decode JSON")
        return

    conditions = ["relay_state" in data_packet, "temperature_sensor" in data_packet]

    if all(conditions):

        # Parsed data packet
        parsed_packet = {
            # General information about the message
            "message_id": message.message_id,
            "deviceId": message.attributes["deviceId"],
            "deviceNumId": message.attributes["deviceNumId"],
            "datetime": message.publish_time,
            # Unpacking data from the original packet
            "relay_state": data_packet["relay_state"],
            "temperature_sensor": data_packet["temperature_sensor"],
        }

        # Log the prepared packet
        logger.debug(f"Parsed door sensor device packet -> {parsed_packet}")

        # Send the reading to rule engine
        # If the packet goes through without any errors, ack
        # If there is some error while executing it's rule, dont ack the msg
        # engine.execute_rule(parsed_packet)
        # TODO raise the correct rule to execute here depending on the message

        # Acknowledge Cloud PubSub message
        message.ack()

    else:
        logger.error(
            "Some keys were not found in the incoming packet. Discarding message."
        )
        logger.debug(f"Packet with incorrect keys -> {data_packet}")


project_id = "podnet-switch"

subscriber = pubsub_v1.SubscriberClient()
flow_control = pubsub_v1.types.FlowControl(max_messages=10)

# Door-Window Sensor subscription
door_window_sub = subscriber.subscription_path(project_id, "door-sensor-data-sub")
door_window_future = subscriber.subscribe(
    door_window_sub, callback=door_sensor_dev_callback, flow_control=flow_control
)
logger.info(f"Listening for messages on {door_window_sub}...")

# Energy Meter subscription
energy_meter_sub = subscriber.subscription_path(project_id, "energy-meter-data-sub")
energy_meter_future = subscriber.subscribe(
    energy_meter_sub, callback=energy_meter_dev_callback, flow_control=flow_control
)
logger.info(f"Listening for messages on {energy_meter_sub}...")

# Occupancy Sensor subscription
occupancy_sub = subscriber.subscription_path(project_id, "occupancy-data-sub")
occupancy_future = subscriber.subscribe(
    occupancy_sub, callback=occupancy_dev_callback, flow_control=flow_control
)
logger.info(f"Listening for messages on {occupancy_sub}...")

# Podnet Switch subscription
switch_sub = subscriber.subscription_path(project_id, "podnet-switch-data-sub")
switch_future = subscriber.subscribe(
    switch_sub, callback=switch_device_callback, flow_control=flow_control
)
logger.info(f"Listening for messages on {switch_sub}...")

# Section responsible for pulling messages from PubSub
with subscriber:
    try:
        door_window_future.result()
        energy_meter_future.result()
        occupancy_future.result()
        switch_future.result()

    except TimeoutError as e:
        logger.error(f"Request timed out. Error: {e}")

    except Exception as ex:
        logger.error(
            f"Some error happened in the underlying execution. PubSub Callback Error: {ex}"
        )
        door_window_future.cancel()
        energy_meter_future.cancel()
        occupancy_future.cancel()
        switch_future.cancel()
