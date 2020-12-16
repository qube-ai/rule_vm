import json

from google.cloud import pubsub_v1
from loguru import logger

from vm import VM

# Start the VM and add rules from DB
rule_vm = VM()
rule_vm.sync_rules()


def door_sensor_dev_callback(message):
    device_id = message.attributes["deviceId"]

    # Parse the incoming message, if the message is parsable
    # only then run dependent device rules.
    try:
        raw_string = message.data.decode("utf-8")
        logger.debug(f"Raw Message from {device_id} -> {raw_string}")
        data_packet = json.loads(raw_string)
    except json.JSONDecodeError:
        logger.error(f"Unable to decode JSON message from {device_id}")
        return

    device_id = message.attributes["deviceId"]

    # Execute all rules that depend on the state of given device_id
    rule_vm.execute_all_dependent_rules(device_id)

    # Acknowledge Cloud PubSub message
    message.ack()


def energy_meter_dev_callback(message):
    device_id = message.attributes["deviceId"]

    # Parse the incoming message, if the message is parsable
    # only then run dependent device rules.
    try:
        raw_string = message.data.decode("utf-8")
        logger.debug(f"Raw Message from {device_id} -> {raw_string}")
        data_packet = json.loads(raw_string)

    except json.JSONDecodeError:
        logger.error(f"Unable to decode JSON message from {device_id}")
        return

    # Execute all rules that depend on the state of given device_id
    rule_vm.execute_all_dependent_rules(device_id)

    # Acknowledge Cloud PubSub message
    message.ack()


def occupancy_dev_callback(message):
    device_id = message.attributes["deviceId"]

    # Parse the incoming message, if the message is parsable
    # only then run dependent device rules.
    try:
        raw_string = message.data.decode("utf-8")
        logger.debug(f"Raw Message from {device_id} -> {raw_string}")
        data_packet = json.loads(raw_string)
    except json.JSONDecodeError:
        logger.error(f"Unable to decode JSON message from {device_id}")
        return

    # Execute all rules that depend on the state of given device_id
    rule_vm.execute_all_dependent_rules(device_id)

    # Acknowledge Cloud PubSub message
    message.ack()


def switch_pod_callback(message):
    # Parse the incoming message, if the message is parsable
    # only then run dependent device rules.
    device_id = message.attributes["deviceId"]

    try:
        raw_string = message.data.decode("utf-8")
        logger.debug(f"Raw Message from {device_id} -> {raw_string}")
        data_packet = json.loads(raw_string)
    except json.JSONDecodeError:
        logger.error(f"Unable to decode JSON message from {device_id}")
        return

    # Execute all rules that depend on the state of given device_id
    rule_vm.execute_all_dependent_rules(device_id)

    # Acknowledge Cloud PubSub message
    message.ack()


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
energy_meter_sub = subscriber.subscription_path(project_id, "surge-pod-state-sub")
energy_meter_future = subscriber.subscribe(
    energy_meter_sub, callback=energy_meter_dev_callback, flow_control=flow_control
)
logger.info(f"Listening for messages on {energy_meter_sub}...")

# Occupancy Sensor subscription
occupancy_sub = subscriber.subscription_path(project_id, "sense-pod-state-sub")
occupancy_future = subscriber.subscribe(
    occupancy_sub, callback=occupancy_dev_callback, flow_control=flow_control
)
logger.info(f"Listening for messages on {occupancy_sub}...")

# Podnet Switch subscription
switch_sub = subscriber.subscription_path(project_id, "switch-pod-state-sub")
switch_future = subscriber.subscribe(
    switch_sub, callback=switch_pod_callback, flow_control=flow_control
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
