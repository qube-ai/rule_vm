import json

from google.cloud import pubsub_v1
from loguru import logger

from vm import VM

# Start the VM and add rules from DB
rule_vm = VM()
rule_vm.sync_rules()


def slide_pod_callback(message):
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


def surge_pod_1p_callback(message):
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


def surge_pod_3p_callback(message):
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


def sense_pod_callback(message):
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


def switch_pod_1chpm_callback(message):
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


def switch_pod_4ch_callback(message):
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

# Slide Pod subscription
slide_pod_sub = subscriber.subscription_path(project_id, "slide-pod-state-sub")
slide_pod_future = subscriber.subscribe(
    slide_pod_sub, callback=slide_pod_callback, flow_control=flow_control
)
logger.info(f"Listening for messages on {slide_pod_sub}...")

# Surge Pod 1 Phase subscription
surge_pod_1p_sub = subscriber.subscription_path(project_id, "surge-pod-1p-state-sub")
surge_pod_1p_future = subscriber.subscribe(
    surge_pod_1p_sub, callback=surge_pod_1p_callback, flow_control=flow_control
)
logger.info(f"Listening for messages on {surge_pod_1p_sub}...")

# Surge Pod 3 Phase subscription
surge_pod_3p_sub = subscriber.subscription_path(project_id, "surge-pod-3p-state-sub")
surge_pod_3p_future = subscriber.subscribe(
    surge_pod_3p_sub, callback=surge_pod_3p_callback, flow_control=flow_control
)
logger.info(f"Listening for messages on {surge_pod_3p_sub}...")


# Sense Pod subscription
sense_pod_sub = subscriber.subscription_path(project_id, "sense-pod-state-sub")
sense_pod_future = subscriber.subscribe(
    sense_pod_sub, callback=sense_pod_callback, flow_control=flow_control
)
logger.info(f"Listening for messages on {sense_pod_sub}...")

# Switch Pod 1 Channel with PM subscription
switch_pod_1chpm_sub = subscriber.subscription_path(
    project_id, "switch-pod-1chpm-state-sub"
)
switch_pod_1chpm_future = subscriber.subscribe(
    switch_pod_1chpm_sub, callback=switch_pod_1chpm_callback, flow_control=flow_control
)
logger.info(f"Listening for messages on {switch_pod_1chpm_sub}...")

# Switch Pod 4 Channel subscription
switch_pod_4ch_sub = subscriber.subscription_path(
    project_id, "switch-pod-4ch-state-sub"
)
switch_pod_4ch_future = subscriber.subscribe(
    switch_pod_4ch_sub, callback=switch_pod_4ch_callback, flow_control=flow_control
)
logger.info(f"Listening for messages on {switch_pod_4ch_sub}...")


# Section responsible for pulling messages from PubSub
with subscriber:
    try:
        slide_pod_future.result()
        surge_pod_1p_future.result()
        surge_pod_3p_future.result()
        sense_pod_future.result()
        switch_pod_1chpm_future.result()
        switch_pod_4ch_future.result()

    except TimeoutError as e:
        logger.error(f"Request timed out. Error: {e}")

    except Exception as ex:
        logger.error(
            f"Some error happened in the underlying execution. PubSub Callback Error: {ex}"
        )
        slide_pod_future.cancel()
        surge_pod_1p_future.cancel()
        surge_pod_3p_future.cancel()
        sense_pod_future.cancel()
        switch_pod_1chpm_future.cancel()
        switch_pod_4ch_future.cancel()
