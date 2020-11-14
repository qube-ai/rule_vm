from typing import Dict

import arrow
from loguru import logger

import datetime
import pytz
import store
from .base import BaseInstruction
from .base import InstructionConstant
import math


class CheckOccupancy(BaseInstruction):
    instruction_type = InstructionConstant.OCCUPANCY
    name = "CHECK_OCCUPANCY"
    OCCUPANCY_SENSOR_DATA_INTERVAL = (
        1 * 60
    )  # Device sends data every 1 minute, till the device is on
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string"},
            "device_id": {"type": "string"},
            "state": {"type": "string", "enum": ["occupied", "unoccupied"]},
        },
        "required": ["operation", "device_id", "state"],
    }

    def __init__(self, json_data: Dict, rule):
        super(CheckOccupancy, self).__init__(json_data, rule)
        self.target_state = self.json_data["state"].lower()
        self.device_id = self.json_data["device_id"]

    async def evaluate(self, vm_instance):
        current_state = await self.get_current_state()
        logger.debug(
            f"Evaluating occupancy sensor (current_state == target_state) -> {current_state} == {self.target_state}"
        )
        if current_state == self.target_state:
            return True

        return False

    async def get_current_state(self):
        logger.debug(f"Getting current state for {self.json_data['device_id']}")
        document = await store.get_generated_data(self.json_data["device_id"], 1)
        gen_datetime = arrow.get(document[0]["creation_timestamp"])
        curr_datetime = arrow.now("UTC")

        delta = (curr_datetime - gen_datetime).total_seconds()
        logger.debug(f"Last message from device was received {delta} seconds ago")

        if delta < self.OCCUPANCY_SENSOR_DATA_INTERVAL:
            logger.info(f"{self.json_data['device_id']} is currently occupied")
            return "occupied"
        else:
            logger.info(f"{self.json_data['device_id']} is currently unoccupied")
            return "unoccupied"

    def __eq__(self, other):
        return self.instruction_type == other


class CheckOccupancyFor(BaseInstruction):
    instruction_type = InstructionConstant.OCCUPANCY_FOR
    name = "CHECK_OCCUPANCY_FOR"
    OCCUPANCY_SENSOR_DATA_INTERVAL = (
        2 * 60
    )  # Device sends data every 1 minute, till the device is on
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string"},
            "device_id": {"type": "string"},
            "state": {"type": "string", "enum": ["occupied", "unoccupied"]},
            "for": {"type": "integer", "exclusiveMinimum": 0},
        },
        "required": ["operation", "device_id", "state", "for"],
    }

    def __init__(self, json_data: Dict, rule):
        super(CheckOccupancyFor, self).__init__(json_data, rule)
        self.target_state = json_data["state"].lower()
        self.device_id = json_data["device_id"]
        self.target_state_for = json_data["for"]

    async def evaluate(self, vm_instance):
        current_state, current_state_for = await self.get_current_state_for()
        logger.debug(
            f"{self.device_id} has state {current_state.upper()} for {current_state_for} minutes."
        )
        logger.debug(
            f"Condition requires {self.target_state.upper()} for {self.target_state_for} minutes."
        )
        if current_state.lower() == self.target_state.lower():

            if current_state_for >= self.target_state_for:
                return True
            else:
                # There is some finite time in which this rule
                # can evaluate to True if everything goes well.
                if self.rule.periodic_execution:
                    time_to_next_invocation = (
                        self.target_state_for - current_state_for
                    ) * 60
                    vm_instance.add_rule_for_future_exec(
                        self.rule, time_to_next_invocation
                    )

        # If all other cases return False
        return False

    async def get_current_state_for(self):
        # Fetch the last generated data
        logger.debug(f"Getting current state for {self.json_data['device_id']}")
        latest_document = await store.get_generated_data(self.json_data["device_id"], 1)

        creation_timestamp = latest_document[0]["creation_timestamp"]
        current_dt = datetime.datetime.now(pytz.timezone("UTC"))

        delta = current_dt - creation_timestamp
        for_minutes = delta.total_seconds() / 60
        logger.debug(f"{self.device_id} delta -> {for_minutes}")
        logger.debug(
            f"Comparing {delta.total_seconds()} < {self.OCCUPANCY_SENSOR_DATA_INTERVAL}"
        )

        if delta.total_seconds() < self.OCCUPANCY_SENSOR_DATA_INTERVAL:
            # Room is occupied
            calculated_occupied_time = 0
            # Use a mechanism to calculate for how long the room has been occupied
            # To save the number of documents that have to be looked up to find out
            # for how long a room has been occupied. We can only look for target_state_for + 1
            # documents
            logger.info(f"{self.device_id} currently detects the area is occupied")

            generated_data = (
                store.store.collection("devices")
                .document(self.device_id)
                .collection("generatedData")
                .order_by(
                    "creation_timestamp", direction=store.firestore.Query.DESCENDING
                )
                .limit(math.ceil(self.target_state_for) + 1)
                .stream()
            )

            prev_document = latest_document[0]
            for doc in generated_data:
                # Each document should have a time difference of at max OCCUPANCY_SENSOR_INTERVAL minutes
                logger.info(f"Fetched document {doc.id}")
                doc_dict = doc.to_dict()
                prev_document_dt = prev_document["creation_timestamp"]
                next_document_dt = doc_dict["creation_timestamp"]

                time_diff = (prev_document_dt - next_document_dt).total_seconds()
                logger.debug(
                    f"Time difference between previous and present document({doc.id}) is {time_diff} seconds or {time_diff/60} minutes."
                )
                if time_diff <= self.OCCUPANCY_SENSOR_DATA_INTERVAL:
                    # If the difference between adjacent
                    logger.debug("Added 1 minute to calculated_occupied_time")
                    calculated_occupied_time += 1

                else:
                    logger.debug("Exiting out of for loop.")
                    break

                prev_document = doc_dict
            logger.debug(f"calculated occupied time is: {calculated_occupied_time}")
            return "occupied", calculated_occupied_time

        else:
            # Room is unoccupied
            return "unoccupied", for_minutes

    def __eq__(self, other):
        return self.instruction_type == other
