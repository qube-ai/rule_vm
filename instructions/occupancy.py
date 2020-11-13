from typing import Dict

import arrow
from loguru import logger

import store
from .base import BaseInstruction
from .base import InstructionConstant


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
