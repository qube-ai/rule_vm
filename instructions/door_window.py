from .base import BaseInstruction
from .base import InstructionConstant
from typing import Dict
from loguru import logger
import store


class DoorWindowState(BaseInstruction):
    instruction_type = InstructionConstant.DOOR_WINDOW_STATE
    name = "DW_STATE"

    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string"},
            "device_id": {"type": "string"},
            "state": {"type": "string", "enum": ["open", "close"]}
        },
        "required": ["operation", "device_id", "state"]
    }

    def __init__(self, json_data: Dict, rule):
        super(DoorWindowState, self).__init__(json_data, rule)
        self.target_state = self.json_data["state"].lower()

    async def evaluate(self):
        current_state = await self.get_current_state()
        logger.debug(f"Comparing door window state {current_state} == {self.target_state}")
        if current_state == self.target_state:
            return True

        return False

    async def get_current_state(self):
        logger.debug(f"Getting current state for {self.json_data['device_id']}")
        document = await store.get_generated_data(self.json_data["device_id"], 1)
        state = document[0]["status"].lower()
        logger.debug(f"Current state of is {state}")
        return state

    def __eq__(self, other):
        return self.instruction_type == other


class DoorWindowStateFor(BaseInstruction):
    instruction_type = InstructionConstant.DOOR_WINDOW_STATE_FOR
    name = "DW_STATE_FOR"
