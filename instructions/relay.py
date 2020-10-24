from .base import BaseInstruction
from .base import InstructionConstant
from loguru import logger
import store
from typing import Dict


class IsRelayState(BaseInstruction):

    instruction_type = InstructionConstant.RELAY_STATE
    name = "RELAY_STATE"
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string"},
            "device_id": {"type": "string"},
            "relay_index": {"type": "integer", "maximum": 64, "minimum": 0},
            "state": {"type": "integer", "minimum": 0, "maximum": 1},
        },
        "required": ["operation", "device_id", "relay_index", "state"],
    }

    def __init__(self, json_data: Dict, rule):
        super(IsRelayState, self).__init__(json_data, rule)
        self.device_id = self.json_data["device_id"]
        self.relay_index = self.json_data["relay_index"]
        self.target_state = self.json_data["state"]

    async def evaluate(self):
        current_state = await self.get_current_state(self.relay_index)
        logger.debug(
            f"{self.rule}: Evaluating relay state(current_state == target_state) -> {current_state} == {self.target_state}"
        )
        if current_state == self.target_state:
            return True

        return False

    async def get_current_state(self, relay_index):
        document = await store.get_device_document(self.device_id)
        relay_status = document.to_dict()["relayStatus"]
        return relay_status[relay_index]

    def __eq__(self, other):
        return self.instruction_type == other


class IsRelayStateFor(IsRelayState):

    instruction_type = InstructionConstant.RELAY_STATE_FOR
    name = "RELAY_STATE_FOR"
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string"},
            "device_id": {"type": "string"},
            "relay_index": {"type": "integer", "maximum": 64, "minimum": 0},
            "state": {"type": "integer", "minimum": 0, "maximum": 1},
            "for": {"type": "integer"},
        },
        "required": ["operation", "device_id", "relay_index", "state"],
    }

    def __init__(self, json_data: Dict):
        super(IsRelayStateFor, self).__init__(json_data)
        self.for_duration = self.parsed_data["for"]

        # fetch device document
        # TODO check for how long should the state should persist for
        # TODO get previous generatedData of the device
        # TODO Go through the generated data to find when the relay state changed.
        # TODO compute the time difference between the most recent change and current state

    async def evaluate(self):
        state_matched = await super(IsRelayStateFor, self).evaluate()

        if state_matched:
            # TODO check for how long we have been in this state
            # store.get_generated_data()
            pass
        else:
            # the specified state and current state do not match
            return False
