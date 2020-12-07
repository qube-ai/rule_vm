from typing import Dict

from loguru import logger

import store
import datetime
import pytz
from .base import BaseInstruction
from .base import InstructionConstant


class DoorWindowState(BaseInstruction):
    instruction_type = InstructionConstant.DW_STATE
    name = "DW_STATE"

    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string", "enum": ["dw_state"]},
            "device_id": {"type": "string"},
            "state": {"type": "string", "enum": ["open", "close"]},
        },
        "required": ["operation", "device_id", "state"],
    }

    def __init__(self, json_data: Dict, rule):
        super(DoorWindowState, self).__init__(json_data, rule)
        self.target_state = self.json_data["state"].lower()
        self.device_id = self.json_data["device_id"]

    async def evaluate(self, vm_instance):
        current_state = await self.get_current_state()
        logger.debug(
            f"Comparing door window state {current_state} == {self.target_state}"
        )
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
    instruction_type = InstructionConstant.DW_STATE_FOR
    name = "DW_STATE_FOR"
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string", "enum": ["dw_state_for"]},
            "device_id": {"type": "string"},
            "state": {"type": "string", "enum": ["open", "close"]},
            "for": {"type": "integer", "exclusiveMinimum": 0},
        },
        "required": ["operation", "device_id", "state", "for"],
    }

    def __init__(self, json_data: Dict, rule):
        super(DoorWindowStateFor, self).__init__(json_data, rule)
        self.target_state = json_data["state"].lower()
        self.device_id = json_data["device_id"]
        self.target_state_for = json_data["for"]

    async def evaluate(self, vm_instance):
        current_state, current_state_for = await self.get_current_state_for()

        logger.debug(
            f"The door is {current_state.upper()} for {current_state_for:.2f} minutes."
        )
        logger.debug(
            f"Condition requires door to be {self.target_state.upper()} for {self.target_state_for} minutes."
        )
        if current_state == self.target_state:

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

        # Current state and the target state are not the same
        # So just simply exit. Whenever the state of the device
        # will change in the future. Corresponding rules will
        # automatically be invoked.
        return False

    async def get_current_state_for(self):
        logger.debug(f"Getting current state for {self.json_data['device_id']}")
        document = await store.get_generated_data(self.json_data["device_id"], 1)

        creation_timestamp = document[0]["creation_timestamp"]
        current_dt = datetime.datetime.now(pytz.timezone("UTC"))

        delta = current_dt - creation_timestamp
        for_minutes = delta.total_seconds() / 60
        state = document[0]["status"].lower()
        logger.debug(
            f"Current state of {self.device_id} is {state} for {for_minutes:.2f} minutes"
        )
        return state, for_minutes

    def __eq__(self, other):
        return self.instruction_type == other
