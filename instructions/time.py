from typing import Dict

import arrow

import store
from .base import BaseInstruction
from .base import InstructionConstant
from loguru import logger


# TODO need to create a Rule Statistics to prevent the execution of same rule multiple times.
#  This can happen in the case of time based instructions.
# If multiple tasks are started, all of them will wait for the target time
#  and then all of them will return True, which shouldn't happen.
# Think of a mechanism to prevent this from happening.


class AtTime(BaseInstruction):
    instruction_type = InstructionConstant.AT_TIME
    name = "AT_TIME"

    # TODO `time` format accepts values without timezone. Fix this by making a custom validator.
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string"},
            "time": {"type": "string", "format": "time"}
        },
        "required": ["operation", "time"]
    }

    def __init__(self, json_data: Dict):
        self.json_data = json_data
        self.time_string = self.json_data["time"]
        self.target_time = None

    async def evaluate(self):
        # Find the difference between target time and current time in UTC

        # Convert this to parsed time string
        # Expected time, 09:42:32+05:30
        temp = arrow.get(self.time_string, "HH:mm:ssZZ")
        current_time = arrow.now(temp.tzinfo)
        self.target_time = temp.shift(years=current_time.year - 1, months=current_time.month - 1,
                                      days=current_time.day - 1)

        # delta = 0
        logger.debug(f"Evaluating {self.instruction_type}. Current time({current_time}) and Target time({self.target_time})")
        if current_time > self.target_time:
            # delta = current_time - self.target_time
            # Since we are in the same day, if current time is greater than
            # target time, we should execute the rule
            return True
        else:
            # delta = self.target_time - current_time
            # When we look at current_time and self.target_time
            # We are in the same day. So if current_time is lagging
            # behind, return false.
            return False

        # Sleep for that interval of time
        # print(f"Sleeping for {delta.seconds} seconds...")
        # await trio.sleep(delta.seconds)

    def __eq__(self, other):
        return self.instruction_type == other


class AtTimeWithOccurrence(AtTime):
    instruction_type = InstructionConstant.AT_TIME_WITH_OCCURENCE
    name = "AT_TIME_WITH_OCCURRENCE"
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string"},
            "time": {"type": "string", "format": "time"},
            "occurence": {"type": "integer", "exclusiveMinimum": 0}
        },
        "required": ["operation", "time"]
    }

    def __init__(self, json_data: Dict):
        super(AtTimeWithOccurrence, self).__init__(json_data)
        self.occurrence: int = self.json_data["occurrence"]

    async def evaluate(self):
        document = await store.get_document("collection", "document")
        self.occurrence = document["occurrence"]

        if self.occurrence > 0:
            # Decrement the occurrence and update the DB
            self.occurrence -= 1
            # store.update_document()
            # TODO Update the DB with the updated value

            return await super().evaluate()

        return False

    def __eq__(self, other):
        return self.instruction_type == other
