from typing import Dict

import arrow
from loguru import logger

import store
from .base import BaseInstruction
from .base import InstructionConstant


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
            "time": {"type": "string", "format": "time"},
        },
        "required": ["operation", "time"],
    }

    def __init__(self, json_data: Dict, rule):
        super(AtTime, self).__init__(json_data, rule)
        self.time_string = self.json_data["time"]
        self.target_time = None

    async def evaluate(self):
        # Find the difference between target time and current time in UTC

        # Convert this to parsed time string
        # Expected time, 09:42:32+05:30
        temp = arrow.get(self.time_string, "HH:mm:ssZZ")
        current_time = arrow.now(temp.tzinfo)
        self.target_time = temp.shift(
            years=current_time.year - 1,
            months=current_time.month - 1,
            days=current_time.day - 1,
        )

        # delta = 0
        logger.debug(
            f"Evaluating {self.instruction_type}. Current time({current_time}) and Target time({self.target_time})"
        )
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
    instruction_type = InstructionConstant.AT_TIME_WITH_OCCURRENCE
    name = "AT_TIME_WITH_OCCURRENCE"
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string"},
            "time": {"type": "string", "format": "time"},
            "occurence": {"type": "integer", "exclusiveMinimum": 0},
        },
        "required": ["operation", "time"],
    }

    def __init__(self, json_data: Dict, rule):
        super(AtTimeWithOccurrence, self).__init__(json_data, rule)
        self.occurrence: int = self.json_data["occurrence"]

    async def evaluate(self):
        is_true = await super().evaluate()

        if is_true and self.occurrence > 0:
            rule_doc = await self.rule.get_rule_document()
            rule_doc_dict = rule_doc.to_dict()
            for cond in rule_doc_dict["conditions"]:
                if (
                    cond["occurrence"] == self.occurrence
                    and cond["operation"].lower() == self.json_data["operation"].lower()
                    and cond["time"] == self.json_data["time"]
                ):
                    cond["occurrence"] -= 1
                    self.occurrence -= 1
                    logger.debug(
                        f"Decremented occurrence count for {self.rule} to {self.occurrence}"
                    )

            # Update the document finally
            await store.update_document("rules", rule_doc.id, rule_doc_dict)
            logger.debug(
                f"Updated Firestore with new occurrence value: {self.occurrence}"
            )

        return False

    def __eq__(self, other):
        return self.instruction_type == other
