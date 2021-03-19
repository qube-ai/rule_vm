from typing import Dict

import arrow
from loguru import logger

import store
from .base import BaseInstruction
from .base import InstructionConstant


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

    async def evaluate(self, vm_instance):
        # Find the difference between target time and current time in UTC

        # Convert this to parsed time string
        # Expected time, 09:42:32+05:30
        temp = arrow.get(self.time_string, "HH:mm:ssZZ")
        self.current_time = arrow.now(temp.tzinfo)
        self.target_time = temp.shift(
            years=self.current_time.year - 1,
            months=self.current_time.month - 1,
            days=self.current_time.day - 1,
        )

        # Add rule for future execution
        if self.rule.periodic_execution:
            time_to_next_invocation = self.time_to_next_evaluation()
            vm_instance.add_rule_for_future_exec(self.rule, time_to_next_invocation)

        logger.debug(
            f"Evaluating {self.instruction_type}. Current time({self.current_time}) and Target time({self.target_time})"
        )
        if self.current_time > self.target_time:
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

    def time_to_next_evaluation(self):
        if self.current_time > self.target_time:
            new_target_time = self.target_time.shift(days=1)
            delta = new_target_time - self.current_time
            return delta.seconds
        else:
            delta = self.target_time - self.current_time
            return delta.seconds

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

    async def decrement_occurrence(self):
        if self.rule.id != "immediate":
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

        else:
            self.occurrence -= 1
            logger.debug(
                f"Decremented occurrence count for {self.rule} to {self.occurrence}"
            )

    async def evaluate(self, vm_instance):
        # Call to super automatically evaluates the next time for evaluation
        # To turn it off, disable periodic execution and then make the
        # call to super class
        self.rule.set_periodic_execution(False)
        # gives true or false if it is the time to execute
        current_exec_eval = await super().evaluate(vm_instance)
        self.rule.set_periodic_execution(True)

        # Scheduling the rule to be evaluated in future
        time_to_next_eval = self.time_to_next_evaluation()
        vm_instance.add_rule_for_future_exec(self.rule, time_to_next_eval)


        if current_exec_eval and self.occurrence > 0:
            await self.decrement_occurrence()
            return True
        else:
            return False

    def __eq__(self, other):
        return self.instruction_type == other
