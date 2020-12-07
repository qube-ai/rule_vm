from .base import BaseInstruction
from .base import InstructionConstant
from loguru import logger
import store
from typing import Dict
import pytz
import datetime


class IsRelayState(BaseInstruction):

    instruction_type = InstructionConstant.RELAY_STATE
    name = "RELAY_STATE"
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string", "enum": ["relay_state"]},
            "device_id": {"type": "string"},
            "relay_index": {"type": "integer", "maximum": 64, "minimum": 0},
            "state": {"type": "integer", "minimum": 0, "maximum": 1},
        },
        "required": ["operation", "device_id", "relay_index", "state"],
    }

    def __init__(self, json_data: Dict, rule):
        super(IsRelayState, self).__init__(json_data, rule)
        self.device_id = self.json_data["device_id"]
        self.relay_index = int(self.json_data["relay_index"])
        self.target_state = self.json_data["state"]

    async def evaluate(self, vm_instance):
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


class IsRelayStateFor(BaseInstruction):

    instruction_type = InstructionConstant.RELAY_STATE_FOR
    SWITCH_STATE_UPDATE_INTERVAL = (
        5 * 60
    )  # How often the device will send state values to server
    name = "RELAY_STATE_FOR"
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string", "enum": ["relay_state_for"]},
            "device_id": {"type": "string"},
            "relay_index": {"type": "integer", "maximum": 64, "minimum": 0},
            "state": {"type": "integer", "minimum": 0, "maximum": 1},
            "for": {"type": "integer", "exclusiveMinimum": 0},
        },
        "required": ["operation", "device_id", "relay_index", "state", "for"],
    }

    def __init__(self, json_data: Dict, rule):
        json_data["state"] = int(json_data["state"])
        super(IsRelayStateFor, self).__init__(json_data, rule)
        self.target_state = json_data["state"]
        self.device_id = json_data["device_id"]
        self.relay_index = int(json_data["relay_index"])
        self.target_state_for = json_data["for"]

    async def evaluate(self, vm_instance):
        current_state, current_state_for = await self.get_current_state_for()
        logger.debug(
            f"{self.device_id} has state {current_state} @ relay index {self.relay_index} for {current_state_for} minutes."
        )
        logger.debug(
            f"Conditions required are state {self.target_state} @ relay index {self.relay_index} for {self.target_state_for} minutes."
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
        # In all other cases, return False
        return False

    async def get_current_state_for(self):
        logger.debug(f"Getting current state for {self.device_id}")
        latest_document = await store.get_generated_data(self.device_id, 1)

        # Check whether the latest document has the given state for the relay index
        parsed_latest_document = latest_document[0]

        # Index for the relays are like `relay1`, `relay2`, `relay3` and `relay3`
        relay_key = f"relay{self.relay_index + 1}"
        current_state = parsed_latest_document[relay_key]

        # If the current_state and target_state match only then go ahead and calculate the time
        if current_state == self.target_state:
            # Calculate time diff between latest document and current time
            creation_dt = latest_document[0]["creation_timestamp"]
            current_dt = datetime.datetime.now(pytz.timezone("UTC"))
            delta = current_dt - creation_dt
            for_minutes = delta.total_seconds() / 60

            # If the current time difference already satisfies the time condition for rule evaluation
            # Don't dig in more documents to find the exact time, simply return the value
            # and the condition will automatically evaluate
            if for_minutes >= self.target_state_for:
                logger.debug("Current_state time is more than sufficient. Returning.")
                return current_state, for_minutes

            # The time diff is not sufficient to evaluate the instruction and return True. We need to
            # dig a little further to find the exact time for how long the relay has been in a
            # particular state.
            else:
                logger.debug(
                    "We need to look at other generatedData to find the actual current_state time."
                )
                max_documents_to_fetch = int(
                    (self.target_state_for / (self.SWITCH_STATE_UPDATE_INTERVAL / 60))
                    + 1
                )
                logger.debug(f"We'll fetch at max {max_documents_to_fetch} documents.")
                generated_data = (
                    store.store.collection("devices")
                    .document(self.device_id)
                    .collection("generatedData")
                    .order_by(
                        "creation_timestamp", direction=store.firestore.Query.DESCENDING
                    )
                    .limit(max_documents_to_fetch)
                    .stream()
                )

                required_state_earliest_dt = creation_dt
                for doc in generated_data:
                    logger.debug(f"Fetched {doc.id} document")
                    doc_data = doc.to_dict()
                    if doc_data[relay_key] == self.target_state:
                        required_state_earliest_dt = doc_data["creation_timestamp"]
                        # Compute the current time diff and see if it exceeds target_state_for time
                        diff = (current_dt - required_state_earliest_dt).total_seconds()
                        logger.debug(
                            f"Current time difference is {diff/60:.2f} and required is {self.target_state_for}"
                        )
                        if diff >= (self.target_state_for * 60):
                            logger.debug(
                                "Condition can now be satisfied. We'll NOT look back at anymore documents."
                            )
                            break
                    else:
                        logger.debug(
                            "Current documents relay index state did not match with target state. Exiting."
                        )
                        break

                # Now we have the earliest timestamp with the target_state
                # Calculate the time difference and return
                current_dt = datetime.datetime.now(pytz.timezone("UTC"))
                delta = current_dt - required_state_earliest_dt
                new_for_minutes = delta.total_seconds() / 60

                return current_state, new_for_minutes

        # If the current_state and target_state don't match. Simply exit.
        else:
            # current_state_for_minutes value does not really matter here as the states are not event the same
            # Simply return 0 as this will not be used else where
            return current_state, 0

    def __eq__(self, other):
        return self.instruction_type == other
