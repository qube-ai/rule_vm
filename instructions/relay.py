from .base import BaseInstruction
from .base import InstructionConstant
from .base import InstructionException
import store


class IsRelayState(BaseInstruction):

    instruction_type = InstructionConstant.IS_RELAY_STATE

    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string"},
            "device_id": {"type": "string"},
            "relay_index": {"type": "integer", "maximum": 64, "minimum": 0},
            "state": {"type": "integer", "minimum":0, "maximum": 1}
        },
        "required": ["operation", "device_id", "relay_index", "state"]
    }
    
    def __init__(self, json_data: str):
        
        super(IsRelayState, self).__init__(json_data)

        self.device_document = None

        self.device_id = self.parsed_data["device_id"]
        self.relay_index = self.parsed_data["relay_index"]
        self.state_to_check = self.parsed_data["state"]

        # Check whether the relay_index value is less than the number of relays on the device itself
        device_doc = store.get_device_document_sync(self.device_id)
        relays = device_doc["relay"]

        # If the relay_index is greater than the actual no. of relays, throw an error
        if self.relay_index >= len(relays):
            raise InstructionException(f"relay_index:{self.relay_index} specified is greater than the number of relays:{len(relays)}")

    async def evaluate(self):
        # Get the device document from DB
        self.device_document = await store.get_device_document(self.device_id)
        relay_status = self.device_document["relayStatus"]

        return relay_status[self.relay_index] == self.state_to_check

    def __eq__(self, other):
        return self.instruction_type == other


class IsRelayStateFor(BaseInstruction):
    pass