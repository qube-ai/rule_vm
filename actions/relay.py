from .base import BaseAction
from .base import ActionConstant
from typing import Dict
import store
from loguru import logger
import trio


class ChangeRelayState(BaseAction):
    action_type = ActionConstant.CHANGE_RELAY_STATE

    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "type": {"type": "string", "enum": ["change_relay_state"]},
            "device_id": {"type": "string"},
            "relay_index": {"type": "number"},
            "state": {"type": "number"},
        },
        "required": ["type", "device_id", "relay_index", "state"],
    }

    def __init__(self, action_data: Dict):
        super(ChangeRelayState, self).__init__(action_data)
        self.device_id = action_data["device_id"]
        self.relay_index = action_data["relay_index"]
        self.state = action_data["state"]

    async def perform(self):
        finalRelayStatus = []

        doc  = await store.get_document("devices", self.device_id)
        if(doc):
            doc_id = doc.id
            document = doc.to_dict()
            if(self.device_id.startswith('SW2-')):
                finalRelayStatus = [document["relay_state"]].copy()
                finalRelayStatus[self.relay_index] = self.state
            else:
                finalRelayStatus = document["relayStatus"].copy()
                finalRelayStatus[self.relay_index] = self.state
        else:
            logger.error(f"Could not find any device with the given deviceId.")

        final_data = {"relay_state" : finalRelayStatus[0], "insertedBy" : "dashboard"}
        await store.update_document("devices", self.device_id, final_data)
        def f():
            logger.info(
                f"Succesfully updated device state. States updated -> {final_data}. Path is -> devices/{self.device_id}"
            )

        try:
            await trio.to_thread.run_sync(f)
        except Exception as e:
            logger.error(f"Unable to update the device state. Error: {e}")