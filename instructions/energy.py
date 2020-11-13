from .base import BaseInstruction
from .base import InstructionConstant
from typing import Dict
import store
from loguru import logger


class EnergyMeter(BaseInstruction):
    instruction_type = InstructionConstant.ENERGY_METER
    name = "ENERGY_METER"
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema",
        "type": "object",
        "properties": {
            "operation": {"type": "string", "enum": ["energy_meter"]},
            "device_id": {"type": "string"},
            "variable": {
                "type": "string",
                "enum": [
                    "voltage",
                    "current",
                    "real_power",
                    "apparent_power",
                    "power_factor",
                    "frequency",
                    "energy",
                ],
            },
            "comparison_op": {"type": "string", "enum": ["=", ">", "<"]},
            "value": {"type": "number"},
        },
        "required": ["operation", "device_id", "variable", "comparison_op", "value"],
    }

    def __init__(self, json_data: Dict, rule):
        super(EnergyMeter, self).__init__(json_data, rule)
        self.device_id = self.json_data["device_id"]
        self.variable = self.json_data["variable"]
        self.value = self.json_data["value"]
        self.comparison_op = self.json_data["comparison_op"]

    async def evaluate(self, vm_instance):
        doc = await store.get_device_document(self.device_id)
        document = doc.to_dict()

        if self.comparison_op == "=":
            logger.debug(
                f"{self.rule}: Evaluating (current_{self.variable} == target_{self.value}) -> {document[self.variable]} = {self.value}"
            )
            return document[self.variable] == self.value

        elif self.comparison_op == ">":
            logger.debug(
                f"{self.rule}: Evaluating (current_{self.variable} > target_{self.value}) -> {document[self.variable]} > {self.value}"
            )
            return document[self.variable] > self.value

        elif self.comparison_op == "<":
            logger.debug(
                f"{self.rule}: Evaluating (current_{self.variable} < target_{self.value}) -> {document[self.variable]} < {self.value}"
            )
            return document[self.variable] < self.value
