from .base import BaseInstruction
from .base import InstructionConstant


class CheckTemperature(BaseInstruction):
    instruction_type = InstructionConstant.CHECK_TEMPERATURE
    name = "TEMPERATURE"


class CheckTemperatureFor(BaseInstruction):
    instruction_type = InstructionConstant.CHECK_TEMPERATURE_FOR
    name = "TEMPERATURE_FOR"
