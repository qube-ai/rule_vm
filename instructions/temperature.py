from .base import BaseInstruction
from .base import InstructionConstant


class CheckTemperature(BaseInstruction):
    instruction_type = InstructionConstant.TEMPERATURE
    name = "TEMPERATURE"


class CheckTemperatureFor(BaseInstruction):
    instruction_type = InstructionConstant.TEMPERATURE_FOR
    name = "TEMPERATURE_FOR"
