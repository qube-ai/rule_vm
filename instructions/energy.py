from .base import BaseInstruction
from .base import InstructionConstant


class EnergyMeter(BaseInstruction):
    instruction_type = InstructionConstant.ENERGY_METER
    name = "ENERGY_METER"
