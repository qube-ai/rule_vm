from .base import InstructionConstant
from .logical import LogicalOr
from .logical import LogicalAnd
from .time import AtTime
from .time import AtTimeWithOccurrence
from .relay import IsRelayState
from .relay import IsRelayStateFor
from .temperature import CheckTemperature
from .temperature import CheckTemperatureFor
from .door_window import DoorWindowState
from .door_window import DoorWindowStateFor
from .occupancy import CheckOccupancy
from .occupancy import CheckOccupancyFor
from .energy import EnergyMeter

INSTRUCTION_LUT = {
    InstructionConstant.LOGICAL_OR.value: LogicalOr,
    InstructionConstant.LOGICAL_AND.value: LogicalAnd,
    InstructionConstant.AT_TIME.value: AtTime,
    InstructionConstant.AT_TIME_WITH_OCCURRENCE.value: AtTimeWithOccurrence,
    InstructionConstant.RELAY_STATE.value: IsRelayState,
    InstructionConstant.RELAY_STATE_FOR.value: IsRelayStateFor,
    InstructionConstant.TEMPERATURE.value: CheckTemperature,
    InstructionConstant.TEMPERATURE_FOR.value: CheckTemperatureFor,
    InstructionConstant.DW_STATE.value: DoorWindowState,
    InstructionConstant.DW_STATE_FOR.value: DoorWindowStateFor,
    InstructionConstant.OCCUPANCY.value: CheckOccupancy,
    InstructionConstant.OCCUPANCY_FOR.value: CheckOccupancyFor,
    InstructionConstant.ENERGY_METER.value: EnergyMeter,
}
