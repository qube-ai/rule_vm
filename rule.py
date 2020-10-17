from typing import List
import json
import instructions

class RuleParsingException(Exception):
    pass

class InvalidInstructionException(RuleParsingException):
    pass

class Rule:
    '''Rule is a collection of Conditions(Instructions) chained together.'''

    # Look up table for instruction decoding
    instruction_lut: List = [
        "at_time": instructions.AtTime,
        "at_time_for_x_occurence": instructions.AtTimeWithOccurence,
        "is_relay_state": instructions.IsRelayState,
        "is_relay_state_for": instructions.IsRelayStateFor,
        "check_temperature": instructions.CheckTemperature,
        "check_temperature_for": instructions.CheckTemperatureFor,
        "door_window_state": instructions.DoorWindowState,
        "door_window_state_for": instructions.DoorWindowStateFor,
        "check_occupancy_for": instructions.CheckOccupancyFor,
        "check_occupancy": instructions.CheckOccupancy,
    ]


    def __init__(self, json_data: str):
        '''Converts JSON data into rule object'''

        self.instruction_stream: List = []

        # Parse JSON data
        try:
            rule = json.loads(json_data)

            # Each rule has a bunch of instructions, Parse the instructions
            # Add add them to instruction_stream instance variable
            ### Parse individual instructions ###
            for ins_data in rule:
                if ins_data["operation"] in Rule.instruction_lut:
                    Instruction = Rule.instruction_lut[ins_data["operation"]]
                    self.instruction_stream.append(Instruction)
                
                else:
                    raise InvalidInstructionException(f"Unknown instruction: {ins_data['operation']}")

        except json.JSONDecodeError:
            print("Unable to decode JSON")
            raise RuleParsingException("Unable to decode JSON")
        
        return self
