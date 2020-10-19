import logging
import queue
import threading
import instructions
import json

import trio
import rule
from parse import compile as pc

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
logging.info("root: Logging setup complete")


class VM:
    TASK_QUEUE_BUFFER_SIZE = 10

    # Used for parsing rules in string format
    instructions_pattern = [
        pc("AT_TIME {time}"),
        pc("AT_TIME_WITH_OCCURRENCE {time} {occurrence:d}"),
        pc("AND"),
        pc("OR"),
        pc("DW_STATE {device_id} {state}"),
        pc("DW_STATE_FOR {device_id} {state} {for:d}"),
        pc("OCCUPANCY_STATE {device_id} {state}"),
        pc("OCCUPANCY_STATE_FOR {device_id} {state} {for:d}"),
        pc("RELAY_STATE {device_id} {relay_index:d} {state}"),
        pc("RELAY_STATE_FOR {device_id} {relay_index:d} {state} {for:d}"),
        pc("TEMPERATURE {device_id} {comparison_op} {value:f}"),
        pc("TEMPERATURE_FOR {device_id} {comparison_op} {value:f} {for:d}"),
        pc("ENERGY_METER {device_id} VOLTAGE {comparison_op} {value:f}"),
        pc("ENERGY_METER {device_id} CURRENT {comparison_op} {value:f}"),
        pc("ENERGY_METER {device_id} REAL_POWER {comparison_op} {value:f}"),
        pc("ENERGY_METER {device_id} APPARENT_POWER {comparison_op} {value:f}"),
        pc("ENERGY_METER {device_id} POWER_FACTOR {comparison_op} {value:f}"),
        pc("ENERGY_METER {device_id} FREQUENCY {comparison_op} {value:f}"),
    ]

    def __init__(self):
        self.run_vm_thread = True
        self.task_queue = queue.Queue(self.TASK_QUEUE_BUFFER_SIZE)
        self.vm_thread = threading.Thread(target=lambda: trio.run(self.__starter))
        self.vm_thread.start()

    async def __starter(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.task_spawner, nursery)
            # Started anything else required for the VM
            logging.info("__starter: Started task_spawner")

    async def task_spawner(self, nursery):
        while self.run_vm_thread:
            if not self.task_queue.empty():
                task = self.task_queue.get_nowait()
                nursery.start_soon(self.__executor, task)
                logging.info(f"task_spawner: Spawned a new task: {task}")

            await trio.sleep(0)

    async def __executor(self, rule):
        """Evaluates a rule using a stack."""

        # Stack used for evaluating a rule
        stack = []

        # Evaluate each expression
        for instruction in rule.instruction_stream:

            if instructions.InstructionConstant.LOGICAL_AND == instruction:
                # Pop values from stack
                op1 = stack.pop()
                op2 = stack.pop()

                # Evaluate op1
                op1_value = None
                if isinstance(op1, instructions.BaseInstruction):
                    # If it's an instruction, evaluate it
                    op1_value = await op1.evaluate()
                else:
                    # It's probably only a bool value
                    op1_value = op1

                # Evaluate op2
                op2_value = None
                if isinstance(op2, instructions.BaseInstruction):
                    # If it's an instruction evaluate it
                    op2_value = await op2.evaluate()
                else:
                    # It's probably only a bool value
                    op2_value = op2

                # Perform logical AND and push it to stack
                stack.append(op1_value and op2_value)

            elif instructions.InstructionConstant.LOGICAL_OR == instruction:
                # Pop values from stack
                op1 = stack.pop()
                op2 = stack.pop()

                # Evaluate op1
                op1_value = None
                if isinstance(op1, instructions.BaseInstruction):
                    # If it's an instruction, evaluate it
                    op1_value = await op1.evaluate()
                else:
                    # It's probably only a bool value
                    op1_value = op1

                # Evaluate op2
                op2_value = None
                if isinstance(op2, instructions.BaseInstruction):
                    # If it's an instruction evaluate it
                    op2_value = await op2.evaluate()
                else:
                    # It's probably only a bool value
                    op2_value = op2

                # Perform logical AND and push it to stack
                stack.append(op1_value or op2_value)

            elif instructions.InstructionConstant.AT_TIME == instruction:
                stack.append(instruction)

            elif instructions.InstructionConstant.AT_TIME_WITH_OCCURENCE == instruction:
                stack.append(instruction)

            elif instructions.InstructionConstant.IS_RELAY_STATE == instruction:
                stack.append(instruction)

            elif instructions.InstructionConstant.IS_RELAY_STATE_FOR == instruction:
                stack.append(instruction)

            elif instructions.InstructionConstant.CHECK_TEMPERATURE == instruction:
                stack.append(instruction)

            elif instructions.InstructionConstant.CHECK_TEMPERATURE_FOR == instruction:
                stack.append(instruction)

            elif instructions.InstructionConstant.CHECK_OCCUPANCY == instruction:
                stack.append(instruction)

            elif instructions.InstructionConstant.CHECK_OCCUPANCY_FOR == instruction:
                stack.append(instruction)

        # The last value would always be a bool, True or False
        return stack.pop()

    def execute_rule(self, rule):
        self.task_queue.put(rule)

    def stop(self):
        logging.info("Shutting down VM thread. Awaiting join.")
        self.run_vm_thread = False
        self.vm_thread.join()

    @staticmethod
    def parse_from_string(rule_script: str) -> rule.Rule:
        rule_lines = rule_script.split("\n")

        parsed_json = []

        for line in rule_lines:
            clean_line = line.strip().lower()

            if clean_line.startswith("and"):
                parsed_json.append({"operation": "logical_and"})

            elif clean_line.startswith("or"):
                parsed_json.append({"operation": "logical_or"})

            else:
                for pattern in VM.instructions_pattern:
                    match = pattern.parse(clean_line)
                    if match:
                        # If match is found, move to the next line
                        operation = clean_line.split(" ")[0]
                        json_instruction = {**match.named}

                        # Check for each instruction
                        if operation == "at_time":
                            json_instruction["operation"] = "at_time"

                        elif operation == "at_time_with_occurrence":
                            json_instruction["operation"] = "at_time_with_occurrence"

                        elif operation == "energy_meter":
                            json_instruction["operation"] = "energy_meter"

                        elif operation == "dw_state":
                            json_instruction["operation"] = "dw_state"

                        elif operation == "dw_state_for":
                            json_instruction["operation"] = "dw_state_for"

                        elif operation == "occupancy_state":
                            json_instruction["operation"] = "occupancy_state"

                        elif operation == "occupancy_state_for":
                            json_instruction["operation"] = "occupancy_state_for"

                        elif operation == "relay_state":
                            json_instruction["operation"] = "relay_state"

                        elif operation == "relay_state_for":
                            json_instruction["operation"] = "relay_state_for"

                        elif operation == "temperature":
                            json_instruction["operation"] = "temperature"

                        elif operation == "temperature_for":
                            json_instruction["operation"] = "temperature_for"

                        elif operation == "energy_meter":
                            json_instruction["operation"] = "energy_meter"

                        else:
                            # This will probably never get executed
                            # The reason is, we'll never have a match with a string that
                            # we don't recognize
                            print(f"Unknown operation: {operation}")

                        parsed_json.append(json_instruction)

                    else:
                        # If match isn't found, move to the next pattern
                        continue

        return VM.parse_from_dict(parsed_json)

    @staticmethod
    def parse_from_json(json_data: str) -> rule.Rule:
        try:
            parsed_json = json.loads(json_data)
            return VM.parse_from_dict(parsed_json)
        except json.JSONDecodeError:
            print("Unable to decode JSON")

    @staticmethod
    def parse_from_dict(rule_dict) -> rule.Rule:
        return rule.Rule(rule_dict)
