import logging
import queue
import threading
import instructions

import trio

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
logging.info("root: Logging setup complete")


class VM:
    TASK_QUEUE_BUFFER_SIZE = 10

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
