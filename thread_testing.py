import logging
import queue
import threading

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

    async def __executor(self, task):
        logging.info(f"__executor: Started executing: {task}")
        await trio.sleep(10)
        logging.info(f"__executor: Task {task} complete!")

    def execute_rule(self, rule):
        self.task_queue.put(rule)

    def stop(self):
        logging.info("Shutting down VM thread. Awaiting join.")
        self.run_vm_thread = False
        self.vm_thread.join()
