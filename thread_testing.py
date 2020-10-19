import logging
import threading
import trio
import queue

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
logging.info("root: Logging setup complete")


class Tracer(trio.abc.Instrument):
    def before_run(self):
        print("!!! run started")

    def _print_with_task(self, msg, task):
        # repr(task) is perhaps more useful than task.name in general,
        # but in context of a tutorial the extra noise is unhelpful.
        print("{}: {}".format(msg, task.name))

    def task_spawned(self, task):
        self._print_with_task("### new task spawned", task)

    def task_scheduled(self, task):
        self._print_with_task("### task scheduled", task)

    def before_task_step(self, task):
        self._print_with_task(">>> about to run one step of task", task)

    def after_task_step(self, task):
        self._print_with_task("<<< task step finished", task)

    def task_exited(self, task):
        self._print_with_task("### task exited", task)

    def before_io_wait(self, timeout):
        if timeout:
            print("### waiting for I/O for up to {} seconds".format(timeout))
        else:
            print("### doing a quick check for I/O")
        self._sleep_time = trio.current_time()

    def after_io_wait(self, timeout):
        duration = trio.current_time() - self._sleep_time
        print("### finished I/O check (took {} seconds)".format(duration))

    def after_run(self):
        print("!!! run finished")


async def __executor(task):
    logging.info(f"Staring executor for task: {task}")
    await trio.sleep(10)
    logging.info(f"Task {task} complete!")


async def task_spawner(nursery, task_queue):
    while True:
        if not task_queue.empty():
            task = task_queue.get_nowait()
            nursery.start_soon(__executor, task)
            logging.info(f"Started an executor task: {task}")
        await trio.sleep(0)


async def __start_vm(task_queue):
    logging.info("Starting VM")
    async with trio.open_nursery() as nursery:
        nursery.start_soon(task_spawner, nursery, task_queue)
        logging.info("Task spawner started.")


def start_vm_thread():
    task_queue = queue.Queue()
    x = threading.Thread(target=lambda: trio.run(__start_vm, task_queue))
    x.start()

    return task_queue


class VM:
    TASK_QUEUE_BUFFER_SIZE = 10

    def __init__(self):
        self.task_queue = queue.Queue(self.TASK_QUEUE_BUFFER_SIZE)
        self.vm_thread = threading.Thread(target=lambda: trio.run(self.__starter))
        self.vm_thread.start()

    async def __starter(self):
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.task_spawner, nursery)
            # Started anything else required for the VM
            logging.info("__starter: Started task_spawner")

    async def task_spawner(self, nursery):
        while True:
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
        # Stop vm_thread
        pass
