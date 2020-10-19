import logging
import threading
import trio

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")
logging.info("root: Logging setup complete")


async def __executor(task):
    logging.info(f"Staring executor for task: {task}")
    await trio.sleep(10)
    logging.info(f"Task {task} complete!")


async def __start_vm():
    logging.info("Starting VM")
    async with trio.open_nursery() as nursery:
        nursery.start_soon(__executor, "A")
        nursery.start_soon(__executor, "B")
        nursery.start_soon(__executor, "C")
        nursery.start_soon(__executor, "D")
    logging.info("All tasks have been executed. Shutting down VM.")


def start_vm_thread():
    x = threading.Thread(target=trio.run, args=(__start_vm,))
    x.start()


def send_task(task):
    logging.info("This task will be sent to VM")
