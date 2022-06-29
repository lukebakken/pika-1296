import asyncio
from code import RabbitMQ


async def do_random():
    while True:
        await asyncio.sleep(2)
        print("[*] Random!")


async def do_rabbitmq():
    rmq = RabbitMQ()
    rmq.start()


async def main():
    random_task = asyncio.create_task(do_random())
    asyncio.create_task(do_rabbitmq())
    await asyncio.gather(random_task)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
