import asyncio


async def foo():
    await bar()


async def bar():
    await asyncio.create_task(print("bar"))


asyncio.run(foo())
# asyncio.run(bar())
