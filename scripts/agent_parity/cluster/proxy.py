"""TCP 6820 -> slurmrestd's unix socket, so the host reaches it as root (rest_auth/local)."""

import asyncio


async def pipe(reader, writer):
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()


async def handle(client_r, client_w):
    up_r, up_w = await asyncio.open_unix_connection("/run/rr/s.sock")
    await asyncio.gather(pipe(client_r, up_w), pipe(up_r, client_w))


async def main():
    server = await asyncio.start_server(handle, "0.0.0.0", 6820)
    await server.serve_forever()


asyncio.run(main())
