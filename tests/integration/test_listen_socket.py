import asyncio
import socket

from taskqueue.api import listen_socket


def test_accepted_connections_get_tcp_nodelay():
    """Regression: without an explicit IPPROTO_TCP, asyncio skips TCP_NODELAY
    and keep-alive responses stall ~40 ms on Nagle + delayed ACK."""
    sock = listen_socket("127.0.0.1", 0)
    port = sock.getsockname()[1]
    seen = {}

    async def main():
        async def on_connect(reader, writer):
            seen["nodelay"] = writer.get_extra_info("socket").getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
            writer.close()

        server = await asyncio.start_server(on_connect, sock=sock)
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        await asyncio.sleep(0.1)
        writer.close()
        server.close()

    asyncio.run(main())
    assert sock.proto == socket.IPPROTO_TCP
    assert seen["nodelay"] != 0
