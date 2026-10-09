import os
import asyncio
import logging

logging.basicConfig(level=logging.INFO)

async def handle_client(reader, writer):
    try:
        # Read the browser or script's initial request line
        request_line = await reader.readline()
        if not request_line:
            writer.close()
            return

        parts = request_line.decode('utf-8', errors='ignore').split()
        if len(parts) < 2:
            writer.close()
            return

        method, url = parts[0], parts[1]

        # -------------------------------------------------------------
        # Handle Secure HTTPS Traffic (CONNECT Tunneling)
        # -------------------------------------------------------------
        if method.upper() == 'CONNECT':
            host, port = url.split(':') if ':' in url else (url, 443)
            port = int(port)

            # Open a clean pipeline to the destination site (e.g., ipinfo.io)
            remote_reader, remote_writer = await asyncio.open_connection(host, port)
            
            # Send a clear success header back to your local machine
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()

            # Stream the data bidirectionally
            async def pipe(src, dst):
                try:
                    while True:
                        data = await src.read(8192)
                        if not data:
                            break
                        dst.write(data)
                        await dst.drain()
                except Exception:
                    pass
                finally:
                    try:
                        dst.close()
                        await dst.wait_closed()
                    except Exception:
                        pass

            asyncio.create_task(pipe(reader, remote_writer))
            asyncio.create_task(pipe(remote_reader, writer))

        # -------------------------------------------------------------
        # Handle Standard HTTP Traffic (GET/POST)
        # -------------------------------------------------------------
        else:
            if url.startswith('http://'):
                url = url[7:]
            
            path_parts = url.split('/', 1)
            host_parts = path_parts[0].split(':')
            host = host_parts[0]
            port = int(host_parts[1]) if len(host_parts) > 1 else 80

            remote_reader, remote_writer = await asyncio.open_connection(host, port)
            remote_writer.write(request_line)

            while True:
                line = await reader.readline()
                remote_writer.write(line)
                if line == b'\r\n' or not line:
                    break
            await remote_writer.drain()

            while True:
                data = await remote_reader.read(8192)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
                
            writer.close()
            await writer.wait_closed()
            remote_writer.close()
            await remote_writer.wait_closed()

    except Exception as e:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def main():
    # Dynamically read the active network port assigned by Render
    port = int(os.environ.get("PORT", 10000))
    server = await asyncio.start_server(handle_client, '0.0.0.0', port)
    logging.info(f"🚀 High-Speed HTTP Web Proxy running on port {port}...")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
