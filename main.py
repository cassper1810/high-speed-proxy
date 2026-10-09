import os
import asyncio
import logging

logging.basicConfig(level=logging.INFO)

async def pipe(reader, writer):
    try:
        while True:
            data = await reader.read(8192)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def handle_client(reader, writer):
    try:
        # Read the initial browser or script request header line
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
        # Handle HTTPS Connection (CONNECT Tunneling)
        # -------------------------------------------------------------
        if method.upper() == 'CONNECT':
            if ':' in url:
                host, port = url.split(':')
                port = int(port)
            else:
                host, port = url, 443

            # Open connection to the destination site (e.g. ipinfo.io)
            remote_reader, remote_writer = await asyncio.open_connection(host, port)
            
            # Send the clean connection protocol acknowledgement back to your machine
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()

            # Pass bi-directional streams cleanly across the socket layers
            asyncio.create_task(pipe(reader, remote_writer))
            asyncio.create_task(pipe(remote_reader, writer))

        # -------------------------------------------------------------
        # Handle Standard HTTP Traffic (GET/POST)
        # -------------------------------------------------------------
        else:
            # Strip protocol prefix if present
            clean_url = url[7:] if url.startswith('http://') else url
            path_parts = clean_url.split('/', 1)
            host_parts = path_parts[0].split(':')
            host = host_parts[0]
            port = int(host_parts[1]) if len(host_parts) > 1 else 80

            remote_reader, remote_writer = await asyncio.open_connection(host, port)
            
            # Forward the complete initial line along with the rest of the stream headers
            remote_writer.write(request_line)
            while True:
                line = await reader.readline()
                remote_writer.write(line)
                if line == b'\r\n' or not line:
                    break
            await remote_writer.drain()

            asyncio.create_task(pipe(remote_reader, writer))
            asyncio.create_task(pipe(reader, remote_writer))

    except Exception as e:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def main():
    # Render binds the active running port natively via the PORT variable
    port = int(os.environ.get("PORT", 10000))
    server = await asyncio.start_server(handle_client, '0.0.0.0', port)
    logging.info(f"🚀 High-Speed Proxy running on port {port}...")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
