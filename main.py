import os
import asyncio

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
        except Exception:
            pass

async def handle_client(reader, writer):
    try:
        # Read the initial incoming request line from your local machine
        request_line = await reader.readline()
        if not request_line:
            writer.close()
            return

        parts = request_line.decode('utf-8', errors='ignore').split()
        if len(parts) < 2:
            writer.close()
            return

        method, url = parts[0], parts[1]

        # Handle HTTPS CONNECT traffic natively through Render's web filters
        if method.upper() == 'CONNECT':
            host, port = url.split(':') if ':' in url else (url, 443)
            port = int(port)

            remote_reader, remote_writer = await asyncio.open_connection(host, port)
            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()

            asyncio.create_task(pipe(reader, remote_writer))
            asyncio.create_task(pipe(remote_reader, writer))

        # Handle standard HTTP traffic cleanly
        else:
            clean_url = url[7:] if url.startswith('http://') else url
            path_parts = clean_url.split('/', 1)
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

            asyncio.create_task(pipe(remote_reader, writer))
            asyncio.create_task(pipe(reader, remote_writer))

    except Exception:
        try:
            writer.close()
        except Exception:
            pass

async def main():
    # Read the dynamic port assigned by Render's routing architecture
    port = int(os.environ.get("PORT", 10000))
    server = await asyncio.start_server(handle_client, '0.0.0.0', port)
    print(f"🚀 Custom Proxy Operational on port {port}...")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
