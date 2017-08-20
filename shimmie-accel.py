#!/usr/bin/env python3

import asyncio
import json
import time
import socket
from configparser import SafeConfigParser

import logging
log = logging.getLogger('shacc')


class Config(object):
    def __init__(self, filename):
        cp = SafeConfigParser()
        cp.read(filename)
        self.address = cp.get('accel', 'address') or '0.0.0.0'
        self.port = cp.getint('accel', 'port') or 21212
        self.refresh = cp.getint('accel', 'refresh') or 300
        self.timeout = cp.getint('accel', 'timeout') or 120
        self.protocol = cp.get('database', 'protocol')
        self.database = cp.get('database', 'database')
        self.hostname = cp.get('database', 'hostname')
        self.username = cp.get('database', 'username')
        self.password = cp.get('database', 'password')


class Accel():
    def __init__(self):
        self.tags = {}
        self._update_in_progress = False

        self.db = None
        self.config = Config('shimmie-accel.ini')

        if self.config.protocol == "postgres":
            self._dsn = "dbname=%s user=%s password=%s" % (
                self.config.database,
                self.config.username, 
                self.config.password
            )
        else:
            raise Exception("Unsupported database: %r" % self.config.protocol)

    async def _update_tags(self):
        log.info("Fetching fresh data")
        tags = {}

        import aiopg
        async with aiopg.create_pool(self._dsn, timeout=self.config.timeout) as db:
            async with db.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        SELECT lower(tag), array_agg(image_id)
                        FROM image_tags
                        JOIN tags ON image_tags.tag_id = tags.id
                        GROUP BY tags.tag
                    """)
                    async for tag, image_ids in cur:
                        tags[tag] = set(image_ids)
        log.info("Fetched fresh data")
        self.tags = tags
        return tags

    async def handle_query(self, reader, writer):
        #addr = writer.get_extra_info('peername')
        data = None
        try:
            data = await reader.read(4096)
            data = data.strip()
            req = json.loads(data.decode('utf8'))

            start = time.time()
            yays = [x.lower() for x in req.get('yays', [])]
            nays = [x.lower() for x in req.get('nays', [])]
            offset = req.get('offset', 0)
            limit = req.get('limit', 50)

            if not yays:
                yays.append('')

            results = self.tags.get(yays[0], set()).copy()
            for tag in yays[1:]:
                results &= self.tags.get(tag, set())
            for tag in nays:
                results -= self.tags.get(tag, set())

            data = sorted(list(results), reverse=True)[offset:offset+limit]
            log.info("%r %.4f" % (req, time.time() - start))
            
            writer.write(json.dumps(data).encode('utf8'))
            await writer.drain()
            writer.close()
        except Exception:
            log.exception("Error handling request %r:" % data)
        return True

    async def refresher(self):
        while self.config.refresh > 0:
            if not self._update_in_progress:
                try:
                    self._update_in_progress = True
                    await self._update_tags()
                finally:
                    self._update_in_progress = False
            await asyncio.sleep(self.config.refresh)

    def run(self):
        logging.basicConfig(
            # format="%(asctime)s %(message)s",
            format="%(message)s",
            level=logging.DEBUG
        )

        loop = asyncio.get_event_loop()

        loop.run_until_complete(
            self._update_tags()
        )

        server = loop.run_until_complete(asyncio.gather(
            asyncio.Task(self.refresher()),
            asyncio.start_server(self.handle_query, self.config.address, self.config.port, loop=loop),
        ))
        server.close()
        loop.run_until_complete(server.wait_closed())

        loop.close()


if __name__ == "__main__":
    Accel().run()
