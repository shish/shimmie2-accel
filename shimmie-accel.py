#!/usr/bin/env python

import json
import SocketServer
import threading
import time
import socket
from ConfigParser import SafeConfigParser

import logging
log = logging.getLogger('shacc')


def get_db(config):
    log.info("Connecting to DB")
    if config.protocol == "postgres":
        import psycopg2
        db = psycopg2.connect("dbname=%s user=%s password=%s" % (
            config.database, config.username, config.password))
    else:
        raise Exception("Unsupported database: %r" % config.protocol)
    return db


def get_tags(db):
    log.info("Fetching fresh data")
    cur = db.cursor()

    cur.execute("SELECT id, tag FROM tags")
    tag_id_to_tag = {}
    for (tag_id, tag) in cur:
        tag_id_to_tag[tag_id] = tag.lower()

    cur.execute("SELECT tag_id, image_id FROM image_tags ORDER BY image_id DESC")
    tags = {}
    for tag_id, image_id in cur:
        tag = tag_id_to_tag[tag_id]
        if tag not in tags:
            tags[tag] = set()
        tags[tag].add(image_id)

    cur.close()

    log.info("Fetched fresh data")
    return tags


def handle_req(req, tags):
    yays = [x.lower() for x in req.get('yays', [])]
    nays = [x.lower() for x in req.get('nays', [])]
    offset = req.get('offset', 0)
    limit = req.get('limit', 50)

    if not yays:
        yays.append('')

    results = tags.get(yays[0], set()).copy()
    for tag in yays[1:]:
        results &= tags.get(tag, set())
    for tag in nays:
        results -= tags.get(tag, set())

    data = sorted(list(results), reverse=True)[offset:offset+limit]
    log.info("%r" % req)
    return data


_update_in_progress = False

class ShAccHandler(SocketServer.StreamRequestHandler):
    def handle(self):
        data = None
        try:
            data = self.request.recv(4096).strip()
            req = json.loads(data)
            if req.get('reset'):
                global _update_in_progress
                if not _update_in_progress:
                    try:
                        _update_in_progress = True
                        self.request.close()
                        self.server.tags = get_tags(self.server.db)
                    finally:
                        _update_in_progress = False
            else:
                data = handle_req(req, self.server.tags)
                self.request.send(json.dumps(data))
        except Exception:
            log.exception("Error handling request %r:" % data)
        return True


class ThreadingTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass


class Config(object):
    def __init__(self, filename):
        cp = SafeConfigParser()
        cp.read(filename)
        self.address = cp.get('accel', 'address') or '0.0.0.0'
        self.port = cp.getint('accel', 'port') or 21212
        self.refresh = cp.getint('accel', 'refresh') or 300
        self.protocol = cp.get('database', 'protocol')
        self.database = cp.get('database', 'database')
        self.hostname = cp.get('database', 'hostname')
        self.username = cp.get('database', 'username')
        self.password = cp.get('database', 'password')


class Refresher(threading.Thread):
    def __init__(self, refresh, server):
        threading.Thread.__init__(self)
        self.name = "refresher"
        self.daemon = True

        self.refresh = refresh
        self.server = server

    def run(self):
        (ip, port) = self.server.server_address
        if ip == '0.0.0.0':
            ip = '127.0.0.1'
        while True:
            try:
                log.info("Refreshing")
                time.sleep(self.refresh)

                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((ip, port))
                sock.send(json.dumps({'reset': True}))
                sock.close()
            except Exception:
                log.exception("Error refreshing")


def main():
    config = Config('shimmie-accel.ini')
    logging.basicConfig(
        format="%(asctime)s %(thread)d %(message)s",
        level=logging.DEBUG
    )

    ThreadingTCPServer.allow_reuse_address = 1
    server = ThreadingTCPServer((config.address, config.port), ShAccHandler)
    server.db = get_db(config)
    server.tags = get_tags(server.db)

    if config.refresh:
        refresher = Refresher(config.refresh, server)
        refresher.start()

    server.serve_forever()


if __name__ == "__main__":
    main()
