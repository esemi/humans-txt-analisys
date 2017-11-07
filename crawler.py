#! /usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from collections import Counter

import asyncio
import aiohttp
from async_timeout import timeout

from db import get_domains_for_crawling, save_humans_txt, is_success_response

MAX_CLIENTS = 5000
LIMIT_DOMAINS = 1000000
TIMEOUT = 20


def url_gen(root_domain):
    return list(map(lambda x: x % root_domain, ['https://%s/humans.txt', 'https://www.%s/humans.txt',
                                                'http://%s/humans.txt', 'http://www.%s/humans.txt']))


async def task(rank, domain, sem: asyncio.Semaphore, cnt: Counter):
    async def fetch(url) -> tuple:
        try:
            async with timeout(TIMEOUT):
                async with session.get(url, allow_redirects=False, verify_ssl=False) as resp:
                    logging.debug('response %s %s', resp.url, resp.status)
                    content = await resp.text(errors='ignore')
                    return resp.status, content, resp.url
        except BaseException as e:
            logging.debug('exception %s %s', url, type(e))
            return 0, None, url

    async with sem:
        async with aiohttp.ClientSession() as session:
            for url in url_gen(domain):
                code, content, url_end = await fetch(url)

                # todo if is redirect - next try
                # todo if success but not txt content - exception and break

                if is_success_response(code):
                    break
            cnt[code] += 1
            await save_humans_txt(domain, rank, content, code)

            if not sum(cnt.values()) % 1000:
                logging.info('progress counters %s', cnt.items())


async def run():
    domains = get_domains_for_crawling(LIMIT_DOMAINS)
    logging.info('found %d domains for crawling', len(domains))
    if not domains:
        return

    sem = asyncio.Semaphore(MAX_CLIENTS)
    cnt = Counter()
    tasks = [asyncio.ensure_future(task(rank, root_domain, sem, cnt)) for rank, root_domain in domains]
    del domains
    await asyncio.wait(tasks)
    logging.info('end with counters %s', cnt.items())


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
    ioloop = asyncio.new_event_loop()
    ioloop.run_until_complete(run())
    ioloop.close()
