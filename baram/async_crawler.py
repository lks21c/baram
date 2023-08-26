import asyncio
from typing import Optional

import aiohttp
import nest_asyncio
from aiohttp import ClientSession


class AsyncCrawler(object):

    def request_urls(self, method: str, urls: list, **kwargs: dict) -> list:
        '''
        Retruns crawled htmls from urls using async io.

        :param method: GET or POST
        :param urls: http url list
        :param **kwargs: additional parameters.
        :return: html list
        '''

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            nest_asyncio.apply()
        finally:
            return loop.run_until_complete(self._fetch_pages(method, urls, **kwargs))

    async def _fetch_pages(self, method: str, urls: list, **kwargs: dict) -> Optional[tuple]:
        async with aiohttp.ClientSession() as session:
            return await asyncio.gather(
                *[self._fetch_page(session, method, url, **kwargs) for url in urls], return_exceptions=True)

    async def _fetch_page(self, session: ClientSession, method: str, url: str, **kwargs: dict) -> str:
        async with session.request(method, url, ssl=False, **kwargs) as response:
            html = await response.text()
            return html if response.status == 200 else None
