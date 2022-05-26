import asyncio

import aiohttp
import nest_asyncio


class AsyncCrawler(object):

    def get_urls(self, urls: list, **kwargs: dict) -> list:
        '''
        Retruns crawled htmls from urls using async io.

        :param urls: http url list
        :param **kwargs: additional parameters.
        :return: html list
        '''

        try:
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self._fetch_pages('GET', urls, **kwargs))
        except RuntimeError:
            nest_asyncio.apply()
            return loop.run_until_complete(self._fetch_pages('GET', urls, **kwargs))

    def post_urls(self, urls: list, **kwargs: dict) -> list:
        try:
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self._fetch_pages('POST', urls, **kwargs))
        except RuntimeError:
            nest_asyncio.apply()
            return loop.run_until_complete(self._fetch_pages('POST', urls, **kwargs))

    async def _fetch_pages(self, method: str, urls: list, **kwargs: dict):
        if method == 'GET':
            async with aiohttp.ClientSession() as session:
                return await asyncio.gather(
                    *[self._fetch_get_page(session, url, **kwargs) for url in urls], return_exceptions=True)
        elif method == 'POST':
            async with aiohttp.ClientSession() as session:
                return await asyncio.gather(
                    *[self._fetch_post_page(session, url, **kwargs) for url in
                      urls],
                    return_exceptions=True)

    async def _fetch_get_page(self, session, url: str, **kwargs: dict) -> str:
        async with session.get(url, **kwargs) as response:
            html = await response.text()
            return html if response.status == 200 else None

    async def _fetch_post_page(self, session, url: str,  **kwargs: dict) -> str:
        async with session.post(url,  **kwargs) as response:
            html = await response.text()
            return html if response.status == 200 else None