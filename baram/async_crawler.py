import asyncio

import aiohttp
import nest_asyncio


class AsyncCrawler(object):

    def crawl_urls(self, urls: list, header: dict = None) -> list:
        '''
        Retruns crawled htmls from urls using async io.

        :param urls: http url list
        :param header: http request header as dictionary
        :return: html list
        '''

        try:
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self._fetch_pages(urls, header))
        except RuntimeError:
            nest_asyncio.apply()
            return loop.run_until_complete(self._fetch_pages(urls, header))

    async def _fetch_pages(self, urls: list, header: dict):
        async with aiohttp.ClientSession() as session:
            return await asyncio.gather(
                *[self._fetch_page(session, url, header) for url in urls], return_exceptions=True)

    async def _fetch_page(self, session, url: str, header: dict) -> str:
        async with session.get(url, headers=header) as response:
            html = await response.text()
            return html if response.status == 200 else None
