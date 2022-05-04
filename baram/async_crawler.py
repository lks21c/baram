import asyncio

import aiohttp
import nest_asyncio


class AsyncCrawler(object):

    def crawl_urls(self, urls: list):
        '''
        Retruns crawled htmls from urls using async io.

        :param urls: http url list
        :return: html list
        '''

        try:
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(self._fetch_pages(urls))
        except RuntimeError:
            nest_asyncio.apply()
            return loop.run_until_complete(self._fetch_pages(urls))

    async def _fetch_pages(self, urls: list):
        async with aiohttp.ClientSession() as session:
            return await asyncio.gather(
                *[self._fetch_page(session, url) for url in urls], return_exceptions=True)

    async def _fetch_page(self, session, url: str):
        async with session.get(url) as response:
            html = await response.text()
            return html if response.status == 200 else None
