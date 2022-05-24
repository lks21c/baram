import pytest

from baram.async_crawler import AsyncCrawler


@pytest.fixture()
def ac():
    return AsyncCrawler()


def test_crawl_urls(ac):
    urls = ['http://www.google.com',
            'http://www.naver.com']
    htmls = ac.crawl_urls(urls)
    assert htmls is not None
    print(htmls[0])


def test_crawl_urls_with_header(ac):
    urls = ['http://www.google.com',
            'http://www.naver.com']
    header = {
        'User-Agent': 'Mozilla'
    }
    htmls = ac.crawl_urls(urls, header)
    assert htmls is not None
    print(htmls[0])
