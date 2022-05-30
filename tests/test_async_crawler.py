from pprint import pprint

import pytest

from baram.async_crawler import AsyncCrawler


@pytest.fixture()
def ac():
    return AsyncCrawler()


def test_crawl_urls(ac):
    urls = ['http://www.google.com',
            'http://www.naver.com']
    htmls = ac.request_urls('GET', urls)
    assert htmls is not None
    print(htmls[0])


def test_crawl_urls_with_header(ac):
    urls = ["http://httpbin.org/get"]
    header = {
        'User-Agent': 'Mozilla22'
    }
    htmls = ac.request_urls('GET', urls, headers=header)
    assert htmls is not None
    print(htmls[0])


def test_naver_cafe(ac):
    headers = {
        'authority': 'apis.naver.com',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/json;charset=UTF-8',
        'cookie': 'NNB=VHEGGEW73VNWC; NID_AUT=7ECcooBLlCFP+4Qp7OhRgJlJOHx+T3pv4SFW5SOzjUY3LDw5DEej2kiVpTPH864k; NID_JKL=Nc9wZV+m4q9R68U2dsff/xPBlkdXaSEu6rsU/fiam00=; nx_ssl=2; page_uid=hFWC8wp0YiRssieBhsdssssstwh-414629; BMR=s=1653289999911&r=https%3A%2F%2Fm.blog.naver.com%2F21ahn%2F221329219163&r2=https%3A%2F%2Fwww.google.com%2F; NID_SES=AAABgZEM40Y8tLIHFvcWEpkW64hFo0PsK6fpqx+qRBZIoaptSaVr/VQ0dCKSUY584pZdfTdlHCGT53urxhsgW2+ZewI6mbm0tEkoqvrAUY9NUOcQn1QKHCxFeVJ248E5E3+GI7mBVVjQwW9zJfH8ifkzgvhDp6EWztjR9tKd+vzMLiE3tsrc7LmEoBn0RpbCHqZeKHbQXKQcfVree0q4y8bs4uCWod2VNyQHCG3Vufs+laHUYsJm9o1HZWCC3Gtev9GiqSBwyAJJrQPxYVHI+j5VAXRK+1HHK2fCNagucIQXXupzCyjO/tkHOP/dEhuY0LYtF9hxf8FqCARAdadwxUbLg4Pr8mdA2WirrRiQ52K/GzROpPvq4QYjNWC5W24AZDYPU407RK4CfQCLNhu3OtPz8xZ5sneqAnSXTm8Wxy2Pupsr0iTmGK8+c+t1ibnWAS//UOaYeB2JpHBRCH5vaaxqEcfU/Or9em3j3mG4+zj3gdbc74glJEjRI9vW2CDOpuEJjgXqItn1Pt2fn9zDeRYydzw=',
        'origin': 'https://section.cafe.naver.com',
        'referer': 'https://section.cafe.naver.com/ca-fe/home/search/articles?q=%EC%8B%A4%EB%B9%84&p=1&em=1&pr=7&ps=20220110&pe=20220510&ia=%EC%8B%A4%EB%B9%84',
        'sec-ch-ua': '".Not/A)Brand";v="99", "Google Chrome";v="103", "Chromium";v="103"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Mobile Safari/537.36',
        'x-cafe-product': 'pc',
    }

    json_data = {
        'query': '실비',
        'page': 1,
        'sortBy': 0,
        'exceptMarketArticle': 1,
        'period': [
            '20220110',
            '20220510',
        ],
        'includeAll': '실비',
    }

    htmls = ac.request_urls('POST',
                            ['https://apis.naver.com/cafe-home-web/cafe-home/v1/search/articles'],
                            json=json_data,
                            headers=headers)
    pprint(htmls[0])
