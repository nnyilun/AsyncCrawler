from __future__ import annotations
import re
from types import TracebackType
from typing import Type, Optional
from bs4 import BeautifulSoup as bs
from crawler_base import CrawlerBase
from async_scheduler import AsyncScheduler
from config import config
from logger import logger


class Top250Crawler(CrawlerBase):
    def __init__(self, async_scheduler:AsyncScheduler) -> None:
        logger.info("init top250 crawler...")

        logger.debug("get douban_top250 config")
        self.top250_config = config.get("douban_top250")
        self.max_movies = self.top250_config["max_movies"]
        self.one_page_movie_num = self.top250_config["one_page_movie_num"]

        self.save_path = self.top250_config["save_path"]
        self.file_name = self.top250_config["save_file_name"]

        logger.debug("generate tasks list")
        self.top250_tasks = [(f'https://movie.douban.com/top250?start={_ * self.one_page_movie_num}', self.top250_handler) 
                                for _ in range(-(-self.max_movies // self.one_page_movie_num))]

        super().__init__(async_scheduler,
                         tasks=self.top250_tasks,
                         save_path=self.save_path,
                         file_name=self.file_name)

        logger.info("init top250 crawler finish!")


    def __enter__(self) -> Top250Crawler:
        return self


    def __exit__(self, exc_type: Type[Optional[BaseException]], exc_value: Optional[BaseException], traceback: Optional[TracebackType]) -> bool:
        self.async_scheduler.join()
        self.save_results_to_txt()
        if traceback:
            logger.error(f'exit error: [{exc_type}]{exc_value}\n{traceback}')
            print(traceback)
            return False
        return True

    
    def top250_handler(self, resp:str) -> None:
        logger.debug("enter top250 handler")
        with self.lock:
            if resp is None:
                logger.warn('resp is None!', exc_info=True, stack_info=True)
                return None

            soup = bs(resp, 'lxml')
            for item in soup.select('li .item'):
                title = item.select_one('.title').text.strip()
                director = item.select_one('.bd p').text.split('\xa0\xa0\xa0')[0].split(': ')[1].strip()
                url = item.select_one('.hd a')['href'].strip()

                match = re.search(r'/subject/(\d+)/', url)
                if match:
                    id = match.group(1)
                else:
                    id = None

                self.results.append({'title': title, 'director': director, 'url': url, 'id': id})
                logger.debug(f"top250 handler result: {self.results[-1]}")
        logger.debug(f"top250 handler success")


if __name__ == "__main__":
    with AsyncScheduler() as scheduler:
        scheduler.start()
        top250_crawler = Top250Crawler(scheduler)
        top250_crawler.start()
        scheduler.join()

    top250_crawler.save_results_to_csv()
    top250_crawler.save_results_to_txt()
    print(len(top250_crawler.get_results()))