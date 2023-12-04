from __future__ import annotations
import os
import re
import json
from functools import partial
from types import TracebackType
from typing import Type, Optional
from bs4 import BeautifulSoup as bs
from crawler_base import CrawlerBase
from async_scheduler import AsyncScheduler
from config import config
from logger import logger, DEBUG


class LongCommentCrawler(CrawlerBase):
    def __init__(self, async_scheduler: AsyncScheduler) -> None:
        logger.info("init long comment crawler...")

        logger.debug("get long_comment config")
        self.long_comment_config = config.get("long_comment")
        self.max_comment_page_num = self.long_comment_config["max_comment_page_num"]
        self.one_page_review_num = self.long_comment_config["one_page_review_num"]

        self.top250_path = self.long_comment_config["top250_path"]
        self.top250_txt_name = self.long_comment_config["top250_txt_name"]
        self.save_path = self.long_comment_config["save_path"]

        logger.debug("generate tasks list")
        # Visit the movie review page and get the total number of reviews
        # {"title": , "director": , "url": , "id": }
        self.top250_id_list = []
        self.get_reviews_num_tasks = []

        # Visit the movie review page to get the full review id
        # {'title': title, 'id': id, 'comment_num': number_of_comments}
        self.movie_review_page_num = []
        self.movie_review_page_tasks = []

        # Get all comments by the full comment id
        # {'title': title, "review_id": review_id, "star": , "ch_star": }
        self.full_comment_id_list = []
        self.get_full_comment_tasks = []

        # {"title": title, "review_id": review_id, "star": star, "ch_star": ch_star, "comment": text}
        self.long_comment_results = []

        super().__init__(async_scheduler,
                         save_path=self.save_path)

        logger.info("init long comment crawler finish!")

    
    def __enter__(self) -> LongCommentCrawler:
        return self


    def __exit__(self, exc_type: Type[Optional[BaseException]], exc_value: Optional[BaseException], traceback: Optional[TracebackType]) -> bool:
        self.async_scheduler.join()
        self.save_full_comments()
        if traceback:
            logger.error(f'exit error: [{exc_type}]{exc_value}\n{traceback}')
            print(traceback)
            return False
        return True


    def start_and_join(self) -> None:
        logger.info("[LongCommentCrawler] start and join")
        logger.debug("start get reviews num tasks")
        self.read_top250_file()
        self.crawler_add_tasks(self.get_reviews_num_tasks)
        self.start()
        self.async_scheduler.join()

        logger.debug("start movie review page tasks")
        self.generate_movie_all_page_tasks()
        self.crawler_add_tasks(self.movie_review_page_tasks)
        self.start()
        self.async_scheduler.join()

        logger.debug("start get full comment tasks")
        self.generate_full_comment_tasks()
        self.crawler_add_tasks(self.get_full_comment_tasks)
        self.start()
        self.async_scheduler.join()

        logger.info("finish all long comment tasks!")


    def read_top250_file(self, filepath:str=None) -> None:
        logger.debug(f"read top250 file from filepath={filepath}")
        with open(filepath if filepath else os.path.join(self.top250_path, self.top250_txt_name), 
                  'r', encoding='utf-8') as f_obj:
            for line in f_obj:
                dict_line = json.loads(line.strip())
                self.top250_id_list.append(dict_line)
        
        logger.debug("create get long comment ids tasks")
        self.get_reviews_num_tasks = [(f'https://movie.douban.com/subject/{_["id"]}/reviews', 
                                       partial(self.movie_page_handler, title=_["title"], id=_["id"])) 
                                      for _ in self.top250_id_list]
        logger.debug("process top250 file success")

    
    def generate_movie_all_page_tasks(self) -> None:
        logger.debug(f"generate all movie review pages, movie num={len(self.movie_review_page_num)}")
        self.movie_review_page_tasks = []
        for info in self.movie_review_page_num:
            tasks = [(f'https://movie.douban.com/subject/{info["id"]}/reviews?start={_ * self.one_page_review_num}', 
                      partial(self.review_page_handler, title=info["title"])) 
                     for _ in range(1, min(self.max_comment_page_num, -(-int(info["comment_num"]) // self.one_page_review_num)))]
            self.movie_review_page_tasks += tasks
        logger.debug(f"generate all movie review pages success")

    
    def generate_full_comment_tasks(self) -> None:
        logger.debug(f"generate full comments, comments num={len(self.full_comment_id_list)}")
        self.get_full_comment_tasks = []
        for info in self.full_comment_id_list:
            tasks = [(f'https://movie.douban.com/j/review/{info["review_id"]}/full',
                     partial(self.review_handler, title=info["title"], review_id=info["review_id"], star=info["star"], ch_star=info["ch_star"]))]
            self.get_full_comment_tasks += tasks
        logger.debug(f"generate full comments success")


    def movie_page_handler(self, resp:str, title:str=None, id:str=None) -> None:
        logger.debug("enter movie page handler")

        soup = bs(resp, 'html.parser')
        title_comments_match = re.search(r'(.*)的影评 \((\d+)\)', soup.title.string)
        if title_comments_match:
            number_of_comments = title_comments_match.group(2)
        else:
            number_of_comments = self.max_comment_page_num * self.one_page_review_num

        with self.lock:
            self.movie_review_page_num += [{'title': title, 'id': id, 'comment_num': number_of_comments}]
        
        logger.debug("movie page handler success")


    def review_page_handler(self, resp:str, title:str=None) -> None:
        '''get review id and star'''
        logger.debug("enter review page handler")

        soup = bs(resp, 'lxml')
        results = []
        for review in soup.select('.review-item'):
            result = {"title": title}
            match = re.search(r'review_(\d+)_full', str(review))
            if match:
                review_id = match.group(1)
                result["review_id"] = review_id

            rating = review.select_one('.main-title-rating')
            result["star"] = rating.get('class')[0] if rating else None
            result["ch_star"] = rating.get('title') if rating else None
            results.append(result)
            # logger.debug(f"review page handler result: {results[-1]}")

        with self.lock:
            self.full_comment_id_list += results

        logger.debug("review page handler success")


    def review_handler(self, resp:str, title:str=None, review_id:str=None, star:str=None, ch_star:str=None) -> None:
        '''parse full comment'''
        logger.debug("enter review handler")
        comment = json.loads(resp)['html']
        soup = bs(comment, 'html.parser')
        text = soup.get_text()
        with self.lock:
            self.long_comment_results += [{"title": title, "review_id": review_id, "star": star, "ch_star": ch_star, "comment": text}]
        logger.debug("review handler success")


    def save_full_comments(self, path:str=None) -> None:
        logger.info(f"save full comments at {path if path else self.save_path}, num={len(self.long_comment_results)}")
        os.makedirs(self.save_path, exist_ok=True)
        with self.lock:
            for i in self.long_comment_results:
                os.makedirs(os.path.join(self.save_path, i["title"]), exist_ok=True)
                with open(os.path.join(self.save_path, i["title"], f"{i['review_id']}.txt"), 'w') as f_obj:
                    json.dump(i, f_obj, ensure_ascii=False, indent=4)
        logger.info("save full comments success!")


if __name__ == "__main__":
    logger.setLevel(DEBUG)
    with AsyncScheduler() as scheduler:
        scheduler.start()
        crawler = LongCommentCrawler(scheduler)
        crawler.start_and_join()
        crawler.save_full_comments()
    print("Done!")
