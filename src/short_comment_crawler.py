from __future__ import annotations
import os
import json
from functools import partial
from types import TracebackType
from typing import Type, Optional
from bs4 import BeautifulSoup as bs
from crawler_base import CrawlerBase
from async_scheduler import AsyncScheduler
from config import config
from logger import logger, DEBUG


class ShortCommentCrawler(CrawlerBase):
    def __init__(self, async_scheduler: AsyncScheduler) -> None:
        logger.info("init short comment crawler...")

        logger.debug("get short_comment config")
        self.short_comment_config = config.get("short_comment")
        self.max_comment_page_num = self.short_comment_config["max_comment_page_num"]
        self.one_page_review_num = self.short_comment_config["one_page_review_num"]

        self.top250_path = self.short_comment_config["top250_path"]
        self.top250_txt_name = self.short_comment_config["top250_txt_name"]
        self.save_path = self.short_comment_config["save_path"]

        logger.debug("generate tasks list")
        # Visit the movie review page and get the total number of reviews
        # {"title": , "director": , "url": , "id": }
        self.top250_id_list = []
        self.get_reviews_num_tasks = []

        # Visit the movie review page to get the short review
        # {'title': title, 'id': id, 'comment_num': number_of_comments}
        self.movie_review_num = []
        self.movie_reviews_tasks = []
        
        # {"title": title, "id":id, 
        #  "comment_text": comment_text, 
        #  "textual_rating": textual_rating, "complete_numeric_rating": complete_numeric_rating}
        self.short_comment_results = []

        super().__init__(async_scheduler,
                         save_path=self.save_path)
        
        logger.info("init short comment crawler finish!")

    
    def __enter__(self) -> ShortCommentCrawler:
        return self


    def __exit__(self, exc_type: Type[Optional[BaseException]], exc_value: Optional[BaseException], traceback: Optional[TracebackType]) -> bool:
        self.async_scheduler.join()
        self.save_short_comments()
        if traceback:
            logger.error(f'exit error: [{exc_type}]{exc_value}\n{traceback}')
            print(traceback)
            return False
        return True


    def start_and_join(self) -> None:
        logger.info("[ShortCommentCrawler] start and join")
        logger.debug("start get reviews num tasks")
        self.read_top250_file()
        self.crawler_add_tasks(self.get_reviews_num_tasks)
        self.start()
        self.async_scheduler.join()

        logger.debug("start get short comment tasks")
        self.generate_short_comment_tasks()
        self.crawler_add_tasks(self.movie_reviews_tasks)
        self.start()
        self.async_scheduler.join()

        logger.info("finish all short comment tasks!")


    def read_top250_file(self, filepath:str=None) -> None:
        logger.debug(f"read top250 file from filepath={filepath}")
        with open(filepath if filepath else os.path.join(self.top250_path, self.top250_txt_name), 
                  'r', encoding='utf-8') as f_obj:
            for line in f_obj:
                dict_line = json.loads(line.strip())
                self.top250_id_list.append(dict_line)
        
        logger.debug("create get long comment ids tasks")
        self.get_reviews_num_tasks = [(f'https://movie.douban.com/subject/{_["id"]}/comments?start=0&limit=1&status=P&sort=new_score', 
                                       partial(self.movie_page_handler, title=_["title"], id=_["id"])) 
                                      for _ in self.top250_id_list]
        logger.debug("process top250 file success")


    def generate_short_comment_tasks(self) -> None:
        logger.debug(f"generate short comment tasks, movie num={len(self.movie_review_num)}")
        self.movie_reviews_tasks = []
        for info in self.movie_review_num:
            tasks = [(f'https://movie.douban.com/subject/{info["id"]}/comments?start={_ * self.one_page_review_num}&limit={self.one_page_review_num}&status=P&sort=new_score', 
                      partial(self.short_comment_handler, title=info["title"], id=info["id"])) 
                     for _ in range(1, min(self.max_comment_page_num, -(-int(info["comment_num"]) // self.one_page_review_num)))]
            self.movie_reviews_tasks += tasks
        logger.debug(f"generate short comment tasks")

    
    def movie_page_handler(self, resp:str, title:str=None, id:str=None) -> None:
        logger.debug("enter movie page handler")

        soup = bs(resp, 'html.parser')
        watched_span = soup.find('span', string=lambda text: '看过' in text)
        watched_count = 0
        if watched_span:
            watched_count = int(''.join(filter(str.isdigit, watched_span.get_text())))

        with self.lock:
            self.movie_review_num += [{"title": title, "id": id, "comment_num": watched_count}]

        logger.debug("movie page handler success")

    
    def short_comment_handler(self, resp:str, title:str=None, id:str=None) -> None:
        logger.debug("enter short comment handler")
        soup = bs(resp, 'html.parser')
        comments_and_ratings = []
        for comment_section in soup.find_all('div', class_='comment-item'):
            star_class = comment_section.find('span', class_=lambda x: x and x.startswith('allstar'))
            complete_numeric_rating = None
            if star_class:
                class_name = star_class.get('class')
                complete_numeric_rating = ' '.join(class_name)

            star_rating = comment_section.find('span', class_='rating')
            textual_rating = star_rating['title'] if star_rating and 'title' in star_rating.attrs else None
            
            comment = comment_section.find('p', class_='comment-content')
            comment_text = comment.get_text(strip=True) if comment else ''

            comments_and_ratings.append({"title": title, "id":id, 
                                         "comment_text": comment_text, 
                                         "textual_rating": textual_rating, "complete_numeric_rating": complete_numeric_rating})
            
        with self.lock:
            self.short_comment_results += comments_and_ratings

        logger.debug("short comment handler success")

    
    def save_short_comments(self, path:str=None) -> None:
        logger.info(f"save short comments at {path if path else self.save_path}, num={len(self.short_comment_results)}")
        os.makedirs(self.save_path, exist_ok=True)
        with self.lock:
            for i in self.short_comment_results:
                with open(os.path.join(self.save_path, f"{i['title']}_{i['id']}.txt"), 'a') as f_obj:
                    json.dump(i, f_obj, ensure_ascii=False, indent=4)
                    f_obj.write('\n')
        logger.info("save short comments success!")



if __name__ == "__main__":
    logger.setLevel(DEBUG)
    with AsyncScheduler() as scheduler:
        scheduler.start()
        crawler = ShortCommentCrawler(scheduler)
        crawler.start_and_join()
        crawler.save_short_comments()
    print("Done!")
