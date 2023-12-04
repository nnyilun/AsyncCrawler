from __future__ import annotations
import os
import csv
import json
from typing import Callable
from types import TracebackType
from typing import Callable, Type, Optional
from threading import Lock
from logger import logger
from config import config
from async_scheduler import AsyncScheduler


class CrawlerBase:
    def __init__(self, async_scheduler:AsyncScheduler, tasks:list[tuple[str, Callable[[str], None] | None]]=None, 
                 save_path:str=None, file_name:str=None) -> None:
        logger.debug("set save path")
        self.save_path = save_path if save_path else config.get("crawler_base")["default_save_path"]
        self.file_name = file_name if file_name else "crawler_base"

        logger.debug("get failed urls path")
        self.failed_urls_path = config.get("crawler_base")["failed_urls_path"]
        self.failed_urls = []
        
        logger.debug("get async scheduler")
        self.async_scheduler = async_scheduler

        logger.debug("create url list and result list")
        self.tasks = tasks if tasks else []
        self.lock = Lock()
        self.results = []

    
    def __enter__(self) -> CrawlerBase:
        return self
    

    def __exit__(self, exc_type: Type[Optional[BaseException]], exc_value: Optional[BaseException], traceback: Optional[TracebackType]) -> bool:
        self.async_scheduler.join()
        if traceback:
            logger.error(f'exit error: [{exc_type}]{exc_value}\n{traceback}')
            print(traceback)
            return False
        return True


    def start(self) -> None:
        logger.info(f"[{self.__class__.__name__}]start!")
        assert self.tasks and len(self.tasks) != 0, f"[{self.__class__.__name__}]tasks is None!"
        self.async_scheduler.reset_progress_bar()
        self.async_scheduler.add_tasks(self.tasks)
        self.tasks = []

    
    def crawler_add_task(self, url:str, resp_handler:Callable[[str], None]=None) -> None:
        logger.debug(f"[{self.__class__.__name__}]crawler add task")
        self.tasks.append(url, resp_handler)
        logger.debug(f"[{self.__class__.__name__}]add task success")

    
    def crawler_add_tasks(self, tasks:list[tuple[str, Callable[[str], None] | None]]) -> None:
        logger.debug(f"[{self.__class__.__name__}]crawler add tasks")
        self.tasks += tasks
        logger.debug(f"[{self.__class__.__name__}]add {len(tasks)} tasks success")


    def get_results(self) -> list:
        logger.debug(f"get results in [{self.__class__.__name__}]")
        with self.lock:
            return self.results
        
    
    def read_failed_urls(self, path:str=None) -> None:
        file_path = path if path else self.failed_urls_path
        logger.debug(f"read failed urls from {file_path}...")
        with open(file_path, 'r') as f_obj:
            for line in f_obj:
                self.failed_urls.append(line.strip())
        logger.debug("read failed urls success")
    

    def save_results_to_txt(self, file_name:str=None) -> None:
        file_path = os.path.join(self.save_path, 
                                 file_name if file_name else self.file_name + '.txt')
        
        logger.debug(f"save [{self.__class__.__name__}] results to [txt] at path=[{file_path}]")
        
        with self.lock:
            results = self.results

        os.makedirs(self.save_path, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f_obj:
            for result in results:
                f_obj.write(f'{json.dumps(result, ensure_ascii=False)}\n')
        
        logger.debug(f"save [{self.__class__.__name__}] results to [txt] success")


    def save_results_to_csv(self, file_name:str=None) -> None:
        file_path = os.path.join(self.save_path, 
                                 file_name if file_name else self.file_name + '.csv')
        
        logger.debug(f"save [{self.__class__.__name__}] results to [csv] at path=[{file_path}]")
        
        with self.lock:
            results = self.results

        os.makedirs(self.save_path, exist_ok=True)
        with open(file_path, 'w', newline='', encoding='utf-8') as f_obj:
            writer = csv.DictWriter(f_obj, fieldnames=results[0].keys())
            writer.writeheader()
            for result in results:
                writer.writerow(result)
        
        logger.debug(f"save [{self.__class__.__name__}] results to [csv] success")