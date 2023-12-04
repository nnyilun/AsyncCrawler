from __future__ import annotations
import asyncio
import threading
import functools
from queue import Queue
from types import TracebackType
from typing import Callable, Type, Optional
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import aiofiles
from tqdm import tqdm
from fake_useragent import UserAgent
from config import config
from proxy_pool import ProxyPool
from logger import logger, INFO


class AsyncScheduler:
    def __init__(self, proxy_pool:ProxyPool=None) -> None:
        logger.info('init async scheduler...')
        logger.debug('get scheduler config')
        self.scheduler_config = config.get("scheduler")
        self.num_threads = self.scheduler_config["num_threads"]
        self.max_retries = self.scheduler_config["max_retries"]
        self.timeout = self.scheduler_config["timeout"]
        self.failed_urls_path = self.scheduler_config["failed_urls_path"]

        logger.debug('set proxy pool and user agent')
        self.proxy_pool = proxy_pool
        self.proxy_auth = proxy_pool.get_proxy_auth if proxy_pool else None
        self.user_agent = UserAgent()
        
        logger.debug('create progress_bar')
        self.process_lock = threading.Lock()
        self.progress_bar = tqdm(total=0)

        logger.debug('create queue and executor')
        self.tasks_queue = Queue()
        self.executor = ThreadPoolExecutor(max_workers=self.num_threads)
        self.running = True
        logger.info('init async scheduler finish!')


    @staticmethod
    def get_function_name(func):
        if isinstance(func, functools.partial):
            return func.func.__name__
        else:
            return func.__name__

    
    def get_remaining_tasks_count(self) -> int:
        return self.tasks_queue.qsize()
    

    def queue_empty(self) -> bool:
        return self.tasks_queue.empty()
    

    async def save_failed_url(self, url:str, resp_handler:Callable[[str], None]=None) -> None:
        async with aiofiles.open(self.failed_urls_path, mode='a') as file:
            await file.write(f'url={str(url)}, resp_handler=[{AsyncScheduler.get_function_name(resp_handler)}]{resp_handler}\n')


    async def fetch(self, session:aiohttp.ClientSession, url:str, resp_handler:Callable[[str], None]=None) -> str | None:
        # async with aiohttp.ClientSession() as session:
        for _ in range(self.max_retries):
            try:
                logger.debug(f"url={url} start request, i={_}")
                proxy = self.proxy_pool.get_one_proxy() if self.proxy_pool else None
                headers = {
                    'User-Agent': self.user_agent.random
                }
                async with session.get(
                                        url=url, headers=headers,
                                        proxy=f'http://{proxy}/' if proxy else None, proxy_auth=self.proxy_auth,
                                        timeout=self.timeout
                                        ) as response:
                    resp = await response.read()
                    if response.status == 200:
                        result = resp.decode('utf-8')
                        if resp_handler:
                            logger.debug(f"handler={resp_handler} url={url} success!")
                            resp_handler(result)
                        
                        self.progress_bar.update(1)
                        logger.debug(f"url={url} success!")
                        return result
                    
                    raise Exception(f"response error! status={response.status}")
                
            except Exception as e:
                logger.error(f"exception: {e}, url: {url}", exc_info=True, stack_info=True)
                if self.proxy_pool:
                    self.proxy_pool.proxy_error_cnt(proxy_ip=proxy)
                await asyncio.sleep(1)
            finally:
                pass

        await self.save_failed_url(url=url, resp_handler=resp_handler)
        self.progress_bar.update(1)
        logger.error(f"url={url} attempts exceeded the limit!")
        return None


    async def worker(self, loop:asyncio.AbstractEventLoop) -> None:
        async with aiohttp.ClientSession() as session:
            while True:
                task = await loop.run_in_executor(None, self.tasks_queue.get)
                logger.debug(f"get task={task} from queue")
                if task == (None, None):
                    logger.debug(f"task is None, break running loop")
                    self.tasks_queue.task_done()
                    break
                url, response_handler = task
                await self.fetch(session, url, response_handler)
                self.tasks_queue.task_done()

        logger.debug(f"exit worker")


    def start_worker(self) -> None:
        logger.debug(f"start worker")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.worker(loop))


    def start(self) -> None:
        logger.info(f"start scheduler")
        for _ in range(self.num_threads):
            logger.debug(f"executor submit {_}")
            self.executor.submit(self.start_worker)


    def add_task(self, url:str, resp_handler:Callable[[str], None]=None) -> None:
        logger.debug(f"add task task:url={url}, resp_handler=[{AsyncScheduler.get_function_name(resp_handler)}]{resp_handler}")
        self.tasks_queue.put((url, resp_handler))
        self.progress_bar.total += 1


    def add_tasks(self, tasks:list[tuple[str, Callable[[str], None] | None]]) -> None:
        for task in tasks:
            self.add_task(*task)

    
    def reset_progress_bar(self) -> None:
        logger.debug('resetting progress bar...')
        self.progress_bar.close()
        self.progress_bar = tqdm(total=0)
        logger.debug('progress bar reset successfully')


    def running_status(self) -> bool:
        return self.running

        
    def join(self) -> None:
        self.tasks_queue.join()


    def stop(self) -> None:
        logger.info(f"stop scheduler...")
        self.running = False
        logger.debug(f"send None to threads")
        for _ in range(self.num_threads):
            self.tasks_queue.put((None, None))
        
        logger.debug(f"wait for all tasks to be completed")
        self.tasks_queue.join()
        self.progress_bar.close()
        self.executor.shutdown(wait=True)
        logger.info(f"stop scheduler success!")


    def __enter__(self) -> AsyncScheduler:
        return self


    def __exit__(self, exc_type: Type[Optional[BaseException]], exc_value: Optional[BaseException], traceback: Optional[TracebackType]) -> bool:
        self.stop()
        if traceback:
            logger.error(f'exit error: [{exc_type}]{exc_value}\n{traceback}')
            print(traceback)
            return False
        return True



if __name__ == "__main__":
    logger.setLevel(INFO)
    results_lock = threading.Lock()
    results = []
    def resp_handler(result):
        with results_lock:
            results.append(result)

    with AsyncScheduler() as scheduler:
        scheduler.start()

        tasks = [('http://www.baidu.com/', resp_handler)] * 16
        scheduler.add_tasks(tasks)

        tasks = [('http://www.bing.com/', resp_handler)] * 10
        scheduler.add_tasks(tasks)

        scheduler.join()

    print(len(results))
    print('stop')
