from __future__ import annotations
import requests
import random
from types import TracebackType
from typing import Type, Optional
import aiohttp
from config import config
from logger import logger


class ProxyPool:
    '''If you are using another proxy pool api, you will need to refactor the intra-class functions'''
    
    def __init__(self) -> None:
        logger.info('init proxy pool...')
        logger.debug('get proxy pool config')
        self.proxy_api = config.get("proxy_api")
        self.proxy_api_ip = self.proxy_api["proxy_api_ip"]
        self.__secret_Id = self.proxy_api["SecretId"]
        self.__signature = self.proxy_api["Signature"]

        self.proxy_num = self.proxy_api["proxy_num"]
        self.proxy_max_errors = self.proxy_api["proxy_max_errors"]

        logger.debug('set proxy auth')
        self.__username = self.proxy_api["username"]
        self.__password = self.proxy_api["password"]
        self.__proxy_auth = aiohttp.BasicAuth(self.__username, self.__password)

        logger.debug('create proxy pool list')
        self.all_proxies = []
        self.proxies_cnt = {}
        logger.info('init proxy pool finish!')


    def __enter__(self) -> ProxyPool:
        return self


    def __exit__(self, exc_type: Type[Optional[BaseException]], exc_value: Optional[BaseException], traceback: Optional[TracebackType]) -> bool:
        if traceback:
            logger.error(f'exit error: [{exc_type}]{exc_value}\n{traceback}')
            print(traceback)
            return False
        return True
    

    def update_all_proxies(self) -> None:
        logger.info('update all proxies...')
        self.all_proxies = requests.get(
                f'{self.proxy_api_ip}/?secret_id={self.__secret_Id}&signature={self.__signature}&num={self.proxy_num}&format=json'
            ).json().get('data').get('proxy_list')
        assert len(self.all_proxies) != 0, "get proxies failed!"
        logger.info(f'update num={len(self.all_proxies)} proxies')


    def update_one_proxy(self) -> None:
        logger.debug('update one proxy')
        proxy = requests.get(
                f'{self.proxy_api_ip}/?secret_id={self.SecretId}&signature={self.Signature}&num=1&format=json'
            ).json().get('data').get('proxy_list')[0]
        self.all_proxies.append(proxy)

    
    def proxy_error_cnt(self, proxy_ip:str) -> bool:
        logger.debug(f'proxy error cnt: proxy_ip={proxy_ip}')
        if proxy_ip in self.proxies_cnt:
            self.proxies_cnt[proxy_ip] += 1
        else:
            self.proxies_cnt[proxy_ip] = 1

        logger.debug(f'proxy={proxy_ip}, proxies_cnt: {self.proxies_cnt[proxy_ip]}')

        if self.proxies_cnt[proxy_ip] >= self.proxy_max_errors:
            self.all_proxies.remove(proxy_ip)
            self.proxies_cnt.pop(proxy_ip)
            self.update_one_proxy()
            logger.warning(f"proxy '{proxy_ip}' failed too many times, and re-acquired a new proxy!")


    def get_one_proxy(self) -> str:
        logger.debug('get one proxy')
        assert len(self.all_proxies) != 0, "proxy list is empty!"
        return random.choice(self.all_proxies)
    
    @property
    def get_proxy_auth(self) -> aiohttp.BasicAuth:
        logger.debug('get proxy auth')
        return self.__proxy_auth
    

if __name__ == "__main__":
    pass