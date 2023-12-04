from proxy_pool import ProxyPool
from async_scheduler import AsyncScheduler
from top250_crawler import Top250Crawler
from long_comment_crawler import LongCommentCrawler
from short_comment_crawler import ShortCommentCrawler

def main():
    with ProxyPool() as proxy_pool:
        proxy_pool.update_all_proxies()

        with AsyncScheduler(proxy_pool) as scheduler:
            scheduler.start()
            
            with Top250Crawler(scheduler) as crawler:
                crawler.start()

            with ShortCommentCrawler(scheduler) as crawler:
                crawler.start_and_join()

            with LongCommentCrawler(scheduler) as crawler:
                crawler.start_and_join()

    print("Done!")


if __name__ == "__main__":
    main()