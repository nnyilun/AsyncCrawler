# 一个使用了异步和线程池的爬虫

* 爬取的内容为豆瓣电影top250，及电影的长评和短评。仅做学习使用。

* 可以使用代理，仅需实现ProxyPool中的函数接口

* 有日志输出，方便线程池和异步相关调试

## AsyncScheduler

核心调度器，包含了一个线程池，线程数量由config.json配置。

可以通过Queue动态添加任务。

每个线程上运行了协程aiohttp.ClientSession()，可以自动从Queue中获取任务并发送请求。

aiohttp.ClientSession()的异常会被捕获并通过logger输出。优点是不影响其余爬虫任务，缺点是不能马上因错误及时中断。

## CrawlerBase

爬虫基类，可以使用AsyncScheduler，并有基础的保存结果、保存访问失败的url的功能。

### Top250Crawler

负责爬取豆瓣电影top250的数据，得到需要获取的所有电影id。

### LongCommentCrawler

爬取电影长评，可以读取Top250Crawler保存的文件，并生成对应的长评任务。

格式化保存长评。

### ShortCommentCrawler

爬取电影短评，可以读取Top250Crawler保存的文件，并生成对应的短评任务。

格式化保存短评。

## logger

按格式输出日志。

## ProxyPool

线程池，在这里使用的是“快代理”提供的代理服务。实际效果并不如不使用代理，异步使用代理比传统的requests.get()使用代理不稳定，可能是调度器存在问题。

也可以使用其他代理，只要实现了ProxyPool中的所有函数接口，即可被AsyncScheduler调用。