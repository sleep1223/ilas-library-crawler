import asyncio
import time
from asyncio import Future, CancelledError

from utils import _get_logger

# 设置日志输出配置
logging = _get_logger('Simpyder')


class AsyncCoroutineFramework:
    def __init__(self, max_coroutines, timeout):
        self.max_coroutines = max_coroutines  # 最大协程数
        self.timeout = timeout  # 超时时间
        self.coroutine_counter = 0  # 协程计数器
        # self.total_execution_time = 0  # 总协程执行时间
        self.total_time = 0  # 总执行时间
        self.future: Future[list] = None

    async def execute_coroutine(self, coro_name, coro_function, *args, **kwargs):
        start_time = time.time()  # 记录单个协程开始时间

        # 检查协程数是否已达到最大限制
        if self.coroutine_counter >= self.max_coroutines:
            # logging.warning(f"Reached maximum coroutine limit ({self.max_coroutines}). Waiting for available slots...")
            while self.coroutine_counter >= self.max_coroutines:
                await asyncio.sleep(0.5)

        self.coroutine_counter += 1  # 增加协程计数器
        try:
            # 执行协程函数，并传入参数
            await asyncio.wait_for(coro_function(*args, **kwargs), timeout=self.timeout)
        except asyncio.TimeoutError:
            logging.error(f"Coroutine {coro_name} timed out after {self.timeout} seconds.")
        finally:
            self.coroutine_counter -= 1  # 减少协程计数器
            execution_time = time.time() - start_time  # 计算单个协程执行时间
            # self.total_execution_time += execution_time  # 累加总协程执行时间

            logging.info(f"Coroutine {coro_name} executed in {execution_time:.2f} seconds. ")

    async def run(self, coroutines):
        start_time = time.time()
        # await asyncio.gather(*[self.execute_coroutine(name, coro, *args, **kwargs) for name, coro, args, kwargs in coroutines])

        self.future = asyncio.gather(*[self.execute_coroutine(coroutine[0], coroutine[1], *([] if len(coroutine) < 3 else coroutine[2]),
                                                              **({} if len(coroutine) < 4 else coroutine[3])) for coroutine in coroutines])
        try:
            await self.future
        except CancelledError:
            logging.error('协程池退出')
        logging.critical("Simpyder任务执行完毕")
        self.total_time = time.time() - start_time
        logging.critical(f'累计消耗时间：{self.total_time:.2f} s')
        logging.critical(f'累计协程数量：{len(coroutines)}')
        # logging.critical('累计爬取链接：% s' % str(self._url_count))
        # logging.critical('累计生成对象：% s' % str(self._item_count))
        self.exit()

    def exit(self):
        self.future.cancel()

    # def __del__(self):
    #     self.exit()


async def coroutine(name, delay=1, message=''):
    logging.info(f"Coroutine {name} started with delay: {delay} seconds")
    await asyncio.sleep(delay)
    if message:
        logging.info(f"Coroutine {name} send message: {message}")
        framework.exit()
    logging.info(f"Coroutine {name} finished.")


if __name__ == '__main__':
    framework = AsyncCoroutineFramework(max_coroutines=2, timeout=None)
    coroutines = [
        ("Coroutine 1", coroutine, ['1']),
        ("Coroutine 2", coroutine, ["Test", 1], {'message': 'Hello World'}),
        ("Coroutine 3", coroutine, ["Delayed", 3]),
    ]

    asyncio.run(framework.run(coroutines))
