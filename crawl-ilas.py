import asyncio
import re
import traceback

from bs4 import BeautifulSoup
from httpx import AsyncClient, HTTPStatusError, TimeoutException, ReadTimeout, ConnectTimeout
import motor.motor_asyncio
from pymongo.errors import DuplicateKeyError
from aiocrawl import AsyncCoroutineFramework

from utils import _get_logger

logging = _get_logger('ILAS')


class RecnoFinder:
    def __init__(self, base_url='http://221.233.125.126:8097/ILASOPAC'):
        self.base_url = base_url
        self.client: AsyncClient

    async def check_recno_exists(self, recno) -> bool:
        url = f'/GetholdingShow.do?recno={recno}'
        retries = 0
        max_retries = 3
        while retries < max_retries:
            try:
                req = await self.client.get(url)
                if req.status_code != 200:
                    req.raise_for_status()
                break
            except (TimeoutException, HTTPStatusError):
                retries += 1
                await asyncio.sleep(10)
        else:
            return False

        pattern = r'<td>([^<>]*?)</td>'
        matches = re.findall(pattern, req.text)
        return len(matches) > 0

    async def guess_recno_range(self, recno_start, recno_end, recno_step=1):
        recno = 0
        for i in range(recno_start, recno_end, recno_step):
            recno = i
            result = await self.check_recno_exists(recno)
            print(recno, result, recno_step, self.client.base_url)
            if result:
                return recno
            await asyncio.sleep(1)
        return recno + abs(recno_step)

    async def find_recno(self):
        recno_start = 0
        recno_end = 10000000
        recno_step = [10000, 1000, 100, 10, 1]
        _recno_start = 0
        _recno_end = 0

        async with AsyncClient(timeout=2, base_url=self.base_url) as self.client:
            # 查找 recno_start
            for step in recno_step:
                recno_end = await self.guess_recno_range(recno_start, recno_end, step)
                recno_start = recno_end - step
            else:
                _recno_start = recno_end

            # 查找 recno_end
            recno_start = 1000000
            recno_end = 0
            recno_step = [-100000, -10000, -1000, -100, -10, -1]
            for step in recno_step:
                recno_end = await self.guess_recno_range(recno_start, recno_end, step)
                recno_start = recno_end + abs(step)
            else:
                _recno_end = recno_end

        return _recno_start, _recno_end


async def http_request(client: AsyncClient, method, url, content=None, data=None):
    for _ in range(5):
        try:
            r = await client.request(method=method, url=url, content=content, data=data)
            return r
        except ConnectTimeout:
            await asyncio.sleep(10)
            print('httpx.ConnectTimeout', _)
        except ReadTimeout:
            await asyncio.sleep(10)
            print('httpx.ReadTimeout', _)
        except TimeoutError:
            await asyncio.sleep(10)
            print('TimeoutError', _)
        except Exception:
            await asyncio.sleep(10)
            print(traceback.format_exc(), _)


async def coroutine(name, delay=None, base_url='http://221.233.125.126:8097/ILASOPAC'):
    logging.info(f"Coroutine {name} start.")
    doc_library = await db.libraries.find_one({'base_url': base_url})
    coll = db[name]
    recnos = {
        'start': 0,
        'end': 0,
        'current': 0
    }
    if doc_library is None:
        res = await RecnoFinder(base_url).find_recno()
        recnos['current'] = res[0] - 1
        recnos['start'] = res[0]
        recnos['end'] = res[1]
        await db.libraries.insert_one({'name': name, 'base_url': base_url, 'recnos': recnos})
        await coll.create_index("info.recno", unique=True)

    doc_library = await db.libraries.find_one({'base_url': base_url})
    recnos = doc_library['recnos']  # 可能不会更新recnos
    if delay:
        logging.info(f"Coroutine {name} started with delay: {delay} seconds")
        await asyncio.sleep(delay)

    for recno in range(recnos['current'] + 1, recnos['end'] + 1):

        async with AsyncClient(proxies=None, timeout=3, verify=False) as client:
            # await asyncio.sleep(0.5)
            req = await http_request(client, 'get', f'{base_url}/GetholdingShow.do?recno={recno}')
            # req = await client.get(f'{base_url}/GetholdingShow.do?recno={recno}')

            # time = datetime.datetime.now() , 'created_at': time,'updated_at': time
            book_data = {'info': {'recno': recno},
                         'inventorys': [],
                         'locations': {}}
            if req.status_code == 200:
                soup = BeautifulSoup(req.text, 'html.parser')
                td_tags = soup.find_all('td')
                th_tags = soup.find_all('th')

                a = {'索书号': 'index_number', '当前所在地点': 'current_location', '卷册说明': '',
                     '当前所在馆': 'secondary_location', '流通类别': 'circulation_type'}
                b = {'条码号': 'barcode', '索书号': 'index_number', '当前所在地点': 'current_location', '馆藏状态': 'status'}
                if len(th_tags) <= 0:
                    raise ValueError('len(th_tags) > 0', client.base_url, recno)

                for k, td in enumerate(td_tags):
                    i = int(k % (len(th_tags) / 2))
                    th = th_tags[i].text.strip()
                    td = td.text.strip()
                    if th in a.keys():
                        value = a[th]
                        if value:
                            book_data['locations'].update({a[th]: td})

                book_inventory = {'barcode': '', 'status': ''}
                for k, td in enumerate(td_tags):
                    i = int(k % (len(th_tags) / 2))
                    th = th_tags[i].text.strip()
                    td = td.text.strip()
                    if th in b.keys():
                        book_inventory.update({b[th]: td})
                        book_data['inventorys'].append(book_inventory)

                # req = await client.get(f'{base_url}/GetBibInfoShow.do?recno={recno}')
                req = await http_request(client, 'get', f'{base_url}/GetBibInfoShow.do?recno={recno}')
                if req.status_code == 200:
                    pattern = r'<div class="item">\s*题名 ：(.*?)\s*作者：(.*?)\s*出版社：(.*?)\s*出版日期：(.*?)\s*</div>\s*<div class="item">\s*出版地：(.*?)\s*尺寸：(.*?)\s*(ISBN/ISSN|ISBN)：(.*?)\s*</div>\s*<div class="item">\s*分类号：(.*?)\s*主题词：(.*?)\s*丛书：(.*?)\s*</div>\s*<div class="item">\s*索书号：(.*?)\s*页码：(.*?)\s*价格：(.*?)\s*</div>'
                    matchObjs = re.findall(pattern, req.text)
                    book_data['info'].update(
                        {'title': matchObjs[0][0], 'author': matchObjs[0][1], 'publisher': matchObjs[0][2], 'publication_date': matchObjs[0][3],
                         'publication_location': matchObjs[0][4], 'size': matchObjs[0][5], 'isbn': matchObjs[0][6], 'category': matchObjs[0][7],
                         'subject': matchObjs[0][8],
                         'series': matchObjs[0][9], 'callno': matchObjs[0][10], 'page_count': matchObjs[0][11], 'price': matchObjs[0][12]})
                    # print(name, book_data['locations'])
                try:
                    res = await coll.insert_one(book_data)
                    print('result %s' % repr(res.inserted_id), book_data['locations'])
                except DuplicateKeyError:
                    print('主键重复', book_data['info'])

        recnos['current'] = recno
        # await db.libraries.update_one({'base_url': base_url}, {'recnos': recnos})
        # recnos['current'] += 1
        # await db.libraries.replace_one({'base_url': base_url}, doc_library)
        await db.libraries.update_one({'base_url': base_url}, {'$set': {'recnos': recnos}})
    logging.info(f"Coroutine {name} finished.")


async def main(libraries_urls: list = None):
    # coroutines = [
    #     ("ILAS wlxy", coroutine, ['ILAS wlxy'], {'base_url': 'http://221.233.125.126:8097/ILASOPAC'}),
    #     ("ILAS hbsf", coroutine, ['ILAS hbsf'], {'base_url': 'https://opac.hbnu.edu.cn/ILASOPAC'}),
    #     ("ILAS wlxy", coroutine, ['ILAS wlxy'], {'base_url': 'http://218.65.20.51:8081'}),
    # ]

    coroutines = [
        (f"ILAS {k}", coroutine, [f"ILAS {k}"], {'base_url': v}) for k, v in enumerate(libraries_urls)
    ]
    await framework.run(coroutines)


if __name__ == '__main__':
    framework = AsyncCoroutineFramework(max_coroutines=4, timeout=None)
    db = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)['ilas']
    urls = ['http://221.233.125.126:8097/ILASOPAC', 'http://218.65.20.51:8081']
    # urls = ['http://221.233.125.126:8097/ILASOPAC']
    asyncio.run(main(urls))
