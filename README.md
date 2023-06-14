- 使用协程并发、HTTP 长连接、异步 MongoDB库 motor 以及 BeautifulSoup 库进行图书信息的爬取。
- 实现了多个 ILAS 网址的并发抓取，并考虑了不同版本之间的差异。
- 实现了协程进度的还原，确保数据爬取的完整性。
- 单个 ILAS 协程请求量百万，单日数据量达千万级别。

[![Build Status](https://travis-ci.org/sleep1223/school-api.svg?branch=master)](https://travis-ci.org/sleep1223/school-api)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/sleep1223/school-api/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/sleep1223/school-api/?branch=master)
[![codecov](https://codecov.io/gh/sleep1223/school-api/branch/master/graph/badge.svg)](https://codecov.io/gh/sleep1223/school-api)

## Usage

```Shell
$ pip install -r requirements.txt
$ python ./aiocrawl.py
或者
$ pdm install
pdm run ./aiocrawl.py
```

