import json
import re

import cloudscraper

scraper = cloudscraper.create_scraper()
import pandas as pd
from aiolimiter import AsyncLimiter
from fake_useragent import UserAgent

import ml_scheduler

limiter = AsyncLimiter(2, time_period=1)
ins_id_regex = re.compile(r'"instrumentId":"(\d+)"')
data_base_url = "https://api.investing.com/api/financialdata/historical/{ins_id}?start-date=2000-01-01&end-date=2024-06-28&time-frame=Monthly&add-missing-rows=false"
ua = UserAgent()

headers = {}


@ml_scheduler.exp_func
async def crawl(exp: ml_scheduler.Exp, url, ins_id=None):

    if ins_id is None or str(ins_id) == 'nan':
        async with limiter:
            text = scraper.get(url,
                               headers={
                                   'User-Agent': ua.chrome,
                                   **headers
                               }).text

        ins_id = ins_id_regex.search(text).group(1)
        await exp.report(ins_id=ins_id)

    ins_id = int(float(ins_id))
    data_url = data_base_url.format(ins_id=ins_id)
    print(data_url)

    async with limiter:
        text = scraper.get(data_base_url.format(ins_id=ins_id),
                           headers={
                               'User-Agent': ua.chrome,
                               **headers
                           }).text
        try:
            data = json.loads(text)['data']
        except json.JSONDecodeError as e:
            print(text)
            raise e

    print(data)
    df = pd.DataFrame(data)

    records = df[df['rowDate'].str.contains(r'Jan')].to_dict('records')

    # group by year
    agg_vol = df.groupby(
        df['rowDate'].str.split('-').str[0])['volumeRaw'].sum().tolist()

    results = {}
    for record, vol in zip(records, agg_vol):
        year = record['rowDate'].split(' ')[-1]
        results[year + ' Open'] = record['last_openRaw']
        results[year + ' Vol'] = vol

    await exp.report(**results)


crawl.run_sqlite(
    'crawldb.sqlite',
    "urls",
    ['ins_id', '2024 Open'],
)
