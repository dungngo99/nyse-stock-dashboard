def to_timeseries(data):
    res = {'low': [], 'open': [], 'high': [], 'close': [], 'datetime': []}
    for row in data:
        res['low'].append(row[0])
        res['open'].append(row[1])
        res['high'].append(row[2])
        res['close'].append(row[3])
        res['datetime'].append(row[4])

    return res


def to_trending(lists):
    if not (type(lists) == list):
        return {}

    res = {}
    for lst in lists:
        cur = config.conn.cursor()

        cur.execute(query.select_latest_charts.format(ticker=lst[8]))
        data = cur.fetchall()
        ts = to_timeseries(data)

        res[lst[8]] = {
            'name': lst[7],
            'exchange': lst[6],
            'price': round(lst[5]),
            'time': lst[4].strftime("%Y-%m-%d %H:%M:%S"),
            'percent': round(lst[2], 2),
            'quoteType': lst[1],
            'timeseries': ts
        }
    return res


def to_profile(profile):
    if len(profile) < 21:
        return {}

    return {
        'openPrice': profile[0],
        'exchangeName': profile[1],
        'marketTime': profile[2],
        'name': profile[3],
        'currency': profile[4],
        'marketCap': profile[5],
        'quoteType': profile[6],
        'exchangeTimezoneName': profile[7],
        'beta': profile[8],
        'yield': profile[9],
        'dividendRate': profile[10],
        'strikePrice': profile[11],
        'ask': profile[12],
        'sector': profile[13],
        'fullTimeEmployees': profile[14],
        'longBusinessSummary': profile[15],
        'city': profile[16],
        'country': profile[17],
        'website': profile[18],
        'industry': profile[19],
        'symbol': profile[20]
    }


def to_news(data):
    res = []
    for tuple in data:
        if not tuple:
            continue

        res.append({
            "id": tuple[0],
            "contentType": tuple[1],
            "title": tuple[2],
            "pubDate": tuple[3],
            "thumbnailUrl": tuple[4],
            "thumbnailWidth": tuple[5],
            "thumbnailHeight": tuple[6],
            "thumbailTag": tuple[7],
            "Url": tuple[8],
            "provider": tuple[9]
        })
    return res