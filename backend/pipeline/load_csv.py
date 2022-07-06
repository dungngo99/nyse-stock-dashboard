import json
import logging
from datetime import datetime

import backend.config as config
import pandas as pd

logging.basicConfig(filename=config.postgres_log_path, level=logging.INFO)


def load_s3(key_path, bucket_name):
    with open(key_path, "r") as file:
        lines = file.readlines()

    res = []
    for key in lines:
        response = config.s3_client.get_object(Bucket=bucket_name, Key=key.strip("\n"))
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", -1)

        if status == 200:
            res.append(json.load(response["Body"]))
            logging.info(f"Loaded key from s3 at s3://{bucket_name}/{key_path}")
    return res


def create_indicators(responses):
    df = pd.DataFrame()
    for response in responses:
        if (
            response is None
            or response["chart"] is None
            or response["chart"]["result"] is None
        ):
            continue

        response = response["chart"]["result"][0]
        metadata = response["meta"]
        timestamps = response["timestamp"]
        indicators = response["indicators"]["quote"][0]

        table = {
            "Timestamps": timestamps,
            "Volume": indicators["volume"],
            "Low": indicators["low"],
            "Open": indicators["open"],
            "High": indicators["high"],
            "Close": indicators["close"],
        }

        tss = pd.Series(timestamps).apply(
            lambda x: datetime.fromtimestamp(int(x)).strftime("%Y-%m-%d|%H:%M:%S")
        )

        df_indicators = pd.DataFrame(table)
        df_indicators["Datetime"] = tss
        df_indicators["symbol"] = metadata["symbol"]
        df = pd.concat([df, df_indicators])

    return df


def create_metadata(responses):
    def get_trading_period(trade_period):
        start_period = 0
        end_period = 0

        for period in trade_period.values():
            start = datetime.fromtimestamp(int(period[0][0]["start"])).strftime(
                "%H:%M:%S"
            )
            end = datetime.fromtimestamp(int(period[0][0]["end"])).strftime("%H:%M:%S")

            start_period = start if start_period == 0 else min(start_period, start)
            end_period = end if end_period == 0 else max(end_period, end)

        date = f"{start_period}|{end_period}"
        return date

    df = pd.DataFrame()
    for response in responses:
        if (
            response is None
            or response["chart"] is None
            or response["chart"]["result"] is None
        ):
            continue

        response = response["chart"]["result"][0]
        metadata = response["meta"]
        timestamps = response["timestamp"]

        tss = pd.Series(timestamps).apply(
            lambda x: datetime.fromtimestamp(int(x)).strftime("%Y-%m-%d|%H:%M:%S")
        )

        metadata = {
            "currency": metadata["currency"],
            "symbol": metadata["symbol"],
            "type": metadata["instrumentType"],
            "first_trade_date": datetime.fromtimestamp(
                metadata["firstTradeDate"]
            ).strftime("%Y-%m-%d|%H:%M:%S"),
            "exchange_timezone": metadata["exchangeTimezoneName"],
            "trade_period": get_trading_period(metadata["tradingPeriods"]),
            "start_trading_datetime": tss.min(),
        }
        metadata = pd.DataFrame(metadata, index=[0])
        df = pd.concat([df, metadata])

    logging.info(f"Created metadata dataframe from s3's json files")
    return df


def create_trending(responses):
    df = pd.DataFrame()

    for response in responses:
        if response is None:
            continue

        quotes = response["finance"]["result"][0]["quotes"]
        trending = {
            "region": [],
            "market_change_percent": [],
            "market_price": [],
            "symbol": [],
        }

        for quote in quotes:
            trending["region"].append(
                quote.get("region", "?").replace(",", "-").replace(" ", "-")
            )
            trending["market_change_percent"].append(
                quote.get("regularMarketChangePercent", -1)
            )
            trending["market_price"].append(quote.get("regularMarketPrice", -1))
            trending["symbol"].append(
                quote.get("symbol", "?").replace(",", "-").replace(" ", "-")
            )

        trending = pd.DataFrame(trending)
        df = pd.concat([df, trending])

    logging.info(f"Created trending dataframe from s3's json files")
    return df


def create_profile(responses):
    df = pd.DataFrame()

    for response in responses:
        if response is None:
            continue

        price = response["price"]
        summaryDetail = response["summaryDetail"]
        assetProfile = response["assetProfile"]

        profile = {"symbol": response["symbol"]}
        profile["exchange_name"] = (
            price["exchangeName"].replace(" ", "-").replace(",", "-")
        )
        profile["market_time"] = datetime.fromtimestamp(
            price["regularMarketTime"]
        ).strftime("%Y-%m-%d|%H:%M:%S")
        profile["name"] = price["shortName"].replace(" ", "-").replace(",", "-")
        profile["market_cap"] = price["marketCap"].get("fmt", -1)

        profile["beta"] = summaryDetail["beta"].get("fmt", -1)
        profile["yield"] = summaryDetail["yield"].get("fmt", -1)
        profile["dividend_rate"] = summaryDetail["dividendRate"].get("fmt", -1)
        profile["strike_price"] = summaryDetail["strikePrice"].get("fmt", -1)
        profile["ask"] = summaryDetail["ask"].get("fmt", -1)

        profile["sector"] = assetProfile["sector"].replace(" ", "-").replace(",", "-")
        profile["fulltime_employees"] = int(assetProfile.get("fullTimeEmployees", 0))
        profile["city"] = assetProfile["city"].replace(" ", "-").replace(",", "-")
        profile["country"] = assetProfile["country"].replace(" ", "-").replace(",", "-")
        profile["website"] = assetProfile["website"]
        profile["industry"] = (
            assetProfile["industry"].replace(" ", "-").replace(",", "-")
        )

        profile = pd.DataFrame(profile, index=[0])
        df = pd.concat([df, profile])

    logging.info(f"Created profile dataframe from s3's json files")
    return df


def create_news(responses):
    df = pd.DataFrame()
    news_df = {
        "id": [],
        "contentType": [],
        "title": [],
        "pubDate": [],
        "thumbnailUrl": [],
        "thumbnailWidth": [],
        "thumbnailHeight": [],
        "thumbailTag": [],
        "Url": [],
        "provider": [],
    }

    def resolutions(thumbnail):
        if thumbnail == None:
            return {"url": "", "width": 0, "height": 0, "tag": ""}

        group = []
        for reso in thumbnail["resolutions"]:
            url = reso["url"]
            w = reso["width"]
            h = reso["height"]
            tag = reso["tag"]
            group.append(((w, h), {"url": url, "width": w, "height": h, "tag": tag}))

        sort = sorted(group, key=lambda x: x[0])
        return sort[0][1]

    for response in responses:
        if response is None:
            continue

        news = response["data"]["main"]["stream"]

        for info in news:
            content = info["content"]
            thumbnail = resolutions(content["thumbnail"])

            news_df["id"].append(info["id"])
            news_df["contentType"].append(content["contentType"])
            news_df["title"].append(content["title"])
            news_df["pubDate"].append(content["pubDate"])
            news_df["thumbnailUrl"].append(thumbnail["url"])
            news_df["thumbnailWidth"].append(thumbnail["width"])
            news_df["thumbnailHeight"].append(thumbnail["height"])
            news_df["thumbailTag"].append(thumbnail["tag"])
            news_df["Url"].append(
                content["clickThroughUrl"]["url"]
                if content["clickThroughUrl"] != None
                else ""
            )
            news_df["provider"].append(content["provider"]["displayName"])

        news_df = pd.DataFrame(news_df)
        df = pd.concat([df, news_df])

    logging.info(f"Created news dataframe from s3's json files")
    return df


def load_csv():
    charts = load_s3(config.charts_txt_path, config.bucket_name)
    news = load_s3(config.news_txt_path, config.news_bucket_name)
    profile = load_s3(config.profile_txt_path, config.profile_bucket_name)
    trending = load_s3(config.trending_txt_path, config.trending_bucket_name)

    indicators = create_indicators(charts)
    metadata = create_metadata(charts)
    trending = create_trending(trending)
    news = create_news(news)
    profile = create_profile(profile)

    df = pd.merge(trending, metadata, on="symbol", how="outer")
    profile = pd.merge(df, profile, on="symbol", how="outer")
    logging.info(f"Merged metadata, profile, and trending tables together")

    profile.to_csv(config.profile_csv_path, index=False, header=False)
    news.to_csv(config.news_csv_path, index=False, header=False)
    indicators.to_csv(config.indicators_csv_path, index=False, header=False)
    logging.info(f"Save dataframes to csv files")


if __name__ == "__main__":
    load_csv()
