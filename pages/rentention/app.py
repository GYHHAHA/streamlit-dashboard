import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
import pytz


timezone = pytz.timezone("Asia/Shanghai")

# Elasticsearch client
client = Elasticsearch(hosts=st.secrets["es"]["url"], api_key=st.secrets["es"]["key"])


# Function to get unique user IDs for a given day and message name
@st.cache_data(ttl=3600)
def get_unique_user_ids(date, message_name):
    response = client.search(
        index="monitor-prod-20*",
        body={
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": date + "T00:00:00",
                                    "lt": date + "T23:59:59",
                                    "time_zone": "+08:00",  # 设置中国时区
                                }
                            }
                        },
                        {"term": {"message.name.keyword": message_name}},
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "unique_userIds": {
                    "terms": {
                        "field": "message.userId",
                        "size": 10000,
                    }
                }
            },
        },
    )["aggregations"]["unique_userIds"]["buckets"]
    return set(u["key"] for u in response)


# Function to calculate retention rate over the last 30 days for a specific interval
def calculate_retention(interval_days):
    rentention_count = []
    retention_rates = []
    dates = []
    for i in range(1, 15):
        day = datetime.now(timezone) - timedelta(days=i + interval_days - 1)
        next_day = day + timedelta(days=interval_days)

        # Format dates
        day_str = day.strftime("%Y-%m-%d")
        next_day_str = next_day.strftime("%Y-%m-%d")
        dates.append(day_str)

        # Get user IDs for 'sign_up' on the specific day and 'root' on the day after the interval
        unique_userIds = get_unique_user_ids(day_str, "backend-sign_up")
        unique_userIds_after_interval = get_unique_user_ids(next_day_str, "root")

        rentention_count.append(
            len(unique_userIds_after_interval.intersection(unique_userIds))
        )
        # Calculate retention rate for the interval
        if unique_userIds:
            retention_rate = len(
                unique_userIds_after_interval.intersection(unique_userIds)
            ) / len(unique_userIds)
        else:
            retention_rate = 0
        retention_rates.append(retention_rate)

    # Create DataFrame for the interval data
    df = pd.DataFrame(
        {
            "Date": dates[::-1],
            "Retention Rate": retention_rates[::-1],
            "retention_count": rentention_count[::-1],
        }
    )
    return df


@st.cache_data(ttl=3600)
def search_funnel():
    end_date = datetime.now(timezone) - timedelta(days=1)
    start_date = end_date - timedelta(days=14 - 1)
    # 定义查询
    query = {
        "size": 0,  # 不返回具体文档，只返回聚合结果
        "query": {
            "bool": {
                "must": [
                    {"range": {"@timestamp": {"gte": start_date, "lte": end_date}}}
                ]
            }
        },
        "aggs": {
            "by_day": {
                "date_histogram": {
                    "field": "@timestamp",
                    "calendar_interval": "day",
                    "time_zone": "+08:00",
                },
                "aggs": {
                    "userId_all": {
                        "filter": {"range": {"message.userId": {"gte": 0}}},
                        "aggs": {
                            "unique_visitorId": {
                                "cardinality": {"field": "message.visitorId.keyword"}
                            }
                        },
                    },
                    "userId_not_0": {
                        "filter": {"range": {"message.userId": {"gt": 0}}},
                        "aggs": {
                            "unique_visitorId": {
                                "cardinality": {"field": "message.visitorId.keyword"}
                            }
                        },
                    },
                    "new_sign_up": {
                        "filter": {"term": {"message.name.keyword": "backend-sign_up"}},
                        "aggs": {
                            "unique_visitorId": {
                                "cardinality": {"field": "message.userId"}
                            }
                        },
                    },
                },
            }
        },
    }
    # 执行查询
    response = client.search(index="monitor-prod-20*", body=query)

    return response


def calculate_funnel():
    response = search_funnel()

    # 解析结果
    data = []
    for bucket in response["aggregations"]["by_day"]["buckets"]:
        date = bucket["key_as_string"]
        all_visitor_count = bucket["userId_all"]["unique_visitorId"]["value"]
        all_registered_count = bucket["userId_not_0"]["unique_visitorId"]["value"]
        all_sign_up_count = bucket["new_sign_up"]["unique_visitorId"]["value"]
        data.append(
            {
                "date": date,
                "all_visitor_count": all_visitor_count,
                "all_registered_count": all_registered_count,
                "all_sign_up_count": all_sign_up_count,
            }
        )

    df = pd.DataFrame(data)
    df["rentention_1d"] = calculate_retention(1)["retention_count"]
    df.date = pd.to_datetime(df.date)
    df.columns = [
        "date",
        "all_visitor_count",
        "all_registered_count",
        "all_sign_up_count",
        "retention_count",
    ]
    return df


# Streamlit layout for selecting retention interval
st.title("用户留存分析")

st.write("新注留存漏斗：")

df = calculate_funnel()

# 创建图表
fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(df["date"], df["all_visitor_count"], label="all_visitor_count", linewidth=1)
ax.plot(
    df["date"],
    df["all_registered_count"],
    label="all_registered_count",
    linewidth=1,
)
ax.plot(df["date"], df["all_sign_up_count"], label="all_sign_up_count", linewidth=1)
ax.plot(df["date"], df["retention_count"], label="retention_count", linewidth=1)

# 添加点和数字标注
for i in range(len(df)):
    ax.scatter(df["date"][i], df["all_visitor_count"][i], s=50, color="blue")
    ax.text(
        df["date"][i],
        df["all_visitor_count"][i] + 5,
        str(df["all_visitor_count"][i]),
        ha="center",
    )

    ax.scatter(df["date"][i], df["all_registered_count"][i], s=50, color="orange")
    ax.text(
        df["date"][i],
        df["all_registered_count"][i] + 5,
        str(df["all_registered_count"][i]),
        ha="center",
    )

    ax.scatter(df["date"][i], df["all_sign_up_count"][i], s=50, color="green")
    ax.text(
        df["date"][i],
        df["all_sign_up_count"][i] + 5,
        str(df["all_sign_up_count"][i]),
        ha="center",
    )

    ax.scatter(df["date"][i], df["retention_count"][i], s=50, color="red")
    ax.text(
        df["date"][i],
        df["retention_count"][i] + 5,
        str(df["retention_count"][i]),
        ha="center",
    )

# 添加图例和标签
ax.set_xlabel("Date")
ax.set_ylabel("Count")
ax.set_title("User Metrics Over Time")
ax.legend()
plt.xticks(rotation=45, ha="right", rotation_mode="anchor")
plt.tight_layout()

st.pyplot(fig)

# Dropdown for selecting the retention interval
interval_option = st.radio(
    "选择留存计算的天数间隔:",
    options=[
        "1日留存",
        "3日留存",
        "7日留存",
        "15日留存",
        "30日留存",
    ],
)

# Map selection to interval days
interval_days_map = {
    "1日留存": 1,
    "3日留存": 3,
    "7日留存": 7,
    "15日留存": 15,
    "30日留存": 30,
}
interval_days = interval_days_map[interval_option]
df = calculate_retention(interval_days)

# Plot the selected retention rate
fig, ax = plt.subplots()
ax.plot(df["Date"], df["Retention Rate"], linewidth=1, color="blue", zorder=1)
ax.scatter(df["Date"], df["Retention Rate"], color="red", s=50, zorder=2)
for x, y in zip(df["Date"], df["Retention Rate"]):
    ax.vlines(x, 0, y, colors="gray", linestyles="dashed", linewidth=0.8, zorder=0)

# Labeling the chart
ax.set_xlabel("Date")
ax.set_ylabel("Retention Rate")
plt.xticks(rotation=45, ha="right", rotation_mode="anchor")

st.pyplot(fig)
