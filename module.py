import datetime
import pandas as pd
import pandas
import numpy as np
import numpy
import sklearn
import tslearn
import os
import tslearn.utils
import seaborn
import pylab
from tslearn.clustering import TimeSeriesKMeans, KShape
import matplotlib.ticker as ticker
import pytz


# Preprocessing method for Lady huntingfield
# Format of other building may be different

# Reading dataset for ladyhunting field
# point_path = "./iHUB - CoM - Lady Huntingfield/Trends from BMS/Meters/LH_ElectricMeter_EMG_9(MSB)_14Jul2020-18Aug2021.csv"
# meter = pandas.read_csv(point_path)
# meter_preprocessed = preprocessing(meter
def preprocessing(meter, interval):
    meter_preprocessed = meter[["Time stamp", "Value"]].dropna(axis=0, how='any')
    meter_preprocessed["Time stamp"] = pandas.to_datetime(meter_preprocessed["Time stamp"], format="%d/%m/%Y %I:%M:%S")
    meter_preprocessed["Value"] = meter_preprocessed["Value"].apply(pd.to_numeric)
    meter_preprocessed = meter_preprocessed[meter_preprocessed["Value"] < 1000]
    meter_preprocessed = meter_preprocessed[["Time stamp", "Value"]].dropna(axis=0, how='any')
    # drop incomplete data
    if interval == "H":
        dailycount = 24
    elif interval == "15min":
        dailycount = 24 * 4
    else:
        raise Exception("Invalid time interval")
    meter_grouped = meter_preprocessed.groupby(pd.Grouper(key="Time stamp", freq="D"))
    meter_filtered = meter_grouped.filter(lambda x: len(x) == dailycount)
    return meter_filtered


def get_bound(dataset, index_list):
    q1 = np.quantile(dataset[index_list], 0.25, axis=0)
    q2 = np.quantile(dataset[index_list], 0.5, axis=0)

    q3 = np.quantile(dataset[index_list], 0.75, axis=0)
    idr = q3 - q1
    upper = q3 + 1.5 * idr
    lower = q1 - 1.5 * idr
    max = np.max(dataset[index_list], axis=0)
    min = np.min(dataset[index_list], axis=0)
    return {
        "min": min,
        "max": max,
        "q1": q1,
        "q2": q2,
        "q3": q3,
        "upper": upper,
        "lower": lower
    }


def get_model(name, n_clusters):
    clusters = {
        "AgglomerativeClustering": {
            "model": sklearn.cluster.AgglomerativeClustering(n_clusters=n_clusters),
            "sklearn": True
        },
        "Birch": {
            "model": sklearn.cluster.Birch(n_clusters=n_clusters),
            "sklearn": True
        },
        "KMeansEuclidean": {
            "model": TimeSeriesKMeans(n_clusters=n_clusters),
            "sklearn": False
        },
        "KMeansDTW": {
            "model": TimeSeriesKMeans(n_clusters=n_clusters, metric='dtw'),
            "sklearn": False
        },
        "KShape": {
            "model": KShape(n_clusters=n_clusters),
            "sklearn": False
        },
    }
    return clusters[name]


# df: preprocessed dataframe, columns = ["Time stamp", "Value"] (Timestamp in datetime format)
# model: model generated by get_model


def get_summary(dataset, time, index_list):
    weekday = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}
    month = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0, 8: 0, 9: 0, 10: 0, 11: 0}
    year = {}
    for index in index_list:
        if time[index].year not in year:
            year[time[index].year] = 0
        year[time[index].year] += 1
        weekday[time[index].weekday()] += 1
        month[time[index].month - 1] += 1
    return {
        "index": index_list,
        "summary": {"year": year, "month": month, "weekday": weekday},
        "statistic": get_bound(dataset, index_list)
    }


def get_statistic(dataset, timestamp, time, label, n_clusters):
    # Cluster summary
    cluster_summary = []
    for cluster_no in range(n_clusters):
        index_list = numpy.where(label == cluster_no)[0].tolist()
        cluster_summary.append(get_summary(dataset, time, index_list))
    weekday_index = {
        "weekday": [],
        "saturday": [],
        "sunday": []
    }
    season_index = {
        "spring": [],
        "summer": [],
        "autumn": [],
        "winter": [],
    }
    for index in range(len(time)):
        if time[index].weekday() == 6:
            weekday_index["sunday"].append(index)
        elif time[index].weekday() == 5:
            weekday_index["saturday"].append(index)
        else:
            weekday_index["weekday"].append(index)
        if time[index].month >= 12 and time[index].month <= 2:
            season_index["summer"].append(index)
        elif time[index].month >= 3 and time[index].month <= 5:
            season_index["autumn"].append(index)
        elif time[index].month >= 6 and time[index].month <= 8:
            season_index["winter"].append(index)
        else:
            season_index["spring"].append(index)
    # weekday
    weekday_summary = {}
    for k, v in weekday_index.items():
        weekday_summary[k] = get_summary(dataset, time, v)
    # season
    season_summary = {}
    for k, v in season_summary.items():
        season_summary[k] = get_summary(dataset, time, v)
    return {
        "n_clusters": n_clusters,
        "cluster_summary": cluster_summary,
        "season_summary": season_summary,
        "weekday_summary": weekday_summary,
        "raw": {
            "data": dataset,
            "timestamp": timestamp,
            "time": time
        }
    }


def clustering(df, model_name, n_clusters):
    grouped_day = df.groupby(df["Time stamp"].dt.date)
    dataset = []
    timestamp = []
    time = []
    for ts, df in grouped_day:
        time.append(ts)
        dataset.append(numpy.array(df["Value"]))
        timestamp.append(numpy.array(df["Time stamp"].apply(lambda x: x.strftime("%H:%M:%S"))))
    ts_dataset = tslearn.utils.to_time_series_dataset(dataset)
    dataset = np.array(dataset)

    model = get_model(model_name, n_clusters)
    using_sklearn = model["sklearn"]
    km = model["model"]

    if using_sklearn:
        label = km.fit_predict(ts_dataset.squeeze())
    else:
        km.fit(ts_dataset)
        label = km.predict(ts_dataset)
    result = get_statistic(dataset, timestamp, time, label, n_clusters)
    result["model_name"] = model_name
    result["model"] = model
    return result
