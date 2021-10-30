import datetime
import pickle

from flask import Flask, request
import dataloader
import module

app = Flask(__name__)
import os
import numpy as np
import random
import sklearn
import sklearn.metrics


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route("/cluster", methods=["GET", "POST"])
def cluster():
    json_data = request.json
    print(json_data)
    building_name = json_data["building_name"]
    if "start_time" not in json_data:
        start_time = None
    else:
        start_time = json_data["start_time"]
    if "end_time" not in json_data:
        end_time = None
    else:
        end_time = json_data["end_time"]
    if "interval" not in json_data:
        interval = "15min"
    else:
        interval = json_data["interval"]
    id, df_raw = dataloader.load_data("STREAM", building_name, start_time, end_time)
    df_preprocessed = module.preprocessing(df_raw, interval)
    cluster_result = module.clustering(df_preprocessed, 'AgglomerativeClustering', 11)
    try:
        # clusters
        clusters = []
        for k, v in enumerate(cluster_result["cluster_summary"]):
            v["name"] = str(k)
            clusters.append(v)
        dataloader.store_clusters("AgglomerativeClustering", clusters, id, cluster_result["raw"])
        clusters = []
        for k, v in cluster_result["season_summary"].items():
            v["name"] = str(k)
            clusters.append(v)
        dataloader.store_clusters("seasons", clusters, id, cluster_result["raw"])
        clusters = []
        for k, v in cluster_result["weekday_summary"].items():
            v["name"] = str(k)
            clusters.append(v)
        dataloader.store_clusters("weekdays", clusters, id, cluster_result["raw"])
    except:
        return {"Message": "Clustering result exists"}, 409
    # Classification model
    label = cluster_result["raw"]["label"]
    time = cluster_result["raw"]["time"]
    supervised_X = np.array(module.get_supervised_training_set(time))
    supervised_Y = np.array(label)
    classifier = sklearn.tree.DecisionTreeClassifier()
    classifier.fit(supervised_X, supervised_Y)

    dir_path = "./models/{}".format(building_name)
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)
    pickle.dump(cluster_result["model"], open(os.path.join(dir_path, "clustering_model.pkl"), 'wb'))
    pickle.dump(classifier, open(os.path.join(dir_path, "classification_model.pkl"), 'wb'))
    return {"Message": "Success"}, 200


@app.route("/forward", methods=["GET", "POST"])
def forward():
    json_data = request.json
    building_name = json_data["building_name"]
    beg_date = json_data["start_time"]
    end_date = json_data["end_time"]
    beg = datetime.datetime.strptime(beg_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    days = []
    day = beg
    while day < end:
        days.append(day)
        day = day + datetime.timedelta(days=1)
    features = np.array(module.get_supervised_training_set(days))
    dir_path = "./models/{}".format(building_name)
    classsification_model = pickle.load(open(os.path.join(dir_path, "classification_model.pkl"), "rb"))
    labels = classsification_model.predict(features)
    errors = []
    for label, day in zip(labels, days):
        try:
            dataloader.save_forcast_result(building_name, label, day)
        except:
            errors.append([label, day])
    if len(errors) == 0:

        return {"message": "success"}, 200
    else:
        return {"message": "failed to insert some predictions", "errors": errors}, 409

if __name__ == '__main__':
    app.run()
