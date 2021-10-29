import pickle

from flask import Flask, request
import dataloader
import module
app = Flask(__name__)
import os

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
    dir_path = "./models/{}".format(building_name)
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)
        pickle.dump(cluster_result["model"], open(os.path.join(dir_path, "clustering_model.pkl"), 'wb'))

    return {"Message": "Success"}, 200

def forward(building_name, beg_date, end_date):
    pass

if __name__ == '__main__':
    app.run()
