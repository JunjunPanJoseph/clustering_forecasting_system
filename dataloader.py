import datetime

from database import conn, config
import pandas as pd
def load_data(table_name, name, time_start=None, time_end=None):
    cur = conn.cursor()
    cur.execute("select * from BUILDING where NAME='{}'".format(name))
    result = cur.fetchone()
    print(result)
    if result is None:
        return None
    building_id = result[0]
    if time_start is None and time_end is None:
        cur.execute("select * from {} where BUILDING_ID={}".format(table_name, building_id))
    elif time_end is None:
        cur.execute("select * from {} where BUILDING_ID={} and TIMESTAMP >= '{}'".format(table_name, building_id, time_start))
    elif time_start is None:
        cur.execute("select * from {} where BUILDING_ID={} and TIMESTAMP <= '{}'".format(table_name, building_id, time_end))
    else:
        cur.execute("select * from {} where BUILDING_ID={} and TIMESTAMP >= '{}' and TIMESTAMP <= '{}'".format(table_name, building_id, time_start, time_end))
    timestamp = []
    value = []
    for line in cur.fetchall():
        timestamp.append(line[2])
        value.append(line[3])
    cur.close()
    df =  pd.DataFrame({"Time stamp": timestamp, "Value": value})
    return building_id, df
def save_data(table_name, building_id, df):
    cur = conn.cursor()
    for index, line in df.iterrows():
        cur.execute("INSERT INTO {} (BUILDING_ID, TIMESTAMP, VALUE) VALUES ({}, '{}', '{}')".format(table_name, building_id, line["Time stamp"][:19], line["Value"]))
    cur.close()
def store_clusters(name, clusters, building_id, raw):
    cur = conn.cursor()
    # store cluster method
    cur.execute("INSERT INTO CLUSTER_METHOD (BUILDING_ID, NAME) VALUES ({}, '{}') RETURNING ID".format(building_id, name))
    conn.commit()
    cluster_method_pk = cur.fetchone()[0]
    # store cluster information
    for cluster in clusters:
        cur.execute("INSERT INTO CLUSTER_MEMBER (CLUSTER_METHOD_ID, NAME, STATISTIC) VALUES (%s, %s, %s) RETURNING ID", (cluster_method_pk, cluster["name"], str(cluster["summary"])))
        conn.commit()
        cluster_pk = cur.fetchone()[0]
        for i in range(len(cluster["statistic"]["min"])):
            cur.execute("INSERT INTO CLUSTER_STREAM (CLUSTER_ID, TIME, MIN, MAX, Q1, Q2, Q3, UPPER, LOWER) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",  (
                cluster_pk, raw["timestamp"][0][i], cluster["statistic"]["min"][i], cluster["statistic"]["max"][i],
                cluster["statistic"]["q1"][i], cluster["statistic"]["q2"][i], cluster["statistic"]["q3"][i],
                cluster["statistic"]["upper"][i], cluster["statistic"]["lower"][i]))
    cur.close()

def save_forcast_result(building_name, label, date):
    cur = conn.cursor()
    # get cluster information
    cur.execute("select * from BUILDING where NAME='{}'".format(building_name))
    result = cur.fetchone()
    building_id = result[0]
    cur.execute('''
        SELECT CLUSTER_STREAM.TIME, CLUSTER_STREAM.Q2 
        FROM CLUSTER_STREAM, CLUSTER_MEMBER, CLUSTER_METHOD
        WHERE 
             CLUSTER_METHOD.BUILDING_ID = %s
        AND CLUSTER_METHOD.NAME = 'AgglomerativeClustering'
        AND CLUSTER_METHOD.ID = CLUSTER_MEMBER.CLUSTER_METHOD_ID
        AND CLUSTER_MEMBER.NAME = %s
        AND CLUSTER_MEMBER.ID = CLUSTER_STREAM.CLUSTER_ID
    ORDER BY CLUSTER_STREAM.TIME
    ''', (building_id, str(label)))
    result = cur.fetchall()
    for res in result:
        time = datetime.datetime.combine(date, res[0])
        cur.execute("INSERT INTO PREDICT (BUILDING_ID, TIMESTAMP, VALUE) VALUES (%s, %s, %s)", (building_id, time, res[1]))
        conn.commit()
    cur.close()

if __name__ == '__main__':
    id, df = load_data("STREAM", "library",time_start="2020-08-22 00:59:57", time_end="2020-08-22 04:59:57")
