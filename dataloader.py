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

def save_result(result):
    pass
if __name__ == '__main__':
    id, df = load_data("STREAM", "library",time_start="2020-08-22 00:59:57", time_end="2020-08-22 04:59:57")
