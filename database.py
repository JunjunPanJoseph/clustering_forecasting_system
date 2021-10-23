import psycopg2
import json
import pandas as pd
config = {
    "db_name": 'test',
    'user': 'ASUS',
    'password': '123asd',
    'host': '127.0.0.1',
    'port': '5432'
}
conn = psycopg2.connect(
    database=config["db_name"],
    user=config["user"],
    password=config["password"],
    host=config["host"],
    port=config["port"]
)
# cur = conn.cursor()
# cur.execute("SELECT * FROM table1 LIMIT 10")
# rows = cur.fetchall()
# print(rows)
# conn.commit()
# cur.close()
# conn.close()
def init_table():
    cur = conn.cursor()
    cur.execute('''
        DROP TABLE IF EXISTS BUILDING CASCADE;
        CREATE TABLE BUILDING (
            ID  SERIAL PRIMARY KEY,
            NAME VARCHAR(100) NOT NULL
        );
        DROP TABLE IF EXISTS STREAM CASCADE;
        CREATE TABLE STREAM (
            ID  SERIAL PRIMARY KEY,
            BUILDING_ID INT NOT NULL,
            TIMESTAMP TIMESTAMP NOT NULL,
            VALUE DECIMAL(12, 4),
            FOREIGN KEY (BUILDING_ID) REFERENCES BUILDING(ID)
        );
        DROP TABLE IF EXISTS PREPROCESSED_STREAM CASCADE;
        CREATE TABLE PREPROCESSED_STREAM (
            ID  SERIAL PRIMARY KEY,
            BUILDING_ID INT NOT NULL,
            TIMESTAMP TIMESTAMP NOT NULL,
            VALUE DECIMAL(12, 4),
            FOREIGN KEY (BUILDING_ID) REFERENCES BUILDING(ID)
        );
        DROP TABLE IF EXISTS CLUSTER_MEMBER CASCADE;
        CREATE TABLE CLUSTER_MEMBER (
            ID  SERIAL PRIMARY KEY,
            BUILDING_ID INT NOT NULL,
            NAME VARCHAR(100) NOT NULL,
            STATISTIC TEXT NOT NULL,
            FOREIGN KEY (BUILDING_ID) REFERENCES BUILDING(ID)
        );
        DROP TABLE IF EXISTS CLUSTER_STREAM CASCADE;
        CREATE TABLE CLUSTER_STREAM (
            ID  SERIAL PRIMARY KEY,
            CLUSTER_ID INT NOT NULL,
            TIME TIME NOT NULL,
            MIN DECIMAL(12, 4),
            MAX DECIMAL(12, 4),
            Q1 DECIMAL(12, 4),
            Q2 DECIMAL(12, 4),
            Q3 DECIMAL(12, 4),
            UPPER DECIMAL(12, 4),
            LOWER DECIMAL(12, 4),
            FOREIGN KEY (CLUSTER_ID) REFERENCES CLUSTER_MEMBER(ID)
        );
        
        DROP TABLE IF EXISTS PREDICT CASCADE;
        CREATE TABLE PREDICT (
            ID  SERIAL PRIMARY KEY,
            BUILDING_ID INT NOT NULL,
            TIMESTAMP TIMESTAMP NOT NULL,
            VALUE DECIMAL(12, 4),
            FOREIGN KEY (BUILDING_ID) REFERENCES BUILDING(ID)
        );
''')
    conn.commit()
    cur.close()
if __name__ == '__main__':
    init_table()
    df = pd.read_csv("../clustering/library.csv")
    cur = conn.cursor()
    cur.execute("INSERT INTO BUILDING (NAME) VALUES ('library')")
    conn.commit()

    cur.execute("select * from BUILDING where NAME='{}'".format('library'))
    result = cur.fetchone()
    print(result)
    building_id = result[0]

    for line in df.iterrows():
        print(line)
        cur.execute("INSERT INTO STREAM (BUILDING_ID, TIMESTAMP, VALUE) VALUES ({}, '{}', '{}')".format(building_id, line[1]["Time stamp"][:19], line[1]["Value"]))
    conn.commit()

    df = pd.read_csv("../clustering/library.csv")
    cur.close()
