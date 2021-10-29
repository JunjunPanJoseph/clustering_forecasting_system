import pandas as pd
from datetime import datetime
from datetime import *
from dateutil import *
from dateutil.tz import *
import mysql.connector

def preprocess_data(filename,meterList):
    df = pd.read_csv(filename)
    boyd=df[meterList]
    boyd=pd.concat([df['Timestamp'],boyd],axis=1)
    #drop all NA rows and reset index
    boyd.dropna(inplace=True)
    boyd.reset_index(drop=True, inplace=True)
    #delete the string 'Melbourne' in the timestamp
    boyd['Timestamp']=boyd["Timestamp"].str.replace("Melbourne", "")
    boyd['Timestamp']=boyd['Timestamp'].str.strip()

    #delete the unit
    for i in range(len(meterList)):
        boyd[meterList[i]]=boyd[meterList[i]].str.replace('kW','')

    #convert to utc timestamp
    utc_zone = tz.gettz('UTC')
    local_zone = tz.gettz('Australia/Melbourne')
    local_datetime=pd.to_datetime(pd.Series(boyd['Timestamp']))
    utc_time_list=[]
    for each in local_datetime:
        each = each.replace(tzinfo=local_zone)
        utc_time = each.astimezone(utc_zone)
        each = utc_time.strftime('%Y-%m-%d %H:%M:%S')
        utc_time_list.append(each)
    utc_time_df=pd.DataFrame(utc_time_list,columns=['utc_timestamp'])
    final_pd=pd.concat([utc_time_df,boyd],axis=1)
    final_pd['Month'] = pd.DatetimeIndex(final_pd['utc_timestamp']).month
    return final_pd

#preprocess all the csv
final_pd_13=preprocess_data('ToolsShell (13).csv',['ME003 MSSB kW','ME005 DB_L1 ME005 DB_L1 kW','ME002 Lift kW','ME001 MCCB kW'])
final_pd_14=preprocess_data('ToolsShell (14).csv',['ME003 MSSB kW','ME005 DB_L1 ME005 DB_L1 kW','ME002 Lift kW','ME001 MCCB kW'])
final_pd_15=preprocess_data('ToolsShell (15).csv',['ME004 DB_LG ME004 DB_LG kW'])
final_pd_16=preprocess_data('ToolsShell (16).csv',['ME004 DB_LG ME004 DB_LG kW'])


def insert_points_mysql(final_pd,connection):
    meter_point_dict={'ME002 Lift kW':78,'ME003 MSSB kW':102,'ME004 DB_LG ME004 DB_LG kW':126,
                      'ME005 DB_L1 ME005 DB_L1 kW':150,'ME001 MCCB kW':174}

    cursor = connection.cursor()
    sql = "INSERT INTO {table} (`value`, `pointId`, `timeStamp`) VALUES (%s, %s, %s)"
    #insert each row in the dataframe to different tables according to the month
    for i in range(len(final_pd)):

        if final_pd['Month'].values[i] >=10:
            table_name = 'hisvalues_'+str(final_pd['Month'].values[i])
        else:
            table_name='hisvalues_0'+str(final_pd['Month'].values[i])
        for each in list(final_pd.columns)[2:-1]:
            try:
                cursor.execute("SET time_zone = '+00:00'")
                cursor.execute(sql.format(table=table_name), (final_pd[each].values[i],meter_point_dict[each],final_pd['utc_timestamp'].values[i]))
                connection.commit()
            except mysql.connector.Error as err:
                print("Something went wrong: {}".format(err))
    return None

connection= mysql.connector.connect(user='admin', password='ykBjWOFx03HeRssxQC8T',
                                           host='ihub.ck2bmhypcabw.ap-southeast-2.rds.amazonaws.com', database='uc_iot_1')

insert_points_mysql(final_pd_13,connection)
insert_points_mysql(final_pd_14,connection)
insert_points_mysql(final_pd_15,connection)
insert_points_mysql(final_pd_16,connection)

connection.close()