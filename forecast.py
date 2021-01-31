import pandas as pd
from utils import Redshift, payroll_days, add_days, Sheets
from query import query
import numpy as np
import datetime
import json
from probabilities import predict

def __main__() :
    """
    For each account in Activation Process, obtain the expected date for each stage and the probability of success/cancel
    Se incluyen alertas para Stage 3, Stage 6 y Billed Date
    """
    engine = Redshift().engine
    q = query().query4
    #Query accounts
    df_2 = pd.read_sql(sql= q, con = engine)
    #MRR from MXN to USD
    mp = {"Business":"Direct", "Outsourcing Account":"Indirect", "Outsourcer":"Indirect"}
    df_2['MRR'] = (df_2['license_cost_c'] + df_2['num_empleados_c'] * df_2['employee_price_c']) / 19.3
    df_2 = df_2.replace({"customer_type_c": mp})
    df_2 = df_2.drop(columns=['license_cost_c', 'employee_price_c'])
    df_2= df_2[df_2["customer_type_c"] == 'Direct']

    #Query dataset of dates
    df = pd.read_sql(sql= query().query3, con = engine)
    #df['stage_4_date'] = df["stage_4_date"].where(~pd.isnull(df["stage_4_date"]), df["migration_date_c"])
    df = df.drop(columns = ['migration_date_c'])
    types = {"stage_1_date":"datetime64[ns]",
            "stage_2_date":"datetime64[ns]",
            "stage_3_date":"datetime64[ns]",
            "stage_4_date":"datetime64[ns]",
            "stage_5_date":"datetime64[ns]",
            "stage_6_date":"datetime64[ns]",
            "stage_7_date":"datetime64[ns]",
            "Stage 8_date":"datetime64[ns]",
            "billed_date":"datetime64[ns]"}
    df = df.astype(types)
    limit = datetime.datetime.today() - datetime.timedelta(days = 90)
    df = df[df["created_date"] > limit]

    #Starting from Opp creation date, add days in order to achieve the agreed number of days for each stage
    df["p_stage_1_date"] = [ add_days( x, 0, 0 ).new_date for x in df["created_date"].dt.date ]
    df["p_stage_2_date"] = [ add_days( x, 1, 0 ).new_date for x in df["p_stage_1_date"] ]
    df["p_stage_3_date"] = [ add_days( x, 3, 1 ).new_date for x in df["p_stage_2_date"] ]
    df["p_stage_4_date"] = [ add_days( x, 1, 0 ).new_date for x in df["p_stage_3_date"] ]
    df["p_stage_5_date"] = [ add_days( x, 1, 0 ).new_date for x in df["p_stage_4_date"]]
    df["p_stage_6_date"] = [ add_days( x, 1, 0 ).new_date for x in df["p_stage_5_date"] ]
    df["p_stage_7_date"] = [ add_days( x, 3, 1 ).new_date for x in df["p_stage_6_date"] ]
    df["p_Stage 8_date"] = [ add_days( x, 0, 0 ).new_date for x in df["p_stage_7_date"] ]
    df["p_Stage 8_end_date"] = [ add_days( x, 3, 2 ).new_date for x in df["p_Stage 8_date"] ]
    df["p_billed_date"] = [ add_days( x, 1, 0 ).new_date for x in df["p_Stage 8_end_date"] ]

    #df["p_total"] = [ (i-j).total_seconds() / 86400 for i, j in zip(df["p_billed_date"].astype('datetime64[ns]'), df["created_date"]) ]
    df["p_Total"] = [ np.busday_count(j.date(),i) + 1 for i, j in zip(df["p_billed_date"], df["created_date"]) ] 

    #Generate alerts. Each alert field is TRUE if some opp has exceeded its estimated date to complete any of these three stages
    #Stage 4, Stage 7 and Billed
    stages = [
        'Customer Intro', 
        'Stage 1', 
        'Stage 2', 
        'Stage 3', 
        'Stage 4', 
        'Stage 5', 
        'Stage 6', 
        'Stage 7', 
        'Stage 8'
        ]

    l1 = []
    for i in range(len(df)) :
        stage = list(df['stage_name'])[i]
        index = stages.index('Stage 4')
        if stage in stages[:index] :
            limit_days = np.busday_count(list(df["created_date"])[i].date(), list(df["p_stage_4_date"])[i])
            days_in = np.busday_count(list(df["created_date"])[i].date(), datetime.date.today())
            if days_in > limit_days + 1:
                l1.append(True)
            else :
                l1.append(False)
        else :
            l1.append('')
    df["Alert1"] = l1

    l2 = []
    for i in range(len(df)) :
        stage = list(df['stage_name'])[i]
        index_1 = stages.index('Stage 7')
        index_2 = stages.index('Stage 4')
        if stage in stages[index_2:index_1] :
            limit_days = np.busday_count(list(df["created_date"])[i].date(), list(df["p_stage_7_date"])[i])
            days_in = np.busday_count(list(df["created_date"])[i].date(), datetime.date.today())
            if days_in > limit_days + 1 :
                l2.append(True)
            else :
                l2.append(False)
        else :
            l2.append('')
    df["Alert2"] = l2

    l3 = []
    for i in range(len(df)) :
        stage = list(df['stage_name'])[i]
        index = stages.index('Stage 7')
        if stage in stages[index:] :
            limit_days = np.busday_count(list(df["created_date"])[i].date(), list(df["p_billed_date"])[i])
            days_in = np.busday_count(list(df["created_date"])[i].date(), datetime.date.today())
            if days_in > limit_days + 1:
                l3.append(True)
            else :
                l3.append(False)
        else :
            l3.append('')
    df["Alert3"] = l3

    df = pd.merge(left=df, right=df_2, left_on='id', right_on='id', how = 'left')
    df = df[df["status_update_c"] != "To cut"]

    #Merge with the probabilities of success/cancell
    df_probs = predict()
    df = pd.merge(left=df, right=df_probs, left_on='id', right_on='id', how = 'left')

    df = df.sort_values(by = ['name'])
    #Insert into google spreasheet
    Sheets().Insert(df, "Forecast")

if __name__ == '__main__' :
    __main__()