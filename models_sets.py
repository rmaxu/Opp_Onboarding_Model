import model_querys as querys
import datetime
import pandas as pd
from utils import Redshift

class Training_Set() :
    """
    Create the training set for the model 
    """
    def __init__(self) :
        engine = Redshift().engine
        #query dataset with dates
        df = pd.read_sql(sql = querys.query1, con = engine)
        mp = {"Business":"Direct", "Outsourcing Account":"Indirect", "Outsourcer":"Indirect"}
        df = df.replace({"customer_type_c": mp})
        df = df[  (df['stage_name'] == 'Billed') | (df['stage_name'] == 'Cancelled Booking')]
        df_direct = df[df["customer_type_c"] == "Direct"]
        df_direct = df_direct[ df_direct['id'] != '0063m00000iqlcJAAQ']

        #Store the dates of stages changes for building a dataframe
        aux_l = []
        d_id = dict(df_direct['id'].value_counts())
        for i in d_id.keys() :
            aux_df = df_direct[df_direct['id'] == i ]
            aux_df = aux_df.sort_values(by='stage_change_date', ascending = False).copy()
            aux_d = {}
            if list(aux_df['new_value'])[0] == None :
                    continue
            for j in range(len(aux_df)) :
                aux_d['id'] = list(aux_df['id'])[j]
                aux_d['created_date'] = list(aux_df['created_date'])[j]
                aux_d['Customer Intro_start_date'] = list(aux_df['created_date'])[j]
                aux_d['stage_name'] = list(aux_df['stage_name'])[j]
                aux_d[  list(aux_df['new_value'])[j] + "_start_date" ] = list(aux_df['stage_change_date'])[j]
                aux_d[  list(aux_df['old_value'])[j] + "_end_date" ] = list(aux_df['stage_change_date'])[j]
                if list(aux_df['new_value'])[j] == 'Cancelled Booking' :
                    aux_d['stage_before_cancelled'] = list(aux_df['old_value'])[j]
            aux_l.append(aux_d)
            
        df_dates = pd.json_normalize(aux_l)
        stages = ['Customer Intro' ,'Stage 1', 'Stage 2', 'Stage 3', 'Stage 4', 'Stage 5', 'Stage 6', 'Stage 7', 'Stage 8']
        df_dates = df_dates[ (df_dates['stage_before_cancelled'].isin(stages)) | (df_dates['stage_before_cancelled'].isna()) ]

        #Match the dates. For two consecutive stages, stage_1 and stage_2, stage_1_end_date is equal to stage_2_start_date.
        #Some date values are missing
        df_dates['Customer Intro_end_date'] = df_dates['Customer Intro_end_date'].where(~pd.isna(df_dates['Customer Intro_end_date']), df_dates['Stage 1_start_date'])
        df_dates['Stage 1_start_date'] = df_dates['Stage 1_start_date'].where(~pd.isna(df_dates['Stage 1_start_date']), df_dates['Customer Intro_end_date'])
        df_dates['Stage 1_end_date'] = df_dates['Stage 1_end_date'].where(~pd.isna(df_dates['Stage 1_end_date']), df_dates['Stage 2_start_date'])
        df_dates['Stage 2_start_date'] = df_dates['Stage 2_start_date'].where(~pd.isna(df_dates['Stage 2_start_date']), df_dates['Stage 1_end_date'])
        df_dates['Stage 2_end_date'] = df_dates['Stage 2_end_date'].where(~pd.isna(df_dates['Stage 2_end_date']), df_dates['Stage 3_start_date'])
        df_dates['Stage 3_start_date'] = df_dates['Stage 3_start_date'].where(~pd.isna(df_dates['Stage 3_start_date']), df_dates['Stage 2_end_date'])
        df_dates['Stage 3_end_date'] = df_dates['Stage 3_end_date'].where(~pd.isna(df_dates['Stage 3_end_date']), df_dates['Stage 4_start_date'])
        df_dates['Stage 4_start_date'] = df_dates['Stage 4_start_date'].where(~pd.isna(df_dates['Stage 4_start_date']), df_dates['Stage 3_end_date'])
        df_dates['Stage 4_end_date'] = df_dates['Stage 4_end_date'].where(~pd.isna(df_dates['Stage 4_end_date']), df_dates['Stage 5_start_date'])
        df_dates['Stage 5_start_date'] = df_dates['Stage 5_start_date'].where(~pd.isna(df_dates['Stage 5_start_date']), df_dates['Stage 4_end_date'])
        df_dates['Stage 5_end_date'] = df_dates['Stage 5_end_date'].where(~pd.isna(df_dates['Stage 5_end_date']), df_dates['Stage 6_start_date'])
        df_dates['Stage 6_start_date'] = df_dates['Stage 6_start_date'].where(~pd.isna(df_dates['Stage 6_start_date']), df_dates['Stage 5_end_date'])
        df_dates['Stage 6_end_date'] = df_dates['Stage 6_end_date'].where(~pd.isna(df_dates['Stage 6_end_date']), df_dates['Stage 7_start_date'])
        df_dates['Stage 7_start_date'] = df_dates['Stage 7_start_date'].where(~pd.isna(df_dates['Stage 7_start_date']), df_dates['Stage 6_end_date'])
        df_dates['Stage 7_end_date'] = df_dates['Stage 7_end_date'].where(~pd.isna(df_dates['Stage 7_end_date']), df_dates['Stage 8_start_date'])
        df_dates['Stage 8_start_date'] = df_dates['Stage 8_start_date'].where(~pd.isna(df_dates['Stage 8_start_date']), df_dates['Stage 7_end_date'])
        df_dates['Stage 8_end_date'] = df_dates['Stage 8_end_date'].where(~pd.isna(df_dates['Stage 8_end_date']), df_dates['Billed_start_date'])

        df_dates.reset_index(drop = True, inplace = True)

        #Compute times and milestones for each stage.
        #Times are time spent per stage and milestone is time that it took the opp to arrive each stage since opportunity creatin date
        for i in stages :
            l_times = []
            l_milestones = []
            for j in range(len(df_dates)) :
                if df_dates['stage_name'][j] == 'Cancelled Booking' :
                    last_stage = df_dates['stage_before_cancelled'][j]
                    index = stages.index(last_stage)
                    if index > stages.index(i) :
                        l_times.append( df_dates[ i + '_end_date' ][j] -  df_dates[ i + '_start_date' ][j] )
                        l_milestones.append( df_dates[ i + '_start_date' ][j] -  df_dates[ 'created_date' ][j] )
                    elif index == stages.index(i) :
                        l_times.append(df_dates['Cancelled Booking_start_date'][j] -  df_dates[ stages[index] + '_start_date' ][j])
                        l_milestones.append(df_dates[ i + '_start_date' ][j] -  df_dates[ 'created_date' ][j])
                    elif index + 1 == stages.index(i) :
                        l_times.append(None)
                        l_milestones.append(df_dates['Cancelled Booking_start_date'][j] -  df_dates[ 'created_date' ][j])
                    else :
                        l_times.append(None)
                        l_milestones.append(None)
                else :
                    l_times.append( df_dates[ i + '_end_date' ][j] -  df_dates[ i + '_start_date' ][j] )
                    l_milestones.append( df_dates[ i + '_start_date' ][j] -  df_dates[ 'created_date' ][j] )
            df_dates[i + '_Time'] = l_times
            df_dates[i + '_Milestone'] = l_milestones
            if df_dates[i + '_Time'].dtypes == 'timedelta64[ns]' :
                df_dates[i + '_Time'] = df_dates[i + '_Time'].dt.days
            if df_dates[i + '_Milestone'].dtypes == 'timedelta64[ns]' :
                df_dates[i + '_Milestone'] = df_dates[i + '_Milestone'].dt.days
                        
        subset = ['id', 'Billed_start_date']
        for i in df_dates.columns :
            if '_Milestone' in i or '_Time' in i :
                subset.append(i)
        df_times = df_dates[subset]
        
        df_2 = pd.read_sql(sql = querys.query2, con = engine)
        
        df_2 = pd.merge(left = df_2, right = df_times, how = 'right', on = 'id')
        df_2['live_date_c'] = [ datetime.datetime.combine(x, datetime.time(0, 0, 0, 0)) if not pd.isna(x) else None for x in  df_2['live_date_c']]
        temp_l = []
        for i in range(len(df_2)) :
            if df_2['stage_name'][i] == 'Cancelled Booking' :
                temp_l.append( df_2['cancelled_date'][i] - df_2['created_date'][i]  )
            elif df_2['stage_name'][i] == 'Billed' :
                temp_l.append( df_2['Billed_start_date'][i] - df_2['created_date'][i]  )
                
        df_2['time_to_close'] = temp_l
        df_2['time_to_close'] = df_2['time_to_close'].dt.days

        df_2 = df_2.replace({'account_source_c':{'Inbound': 1, 'Outbound - BDR':2, 'Event':3}})
        df_2 = df_2.replace({'stage_name':{'Billed': 1, 'Cancelled Booking':0}})

        df_2 = df_2[df_2['time_to_close'] > 10 ]
        df_2 = df_2[ ~ (  (df_2['stage_name'] == 1 ) & (df_2['time_to_close'] > 60) ) ]
        

        self.df = df_2

class active_accounts :
    """
    Create the dataset of the opportunities that are currently in Activation Process
    """
    def __init__(self) :
        engine = Redshift().engine
        df = pd.read_sql(sql = querys.query4, con = engine)

        df['migration_date_c'] = [ datetime.datetime.combine(x, datetime.time(0, 0, 0, 0)) if not pd.isna(x) else None for x in  df['migration_date_c']]

        mp = {"Business":"Direct", "Outsourcing Account":"Indirect", "Outsourcer":"Indirect"}
        df = df.replace({"customer_type_c": mp})
        df_direct = df[df["customer_type_c"] == "Direct"]

        aux_l = []
        d_id = dict(df_direct['id'].value_counts())
        for i in d_id.keys() :
            aux_df = df_direct[df_direct['id'] == i ]
            aux_df =  aux_df.sort_values(by='stage_change_date', ascending = False).copy()
            aux_d = {}
            for j in range(len(aux_df)) :
                aux_d['id'] = list(aux_df['id'])[j]
                aux_d['created_date'] = list(aux_df['created_date'])[j]
                aux_d['Customer Intro_start_date'] = list(aux_df['created_date'])[j]
                aux_d['stage_name'] = list(aux_df['stage_name'])[j]
                if list(aux_df['new_value'])[j] != None :
                    #aux_d['migration_date_c'] = list(aux_df['migration_date_c'])[j]
                    aux_d[  list(aux_df['new_value'])[j] + "_start_date" ] = list(aux_df['stage_change_date'])[j]
                    aux_d[  list(aux_df['old_value'])[j] + "_end_date" ] = list(aux_df['stage_change_date'])[j]
            aux_l.append(aux_d)
            
        df_dates = pd.json_normalize(aux_l)  

        stages = ['Customer Intro' ,'Stage 1', 'Stage 2', 'Stage 3', 'Stage 4', 'Stage 5', 'Stage 6', 'Stage 7', 'Stage 8']

        for i in stages :
            if i + '_start_date' not in df_dates :
                df_dates[ i + '_start_date' ] = None
            if i + '_end_date' not in df_dates :
                df_dates[ i + '_end_date' ] = None

        df_dates['Customer Intro_end_date'] = df_dates['Customer Intro_end_date'].where(~pd.isna(df_dates['Customer Intro_end_date']), df_dates['Stage 1_start_date'])
        df_dates['Stage 1_start_date'] = df_dates['Stage 1_start_date'].where(~pd.isna(df_dates['Stage 1_start_date']), df_dates['Customer Intro_end_date'])
        df_dates['Stage 1_end_date'] = df_dates['Stage 1_end_date'].where(~pd.isna(df_dates['Stage 1_end_date']), df_dates['Stage 2_start_date'])
        df_dates['Stage 2_start_date'] = df_dates['Stage 2_start_date'].where(~pd.isna(df_dates['Stage 2_start_date']), df_dates['Stage 1_end_date'])
        df_dates['Stage 2_end_date'] = df_dates['Stage 2_end_date'].where(~pd.isna(df_dates['Stage 2_end_date']), df_dates['Stage 3_start_date'])
        df_dates['Stage 3_start_date'] = df_dates['Stage 3_start_date'].where(~pd.isna(df_dates['Stage 3_start_date']), df_dates['Stage 2_end_date'])
        df_dates['Stage 3_end_date'] = df_dates['Stage 3_end_date'].where(~pd.isna(df_dates['Stage 3_end_date']), df_dates['Stage 4_start_date'])
        df_dates['Stage 4_start_date'] = df_dates['Stage 4_start_date'].where(~pd.isna(df_dates['Stage 4_start_date']), df_dates['Stage 3_end_date'])
        df_dates['Stage 4_end_date'] = df_dates['Stage 4_end_date'].where(~pd.isna(df_dates['Stage 4_end_date']), df_dates['Stage 5_start_date'])
        df_dates['Stage 5_start_date'] = df_dates['Stage 5_start_date'].where(~pd.isna(df_dates['Stage 5_start_date']), df_dates['Stage 4_end_date'])
        df_dates['Stage 5_end_date'] = df_dates['Stage 5_end_date'].where(~pd.isna(df_dates['Stage 5_end_date']), df_dates['Stage 6_start_date'])
        df_dates['Stage 6_start_date'] = df_dates['Stage 6_start_date'].where(~pd.isna(df_dates['Stage 6_start_date']), df_dates['Stage 5_end_date'])
        df_dates['Stage 6_end_date'] = df_dates['Stage 6_end_date'].where(~pd.isna(df_dates['Stage 6_end_date']), df_dates['Stage 7_start_date'])
        df_dates['Stage 7_start_date'] = df_dates['Stage 7_start_date'].where(~pd.isna(df_dates['Stage 7_start_date']), df_dates['Stage 6_end_date'])
        df_dates['Stage 7_end_date'] = df_dates['Stage 7_end_date'].where(~pd.isna(df_dates['Stage 7_end_date']), df_dates['Stage 8_start_date'])
        df_dates['Stage 8_start_date'] = df_dates['Stage 8_start_date'].where(~pd.isna(df_dates['Stage 8_start_date']), df_dates['Stage 7_end_date'])

        df_dates['Customer Intro_end_date'] = pd.to_datetime(df_dates['Customer Intro_end_date'], errors = 'coerce')
        df_dates['Stage 1_start_date'] = pd.to_datetime(df_dates['Stage 1_start_date'], errors = 'coerce')
        df_dates['Stage 1_end_date'] = pd.to_datetime(df_dates['Stage 1_end_date'], errors = 'coerce')
        df_dates['Stage 2_start_date'] = pd.to_datetime(df_dates['Stage 2_start_date'], errors = 'coerce')
        df_dates['Stage 2_end_date'] = pd.to_datetime(df_dates['Stage 2_end_date'], errors = 'coerce')
        df_dates['Stage 3_start_date'] = pd.to_datetime(df_dates['Stage 3_start_date'], errors = 'coerce')
        df_dates['Stage 3_end_date'] = pd.to_datetime(df_dates['Stage 3_end_date'], errors = 'coerce')
        df_dates['Stage 4_start_date'] = pd.to_datetime(df_dates['Stage 4_start_date'], errors = 'coerce')
        df_dates['Stage 4_end_date'] = pd.to_datetime(df_dates['Stage 4_end_date'], errors = 'coerce')
        df_dates['Stage 5_start_date'] = pd.to_datetime(df_dates['Stage 5_start_date'], errors = 'coerce')
        df_dates['Stage 5_end_date'] = pd.to_datetime(df_dates['Stage 5_end_date'], errors = 'coerce')
        df_dates['Stage 6_start_date'] = pd.to_datetime(df_dates['Stage 6_start_date'], errors = 'coerce')
        df_dates['Stage 6_end_date'] = pd.to_datetime(df_dates['Stage 6_end_date'], errors = 'coerce')
        df_dates['Stage 7_start_date'] = pd.to_datetime(df_dates['Stage 7_start_date'], errors = 'coerce')
        df_dates['Stage 7_end_date'] = pd.to_datetime(df_dates['Stage 7_end_date'], errors = 'coerce')
        df_dates['Stage 8_start_date'] = pd.to_datetime(df_dates['Stage 8_start_date'], errors = 'coerce')


        for i in stages :
            l_times = []
            l_milestones = []
            for j in range(len(df_dates)) :
                current_stage = df_dates['stage_name'][j]
                index = stages.index(current_stage)
                if index == stages.index(i) :
                    l_times.append(  datetime.datetime.now() - df_dates[ i + '_start_date' ][j]  )
                    l_milestones.append( df_dates[ i + '_start_date' ][j] -  df_dates[ 'created_date' ][j] )
                elif index > stages.index(i) :
                    l_times.append( df_dates[ i + '_end_date' ][j] -  df_dates[ i + '_start_date' ][j] )
                    l_milestones.append( df_dates[ i + '_start_date' ][j] -  df_dates[ 'created_date' ][j] )
                else :
                    l_times.append(None)
                    l_milestones.append(None)
            df_dates[i + '_Time'] = l_times
            df_dates[i + '_Milestone'] = l_milestones
            if df_dates[i + '_Time'].dtypes == 'timedelta64[ns]' :
                df_dates[i + '_Time'] = df_dates[i + '_Time'].dt.days
            if df_dates[i + '_Milestone'].dtypes == 'timedelta64[ns]' :
                df_dates[i + '_Milestone'] = df_dates[i + '_Milestone'].dt.days

        df_dates['total_time'] = [ datetime.datetime.now() - i for i in list(df_dates['created_date'])  ]
        df_dates['total_time'] =  df_dates['total_time'].dt.days

        subset = ['id', 'created_date', 'stage_name', 'total_time']
        for i in df_dates.columns :
            if '_Milestone' in i or '_Time' in i :
                subset.append(i)
        df_times = df_dates[subset]

        self.df = df_times