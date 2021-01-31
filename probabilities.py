from model_sets import active_accounts
from models import Models
import pandas as pd

def predict() :
    """
    Obtaing probabilities of success and cancel for each opportunity in Actication Process
    """
    #get set of accounts that are curently in activation process
    df = active_accounts().df

    #Separate into 5 sets, one for each model
    df_cr = df[df['stage_name'] == 'Stage 1']
    df_fh = df[df['stage_name'].isin(['Customer Intro', 'Stage 2', 'Stage 3', 'Stage 4'])]
    df_t = df[df['stage_name'].isin(['Stage 5', 'Stage 6'])]
    df_tp = df[df['stage_name'] == 'Stage 7']
    df_ad = df[df['stage_name'] == 'Stage 8']

    #Prediction for the First Half
    l = []
    df_fh = df_fh[['id', 'total_time']]
    df_fh.dropna(inplace = True)
    if not df_fh.empty :
        model_fh = Models('First Half').model
        pred_fh = model_fh.predict(df_fh.iloc[:,1:2])
        prob_fh = model_fh.predict_proba(df_fh.iloc[:,1:2])
        df_fh['prob_success'] = prob_fh[:,1]
        df_fh['prob_cancel'] = prob_fh[:,0]
        l.append(df_fh[['id', 'prob_success', 'prob_cancel']])
        #print(df_fh)

   #Prediction for Stage 1
    df_cr = df_cr[['id', 'Stage 1_Time', 'Stage 1_Milestone']]
    df_cr.dropna(inplace = True)
    if not df_cr.empty :
        model_cr = Models('Stage 1').model
        pred_cr = model_cr.predict(df_cr.iloc[:,1:3])
        prob_cr = model_cr.predict_proba(df_cr.iloc[:,1:3])

        df_cr['prob_success'] = prob_cr[:,1]
        df_cr['prob_cancel'] = prob_cr[:,0]
        l.append(df_cr[['id', 'prob_success', 'prob_cancel']])

    #Prediction for Training
    df_t = df_t[['id', 'total_time']]
    df_t.dropna(inplace = True)
    if not df_t.empty :
        model_t = Models('Training').model
        pred_t = model_t.predict(df_t.iloc[:,1:2])
        prob_t = model_t.predict_proba(df_t.iloc[:,1:2])
        
        df_t['prob_success'] = prob_t[:,1]
        df_t['prob_cancel'] = prob_t[:,0]
        l.append(df_t[['id', 'prob_success', 'prob_cancel']])

    #Prediction for Stage 7
    df_tp = df_tp[['id', 'Stage 7_Time', 'Stage 7_Milestone']]
    df_tp.dropna(inplace = True)
    if not df_tp.empty :
        model_tp = Models('Stage 7').model
        pred_tp = model_tp.predict(df_tp.iloc[:,1:3])
        prob_tp = model_tp.predict_proba(df_tp.iloc[:,1:3])
        
        df_tp['prob_success'] = prob_tp[:,1]
        df_tp['prob_cancel'] = prob_tp[:,0]
        l.append(df_tp[['id', 'prob_success', 'prob_cancel']])

    #Prediction for Stage 8
    df_ad = df_ad[['id', 'Stage 8_Time', 'Stage 8_Milestone']]
    df_ad.dropna(inplace = True)
    if not df_ad.empty :
        model_ad = Models('Stage 8').model
        pred_ad = model_ad.predict(df_ad.iloc[:,1:3])
        prob_ad = model_ad.predict_proba(df_ad.iloc[:,1:3])
        
        df_ad['prob_success'] = prob_ad[:,1]
        df_ad['prob_cancel'] = prob_ad[:,0]
        l.append(df_ad[['id', 'prob_success', 'prob_cancel']])

    df_results = pd.concat(l, ignore_index=True)
    #df_results.to_csv('results.csv', index = False)
    return df_results