import pandas as pd 
from sklearn.linear_model import LogisticRegression
import sys
from model_sets import Training_Set

class Models :
    """
    Entrena un modelo de Logistic Regression con diferentes datos de entrenamiento dato el parámetro model
    Parameters
    ----------
    train: DataFrame or Array consisting of the training dataset
    labels: list of labels
    Returns
    -------
    sklearn.linear_model.LogisticRegression object
    """
    def __init__(self, model) :
        self.df = Training_Set().df
        if model == 'Stage 1' :
            self.model =  self.__stage_1()
        elif model == 'First Half' :
            self.model =  self.__first_half()
        elif model == 'Training' :
            self.model =  self.__training()
        elif model == 'Stage 7' :
            self.model =  self.__test_payroll()
        elif model == 'Stage 8' :
            self.model =  self.__Stage 8()
        else :
            print("Se deben introducir alguno de los siguientes valores: 'Stage 1', 'First Half', 'Training', 'Stage 7', 'Stage 8' ")
            sys.exit(0)

    def __stage_1(self) :
        """
        Modelo para las cuentas que se encuentren en el stage de Stage 1
        Se entrena con dos variables:
            Stage 1 Time - Tiempo que la cuenta lleva en Stage 1.
            Stage 1 Milestone - Tiempo que le tomo a la cuenta empezar Stage 1.
        Returns
        -------
        sklearn.linear_model.LogisticRegression object with 2 variables: Stage 1 Time & Stage 1 Milestone
        """
    
        df = self.df
        df_cr = df[ (df['stage_before_cancelled'].isna()) | (df['stage_before_cancelled'] == 'Stage 1') ]
        df_cr = df_cr[['id', 'Stage 1_Time', 'Stage 1_Milestone','stage_name']]
        df_cr.dropna(inplace = True)
        df_train = df_cr.iloc[:,1:3]
        labels = df_cr['stage_name']

        self.df_train = df_cr
        
        model = LogisticRegression()
        model.fit(df_train, labels)
        
        return model
    
    def __first_half(self) :
        """
        Modelo para las cuentas que se encuentren en un stage previo a Stage 5
        Se entrena con una variable:
            Stage 5 Milestone - Tiempo que la cuenta lleva en el proceso de activation.
        Returns
        -------
        sklearn.linear_model.LogisticRegression object with 1 variable: Stage 5 Milestone
        """

        df = self.df
        df_m =  df[ (df['stage_before_cancelled'].isna()) | (df['stage_before_cancelled'].isin(['Customer Intro' , 'Stage 1', 'Stage 2', 'Stage 3', 'Stage 4'])) ]
        df_m = df_m[['id', 'Stage 5_Milestone', 'time_to_close', 'stage_name']]
        
        df_m_1 = df_m[df_m['stage_name'] == 1].copy()
        df_m_2 = df_m[df_m['stage_name'] == 0].copy()
        
        df_m_2['Stage 5_Milestone'] = df_m_2['Stage 5_Milestone'].where(~pd.isna(df_m_2['Stage 5_Milestone']), df_m_2['time_to_close'])
        df_m = pd.concat([df_m_1, df_m_2], ignore_index=True)
        df_m = df_m[['id', 'Stage 5_Milestone', 'stage_name']]
        df_m.dropna(inplace = True)

        self.df_train = df_m

        model = LogisticRegression()
        model.fit(df_m.iloc[:,1:2], df_m['stage_name'])
        return model

    def __training(self) :
        """
        Modelo para las cuentas que se encuentren en los stages: Stage 5 o Stage 6
        Se entrena con una variable:
            Stage 7 Milestone - Tiempo que la cuenta lleva en el proceso de activation.
        Returns
        -------
        sklearn.linear_model.LogisticRegression object with 1 variable: Stage 7 Milestone
        """

        df = self.df
        df_3 = df[ (df['stage_before_cancelled'].isna()) | (df['stage_before_cancelled'].isin(['Stage 5' , 'Stage 6', ])) ]
        df_3 = df_3[['id', 'Stage 7_Milestone', 'time_to_close', 'stage_name']]
        
        df_3_1 = df_3[df_3['stage_name'] == 1].copy()
        df_3_2 = df_3[df_3['stage_name'] == 0].copy()
        df_3_2['Stage 7_Milestone'] = df_3_2['Stage 7_Milestone'].where(~pd.isna(df_3_2['Stage 7_Milestone']), df_3_2['time_to_close'])
        df_3 = pd.concat([df_3_1, df_3_2], ignore_index=True)
        
        df_3 = df_3[['id', 'Stage 7_Milestone', 'stage_name']]
        df_3.dropna(inplace = True)

        self.df_train = df_3
        
        model = LogisticRegression()
        model.fit(df_3.iloc[:,1:2], df_3['stage_name'])

        return model
    
    def __test_payroll(self) :
        """
        Modelo para las cuentas que se encuentren en el stage de Stage 7
        Se entrena con dos variables:
            Stage 7 Time - Tiempo que la cuenta lleva en Stage 7.
            Stage 7 Milestone - Tiempo que le tomo a la cuenta empezar Stage 7.
        Returns
        -------
        sklearn.linear_model.LogisticRegression object with 2 variables: Stage 7 Time & Stage 7 Milestone
        """

        df = self.df
        df_cr = df[ (df['stage_before_cancelled'].isna()) | (df['stage_before_cancelled'] == 'Stage 7') ]
        df_cr = df_cr[['id', 'Stage 7_Time', 'Stage 7_Milestone','stage_name']]
        df_cr.dropna(inplace = True)
        df_train = df_cr.iloc[:,1:3]
        labels = df_cr['stage_name']

        self.df_train = df_cr
        
        model = LogisticRegression()
        model.fit(df_train, labels)
        return model

    def __Stage 8(self) : 
        """
        Modelo para las cuentas que se encuentren en el stage de Stage 8
        Se entrena con dos variables:
            Stage 8 Time - Tiempo que la cuenta lleva en Stage 8.
            Stage 8 Milestone - Tiempo que le tomo a la cuenta empezar Stage 8.
        Returns
        -------
        sklearn.linear_model.LogisticRegression object with 2 variables: Stage 8 Time & Stage 8 Milestone
        """

        df = self.df
        df_cr = df[ (df['stage_before_cancelled'].isna()) | (df['stage_before_cancelled'] == 'Stage 8') ]
        df_cr = df_cr[['id', 'Stage 8_Time', 'Stage 8_Milestone','stage_name']]
        df_cr.dropna(inplace = True)
        df_train = df_cr.iloc[:,1:3]
        labels = df_cr['stage_name']

        self.df_train = df_cr
        
        model = LogisticRegression()
        model.fit(df_train, labels)

        return model