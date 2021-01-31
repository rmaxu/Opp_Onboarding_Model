from sqlalchemy import create_engine
from env import env
import os
import datetime
import pygsheets

class Redshift() :
    """
    SQL Alchemy engine
    """
    def __init__(self) :
        self.engine = create_engine("postgresql+psycopg2://{user}:{contr}@{host}:{port}/{base}".format( user = os.environ['REDSHIFT_USER'], 
                                                                                            contr= os.environ["REDSHIFT_PASSWORD"],
                                                                                            port= os.environ["REDSHIFT_PORT"],
                                                                                            base= os.environ["REDSHIFT_DB"], 
                                                                                            host= os.environ["REDSHIFT_HOST"] ), 
                               connect_args={'options': '-csearch_path={schema}'.format( schema = os.environ["REDSHIFT_SCHEMA"] )}, echo = False)

class Sheets() :
    """
    Methods for google sheets using pygsheets
    """
    def Insert(self, dat, s) :
        con = pygsheets.authorize(service_account_file= "client_secret.json")
        spdsheet = con.open_by_url(os.environ["gsheet_url"])
        sheet = spdsheet.worksheet_by_title(s)
        sheet.clear(start = "A1")
        #sheet.add_rows(len(dat) - sheet.rows)
        sheet.set_dataframe(dat, start = "A1", nan = '', extend = True)
    def Get_DF(self, s) :
        con = pygsheets.authorize(service_account_file= "client_secret.json")
        spdsheet = con.open_by_url(os.environ["gsheet_url"])
        sheet = spdsheet.worksheet_by_title(s)
        df = sheet.get_as_df()
        return df
    def update_cells(self, s, lista) :
        con = pygsheets.authorize(service_account_file= "client_secret.json")
        spdsheet = con.open_by_url(os.environ["gsheet_url"])
        sheet = spdsheet.worksheet_by_title(s)
        sheet.clear(start = "A1")
        sheet.update_col(1, lista)
    def update_value(self, sheet, addr, val) :
        con = pygsheets.authorize(service_account_file= "client_secret.json")
        spdsheet = con.open_by_url(os.environ["gsheet_url"])
        sheet = spdsheet.worksheet_by_title(sheet)
        sheet.update_value(addr,val)

def payroll_days(date) :
    """
    Return a list of days (1,...,31) which can be considered payroll days,
    depending on weekdays
    """
    l = {15,30,31,29}
    mm = datetime.date(year=int(str(date)[:4]), month =int(str(date)[5:7]), day=15)
    if date.month == 2 :
        em = datetime.date(year=int(str(date)[:4]), month =int(str(date)[5:7]), day=28)
        l.add(28)
    else :
        em = datetime.date(year=int(str(date)[:4]), month =int(str(date)[5:7]), day=30)
    
    wd = mm
    i = 2
    while i > 0 :
        wd = wd - datetime.timedelta(days=1)
        if wd.isoweekday() not in [6,7] :
            l.add(wd.day)
            i = i-1
            
    wd = em
    i = 2
    while i > 0 :
        wd = wd - datetime.timedelta(days=1)
        if wd.isoweekday() not in [6,7] :
            l.add(wd.day)
            i = i-1
    return l

class add_days() :
    def __init__(self, date, days, payroll_mode) :
        """
        Parameters
        ----------
        date : TYPE datetime.date
            Fecha a partir de la cual se sumaran los dias.
        days : TYPE int
            Cantidad de dias a agregar.
        payroll_mode : int
            Entero con valores 1, 2 y 3:
                0. Se sumaran dias ignorando las fechas de nomina.
                1. Se sumaran dias siempre y cuando estos no sean fechas nomina.
                2. Se sumaran dias solo cuando estos se encuentren dentro de un periodo de nomina
        Returns
        -------
        None.
        """
        self.weekends = [6,7]
        self.holidays = ["01-01", "02-03", "03-21", "05-01", "09-16", "11-20", "12-25"]
        if payroll_mode == 0 :
            self.new_date = self.__adding(date, days)
        elif payroll_mode == 1 :
            self.new_date = self.__adding_1(date, days)
        elif payroll_mode == 2 :
            self.new_date = self.__adding_2(date, days)
        else :
            self.new_date = self.__adding_3(date, days)
    def __adding(self, date, days):
        if days > 0 :
            aux_date = date + datetime.timedelta(days=1)
            if aux_date.isoweekday() not in self.weekends and str(aux_date)[-5:] not in self.holidays  :
                return self.__adding(aux_date, days-1)
            else :
                return self.__adding(aux_date, days)
        else :
            return date
        
    def __adding_1(self, date, days):
        if days > 0 :
            aux_date = date + datetime.timedelta(days=1)
            if aux_date.isoweekday() not in self.weekends and str(aux_date)[-5:] not in self.holidays and  aux_date.day not in payroll_days(aux_date) :
                return self.__adding_1(aux_date, days-1)
            else :
                return self.__adding_1(aux_date, days)
        else :
            return date
        
    def __adding_2(self, date, days):
        if days > 0 :
            aux_date = date + datetime.timedelta(days=1)
            if aux_date.isoweekday() not in self.weekends and str(aux_date)[-5:] not in self.holidays and  aux_date.day in payroll_days(aux_date) :
                aux_date_2 = self.__adding(aux_date, 1)
                if aux_date_2.isoweekday() not in self.weekends and str(aux_date_2)[-5:] not in self.holidays and  aux_date_2.day in payroll_days(aux_date_2) :
                    return self.__adding(aux_date_2, days-2)
                else :
                    return self.__adding_2(aux_date_2,days)
            else :
                return self.__adding_2(aux_date, days)
        else :
            return date
    def __adding_3(self, date, days):
        if days > 0 :
            aux_date = date + datetime.timedelta(days=1)
            if aux_date.isoweekday() not in self.weekends and str(aux_date)[-5:] not in self.holidays and  aux_date.day in payroll_days(aux_date) :
                return self.__adding(aux_date, days-1)
            else :
                return self.__adding_2(aux_date, days)
        else :
            return date