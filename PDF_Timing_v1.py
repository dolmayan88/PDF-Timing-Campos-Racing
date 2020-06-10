# -*- coding: utf-8 -*-
"""
Created on Sun May  5 20:43:34 2019
***Updates***

26/07/2019:
    
All F2 2019 Timings until SIL Included, are entered in PdfTiming Table
of the db. All of them are working except of F2_19R05CAS_R1. Gelael is giving issues
Amount of fields in db until that date: 7032

PDF_Timing v1 created from this file, to start with git repository

@author: aform
"""

import pandas as pd
import numpy as np
from PyPDF2 import PdfFileReader
import tabula
import datetime
import sqlalchemy
from sqlalchemy import create_engine
import math
import time
from pandas.io import sql
import os
import base64
import io
import imaplib
import email
from getpass import getpass
from datetime import date
import tkinter as tk
from tkinter import filedialog




###Libraries from Dash

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table as dt
import dash_daq as daq
from dash.dependencies import Input, Output, State


################ DASH APP LAYOUT START #################


###events entered manually at the beginning of the year###
eventsdict_16={'F2':['R07HOC']}
eventsdict_19={'F2':['R01BAH','R02BAK','R03BCN','R04MNC','R05CAS','R06AUT','R07SIL','R08BUD','R09SPA','R10MON','R11SOC','R12ABU'],'F3':['R01BCN','R02CAS','R03AUT','R04SIL','R05BUD','R06SPA','R07MON','R08SOC','R09MAC']}
eventsdict_20={'F2':['T01BAH','R01AUT','R02AUT','R03BUD','R04SIL','R05SIL','R06BCN','R07SPA','R08MON','R09BAK','R10SOC','R11BAH','R12ABU'],'F3':['R01AUT','R02AUT','R03BUD','R04SIL','R05SIL','R06BCN','R07SPA','R08MON']}
champs=list(eventsdict_19.keys())
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.Label('User Email:'),
    dcc.Input(id='Email_User',type="email",placeholder="Campos Email here"),

    html.Div(id='Email_Hidden_Div', style=dict(display = 'none')),
    
    html.Label('User Password:'),
    dcc.Input(id='Password_User',type="password",placeholder="Campos Password here"),

    html.Button(id='userpass confirm',children='Confirm User and Password'),

    html.Label('Championship:'),
    dcc.Dropdown(id='champ',
        options=[{'label': champ, 'value': champ} for champ in champs],
    value = list(eventsdict_19.keys())[0]            
    ),
    
    html.Label('Season:'), 
    dcc.Dropdown(
        id='Season',options=[
            {'label' : '2016', 'value': '2016'},
            {'label': '2019', 'value': '2019'},
            {'label': '2020', 'value': '2020'}
    ],
        value = '2019'
    ),
    
    html.Label('Event:'), 
    dcc.Dropdown(
        id='opt-events',
        value = list(eventsdict_19.values())[0][0] 
               ),

    html.Label('Select Session:'),
    dcc.Dropdown(id='Session',
        options=[
            {'label': 'P1', 'value': 'P1'},
            {'label': 'Q1', 'value': 'Q1'},
            {'label': 'R1', 'value': 'R1'},
            {'label': 'R2', 'value': 'R2'}            
        ],
        value='R1'
    ),
    html.Label('Event Naming'),
    html.Div(id='Event_Naming_Convention',),
    
    html.Label('Manual / Auto Mode'),
    daq.BooleanSwitch(id='Auto_Button',
            on=False,
            label="Auto",
            labelPosition="top"
            ),  
    
    html.Label('Select Action:'),
    dcc.RadioItems(id='Action',
        options=[
            {'label': 'Add PDFs to the DB', 'value': 'Add'},
            {'label': 'Delete PDF Timings from the DB', 'value': 'Delete'}
        ],
        value='Add'
    ),
    
    html.Div(id='Database_Info'),
    
   daq.PowerButton(
        id='my_power_button',
        on=False,
        size=100,
        label='Start',
        color='#FF5E5E'
    ),

    html.Div(id='output_power_button'),

    html.Div([
    dcc.Upload(id='Upload-TyreAlloc',children=html.Button('Upload TyreAlloc BBDD')),

    html.P(),

    html.Div(id='Output-Upload-TyreAlloc'),

    html.P(),

    dcc.Upload(id='Upload-Calendar',children=html.Button('Upload Calendar BBDD')),

    html.Div(id='Output-Upload-Calendar')])

    
], style={'columnCount': 2})
    
############## DASH LAYOUT END #####################

############## DASH CALLBACKS START #####################


def parse_contents(contents, filename, date):
    content_type, content_string = contents.split(',')

    print('content_type')
    print(content_type)

    print('content_string')
    print(content_string)

    decoded = base64.b64decode(content_string)
    print(decoded)
    print(filename)
    try:
        if 'csv' in filename:
            # Assume that the user uploaded a CSV file
            df = pd.read_csv(
                io.StringIO(decoded.decode('utf-8')))
            if 'TyreAlloc' in filename:
                db = create_engine('mysql://mf6bshg8uxot8src:nvd3akv0rndsmc6v@nt71li6axbkq1q6a.cbetxkdyhwsb.us-east-1.rds.amazonaws.com:3306/ss0isbty55bwe8te')
                conn=db.connect()

                # Empty Table before Append
                truncate_query = sqlalchemy.text("TRUNCATE TABLE TyreAlloc")
                conn.execution_options(autocommit=True).execute(truncate_query)

                df_ddbb_compounds=df.replace(np.nan,'') #Replace NaN by empty cells, if not ddbb crashes
                df_ddbb_compounds.to_sql('TyreAlloc', con=db, if_exists='append',index=False)
                conn.close()
                db.dispose()

            elif 'Calendar' in filename:
                db = create_engine(
                    'mysql://mf6bshg8uxot8src:nvd3akv0rndsmc6v@nt71li6axbkq1q6a.cbetxkdyhwsb.us-east-1.rds.amazonaws.com:3306/ss0isbty55bwe8te')
                conn = db.connect()
                #Empty Table before Append

                truncate_query = sqlalchemy.text("TRUNCATE TABLE Calendar")
                conn.execution_options(autocommit=True).execute(truncate_query)

                df_ddbb_compounds = df.replace(np.nan, '')  # Replace NaN by empty cells, if not ddbb crashes
                df_ddbb_compounds.to_sql('Calendar', con=db, if_exists='append',index=False)
                conn.close()
                db.dispose()

        elif 'xls' in filename:
            # Assume that the user uploaded an excel file
            df = pd.read_excel(io.BytesIO(decoded))
            print(df)
    except Exception as e:
        print(e)
        return html.Div([
            'There was an error processing this file.'
        ])
    if 'Calendar' in filename:
        return html.Div([
            html.H4('BBDD has been updated succesfully with the next Calendar Table'),

            dt.DataTable(
                data=df.to_dict('records'),
                columns=[{'name': i, 'id': i} for i in df.columns]
            )])
    elif 'TyreAlloc' in filename:
        return html.Div([
            html.H4('BBDD has been updated succesfully with the next Tyre Allocation Table'),

            dt.DataTable(
                data=df.to_dict('records'),
                columns=[{'name': i, 'id': i} for i in df.columns]
            )])

@app.callback(Output('Output-Upload-Calendar', 'children'),
              [Input('Upload-Calendar', 'contents')],
              [State('Upload-Calendar', 'filename'),
               State('Upload-Calendar', 'last_modified')])
def update_output(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = [
            parse_contents(list_of_contents, list_of_names, list_of_dates)
            ]
        return children

@app.callback(Output('Output-Upload-TyreAlloc', 'children'),
              [Input('Upload-TyreAlloc', 'contents')],
              [State('Upload-TyreAlloc', 'filename'),
               State('Upload-TyreAlloc', 'last_modified')])
def update_output(list_of_contents, list_of_names, list_of_dates):
    if list_of_contents is not None:
        children = [
            parse_contents(list_of_contents, list_of_names, list_of_dates)
            ]
        return children


@app.callback(Output('output_power_button','children'),
    [Input('champ', 'value'),
    Input('Season','value'),
    Input('Session', 'value'),
    Input('opt-events', 'value'),
    Input('Action', 'value'),
    Input('Event_Naming_Convention','children'),
    Input('my_power_button','on'),
    Input('Auto_Button','on')])

def database_operations(champ,season,session,event,action,naming_convention,power_button_mode,auto_button_mode):
    global entrylist_df,RaceSectorAnalysis_df,RaceHistoryChart_df,db_df
    database_connection()
    # timing_filepath = 'C:/Users/Alvaro/Desktop/Git Repos/Timing Campos Racing/OfficialTimingF1_PDF/'+champ+'/'+season #path campos pc
    # timing_filepath = 'C:/Users/aform/Desktop/Git Projects/Timing Campos Racing/OfficialTimingF1_PDF/' + champ path surface
    list_pdf_flag,has_ones=check_pdf_flag(naming_convention) #list with 4 booleans, FP/Q/R1/R2. iF PDF Flags exists is 1 if not 0


    #If an element of the list has one, it has to exist already the entrylist in the ddbb

            
    
    if power_button_mode:
        if auto_button_mode == False:
            timing_filepath = GetFolderPath()
            if action == 'Add':
                if event[0] == 'R':
                    for x in os.listdir(timing_filepath):
                        if x.startswith(event[1:3]):
                            event_folder=x
                            print(event_folder)
        #                break #escaping from the for loop once has found the match with the event Nr
                            #define entrylist
                            for file in os.listdir(timing_filepath+'/'+event_folder):
                                if 'signed' in file:
                                    if not has_ones:
                                        entrylist_file=file
                                        entrylist_file_path=timing_filepath+'/'+event_folder+'/'+entrylist_file
                                        entrylist_df=Get_EntryList(entrylist_file_path)

                                        entrylist_df['session'] = naming_convention[0:11]
                                        entrylist_df.to_sql('Drivers', engine, if_exists='append', index=False)
                                        print('Entrylist added to the Drivers DDBB!')
                                else: #Case where Entrylist is introduced by hand because is coming from years previous to 2019

                                    DDBBTable='Drivers'
                                    DDBBColumn=['num','driver','license','team','driver_FIA','team_short']
                                    DDBBColumnFilter='session'
                                    DDBBValueFilter=naming_convention[0:11]
                                    entrylist_df=getPartialTable(DDBBColumn, DDBBTable, DDBBColumnFilter, DDBBValueFilter)
                                    print(entrylist_df)
                            if session == 'P1':
                                if list_pdf_flag[0]=='0':
                                    for file in os.listdir(timing_filepath+'/'+event_folder):
                                        if 'Practice' in file:
                                            Sector_file=file
                                            PracticeSectorAnalysis_file_path = timing_filepath+'/'+event_folder+'/'+Sector_file
                                            PracticeSectorAnalysis_df = Get_PracticeSectorAnalysis_File(PracticeSectorAnalysis_file_path)
                                            db_df=create_database_df(None,PracticeSectorAnalysis_df,naming_convention,session)

                                            return session + ' Session of ' + event + ' has been introduced in the database!'
                                else:
                                    return 'Session is already in the database, if you want to write it down again, first use delete function'

                            elif session == 'Q1':
                                if list_pdf_flag[1]=='0':
                                    for file in os.listdir(timing_filepath+'/'+event_folder):
                                        print(file)
                                        if "MON" not in event_folder:
                                            if 'Qualifying' in file:
                                                Sector_file=file
                                                QualySectorAnalysis_file_path = timing_filepath+'/'+event_folder+'/'+Sector_file
                                                QualySectorAnalysis_df = Get_QualySectorAnalysis_File(QualySectorAnalysis_file_path)
                                                db_df=create_database_df(None,QualySectorAnalysis_df,naming_convention,session)
                                                return session + ' Session of ' + event + ' has been introduced in the database!'
                                        else:
                                            print(file)


                                            if 'A-Qualifying' in file:
                                                print('entro aqui')
                                                group="A"
                                                groupA_file=file
                                                QualySectorAnalysis_file_path = timing_filepath+'/'+event_folder+'/'+groupA_file
                                                QualySectorAnalysis_df = Get_QualySectorAnalysis_File(QualySectorAnalysis_file_path)
                                                db_df=create_database_df(None,QualySectorAnalysis_df,naming_convention,session,group)

                                            if 'B-Qualifying' in file:
                                                group="B"
                                                groupB_file=file
                                                QualySectorAnalysis_file_path = timing_filepath+'/'+event_folder+'/'+groupB_file
                                                QualySectorAnalysis_df = Get_QualySectorAnalysis_File(QualySectorAnalysis_file_path)
                                                db_df=create_database_df(None,QualySectorAnalysis_df,naming_convention,session,group)

                                    return session + ' Session of ' + event + ' has been introduced in the database!'

                                else:
                                    return 'Session is already in the database, if you want to write it down again, first use delete function'

                            elif session == 'R1':
                                if list_pdf_flag[2]=='0':
                                    for file in os.listdir(timing_filepath+'/'+event_folder):
                                        if 'Race1History' in file:
                                            History_file=file
                                            RaceHistoryChart_file_path = timing_filepath+'/'+event_folder+'/'+History_file
                                            RaceHistoryChart_df = Get_RaceHistoryChart_File(RaceHistoryChart_file_path,entrylist_df,season)
                                        if 'Race1Sector' in file:
                                            Sector_file=file
                                            RaceSectorAnalysis_file_path = timing_filepath+'/'+event_folder+'/'+Sector_file
                                            RaceSectorAnalysis_df = Get_RaceSectorAnalysis_File(RaceSectorAnalysis_file_path,season)

                                    db_df=create_database_df(RaceHistoryChart_df,RaceSectorAnalysis_df,naming_convention,session)
                                    return session + ' Session of ' + event + ' has been introduced in the database!'
                                else:
                                    return 'Session is already in the database, if you want to write it down again, first use delete function'

                            elif session == 'R2':
                                if list_pdf_flag[3]=='0':
                                    for file in os.listdir(timing_filepath+'/'+event_folder):
                                        if 'Race2History' in file:
                                            History_file=file
                                            RaceHistoryChart_file_path = timing_filepath+'/'+event_folder+'/'+History_file
                                            RaceHistoryChart_df = Get_RaceHistoryChart_File(RaceHistoryChart_file_path,entrylist_df,season)
                                        if 'Race2Sector' in file:
                                            Sector_file=file
                                            RaceSectorAnalysis_file_path = timing_filepath+'/'+event_folder+'/'+Sector_file
                                            RaceSectorAnalysis_df = Get_RaceSectorAnalysis_File(RaceSectorAnalysis_file_path,season)

                                    db_df=create_database_df(RaceHistoryChart_df,RaceSectorAnalysis_df,naming_convention,session)
                                    return session + ' Session of ' + event + ' has been introduced in the database!'
                                else:
                                    return 'Session is already in the database, if you want to write it down again, first use delete function'

                else: # EVENT IS A TEST
                    for x in os.listdir(timing_filepath):
                        if x == event:
                            #If pdf flag exists and is 0:
                                #Introduce excel file into database
                                #Introduce Entrylist into database
                                return 'Test session has been introduced in the database'
                            #If pdf flag exists and is 1:
                                return 'Test session is already in the database. You have to delete it before adding the timing again!'
                            #Else calendar has not the session and has to be created in the csv and introduced to the ddbb with Calendar DDBB button
                                return 'Event does not exist in Calendar DDBB, please add it to Calendar csv file and add it to the DDBB with Calendar DDBB Button'

                        else:
                            continue


            else: #AUTO MODE IS ON
                df_calendar=pd.read_sql_query('SELECT * FROM `Calendar` ORDER BY `AI` DESC',engine)
                for items_calendar in range(0,len(df_calendar)):
                    if champ in df_calendar['Championship'].values[items_calendar]:
                        print(df_calendar['session_id'].values[items_calendar])
#                        if df_tracks['pdf_flag'].values[items_tracks] == 0:
                        return(df_calendar['session_id'].values[items_calendar])
            
            
        
#                    
##                
#    
##            #Boton encendido y Add seleccionado, meter codigo de escribir a la base de datos.Necesita buscar el timestamp filtrando el evento
#    if power_button_mode == False:
#        return 'Button not activated' #'Evento añadido a la base de datos!'
#        else: 'Encendido'
#            #Boton de encendido y Delete Seleccionado . Insertar aqui codigo d eborrar el evento de la base de datos
#            return 'Espere a que acabe el borrado de datos por favor'
#    else:
#        return 'Apagado'
                        
@app.callback(
    Output('opt-events', 'options'),
    [Input('champ', 'value'),
     Input('Season', 'value')]
)
def update_events_dropdown(champ,Season):
    if Season == '2016':
        eventsdict=eventsdict_16
    elif Season == '2017':
        eventsdict=eventsdict_17
    elif Season == '2018':
        eventsdict=eventsdict_18
    elif Season == '2019':
        eventsdict=eventsdict_19
    elif Season == '2020':
        eventsdict=eventsdict_20
        
    return [{'label': i, 'value': i} for i in eventsdict[champ]]

@app.callback(Output('Event_Naming_Convention', 'children'),
   [Input('champ', 'value'),
    Input('Season', 'value'),
    Input('Session', 'value'),
    Input('opt-events', 'value')]
)
def Event_Naming_Generator(champ,Season,Session,opt_events):
    
    return champ + "_" + Season[2:] + opt_events + "_" + Session

@app.callback(Output('Email_Hidden_Div','children'),
              [Input('userpass confirm','n_clicks')],
   [State('Email_User', 'value'),
    State('Password_User', 'value'),
    State('champ', 'value')]
)
def Email_Login(clicks,Email_User,Password_User,championship):
    #global main_path
    if clicks is not None and clicks>0:
        email_user = Email_User
        email_pass = Password_User
        print("hey")

        mail = imaplib.IMAP4_SSL("outlook.office365.com", '993')
        mail.login(email_user, email_pass)
        mail.select('Inbox')
        print("HEY2")
        main_path = GetFolderPath()
        dir_list = os.listdir(main_path)
        print(dir_list)
        print(main_path)
        if "last_search.txt" in dir_list: #last_search.txt exists
            print("txt file ya existe")

            text_file_path = main_path + "/" + "last_search.txt"
            with open(text_file_path, "r") as myfile:

                last_search = myfile.readlines()[0]
                last_search_date = last_search.split("_")[0]
                last_search_event_nr = last_search.split("_")[1]
                event_Nr = last_search_event_nr
                last_search_event = last_search.split("_")[2]
                event_name = last_search_event
        else:
            # Create the empty txt file in the folder
            print("Creating last_search.txt file")
            text_file_path = open(main_path + "/" + "last_search.txt", "w+")
            text_file_path.close()
            print('No hay txt file!')
            last_search_date = '01-Jan-2019'
            event_Nr = 1
            if championship == 'F2':
                event_name = 'BRN'
            else:
                event_name = 'ESP'


        ### Gathering TimeSectorAnalysis and RaceHistoryChart Files ###

        type, data = mail.search(None,
                                 '(FROM "officialtiming@F1.com" SUBJECT ' + championship + ' SINCE "' + last_search_date + '")')
        mail_ids = data[0]
        id_list = mail_ids.split()

        for nums in id_list:
            typ, data = mail.fetch(nums, '(RFC822)')
            raw_email = data[0][1]
            raw_email_string = raw_email.decode('utf-8')
            email_message = email.message_from_string(raw_email_string)

            for part in email_message.walk():
                # this part comes from the snipped I don't understand yet...
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                fileName = str(part.get_filename())
                if bool(fileName) and (('SectorAnalysis' in str(fileName)) or ('HistoryChart' in str(fileName))):
                    if str(fileName).split('_')[2] != str(event_name):
                        event_Nr = int(event_Nr) + 1
                        event_name = str(fileName).split('_')[2]
                    if not os.path.isdir(main_path + "/" + str(event_Nr).zfill(2) + "_" + str(event_name)):
                        os.mkdir(main_path + "/" + str(event_Nr).zfill(2) + "_" + event_name)
                        filePath = os.path.join(main_path + "/" + str(event_Nr).zfill(2) + "_" + str(event_name), fileName)
                    else:
                        filePath = os.path.join(main_path + "/" + str(event_Nr).zfill(2) + "_" + str(event_name), fileName)
                    if not os.path.isfile(filePath):
                        fp = open(filePath, 'wb')
                        fp.write(part.get_payload(decode=True))
                        fp.close()

        # event_Nr=event_Nr
        # event_name=event_name

        ### Gathering FIA Entry Lists ###
        type, data = mail.search(None, '(FROM "cslenzak@fia.com" SINCE "' + last_search_date + '")')
        mail_ids_ = data[0]
        mail_ids = data[0]
        id_list = mail_ids.split()

        ### Entry List ###

        for nums in id_list:
            typ, data = mail.fetch(nums, '(RFC822)')
            raw_email = data[0][1]
            raw_email_string = raw_email.decode('utf-8')
            email_message = email.message_from_string(raw_email_string)

            for part in email_message.walk():
                # this part comes from the snipped I don't understand yet...
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                fileName = str(part.get_filename())
                #        print(fileName)

                if bool(fileName) and (('signed' in str(fileName)) and (
                        'Doc' in str(fileName))):  # or ('Admitted' in str(fileName)) or ('Entry' in str(fileName))):
                    # main_path = main_path + "/" + str(event_Nr) + "_" + str(event_name)
                    filePath = os.path.join(main_path, fileName)

                    if not os.path.isfile(filePath):
                        fp = open(filePath, 'wb')
                        fp.write(part.get_payload(decode=True))
                        fp.close()

        today = date.today()
        text_update = today.strftime('%d-%b-%Y') + '_' + str(event_Nr).zfill(2) + '_' + str(event_name)
        print("Today's date:", today)
        with open(main_path + "/" + "last_search.txt", "w") as myfile:
            myfile.write(text_update)
            myfile.close()
        print('Finished')
        print(event_Nr)
        print(event_name)
        input('Press enter to exit:')

        return "Acabado"
    ####PUT EMAIL CODE HERE####

############## DASH CALLBACKS END #####################     
#Function that converts laptime in format x:xx.xxx to seconds
def convert2time(laptime):
    ''' TAB: Format_data
        This function converts a laptime to seconds as a float. The input has to be a string in the form mm:ss.t'''
    try:
        int(laptime)
    except:
        sec=laptime.split('.')[0]
        tenth=float('0.'+laptime.split('.')[1])
        laptime=time.strptime(sec,'%M:%S')
        laptime=datetime.timedelta(hours=laptime.tm_hour, minutes=laptime.tm_min, seconds=laptime.tm_sec).total_seconds()+tenth
    return float(laptime)
#Function to make a connection with the database in local or remote:
def database_connection():#user,password,ip_or_domain,db_port,db_name):
    global engine 
    engine = create_engine('mysql://mf6bshg8uxot8src:nvd3akv0rndsmc6v@nt71li6axbkq1q6a.cbetxkdyhwsb.us-east-1.rds.amazonaws.com:3306/ss0isbty55bwe8te')


#Function to get a string with the path of the selected file that user has selected
def GetFolderPath():
    root = tk.Tk()
    root.attributes("-topmost", True)   
    root.withdraw()
#    filepath = filedialog.askopenfilename(initialdir='C:\\A_CAMPOS\\F2\\2019\\Events')
    filepath = filedialog.askdirectory()
    root.destroy()
    return filepath

#It deletes from a LIST of DFs the dfs that are empty, giving as an output the
#same input list but without empty dfs (dfs with shape= (0,0))
def DeleteEmptyDataframesfromlist(df_list):
    df_list_wo_empty_dfs=[]
    
    for items in df_list:
        if items.empty or len(items)==1:
            pass
        else:
            df_list_wo_empty_dfs.append(items)
        
    return df_list_wo_empty_dfs
def Get_EntryList(file_path):

    global entrylist_df
    pdf = PdfFileReader(open(file_path,'rb'))
    num_pages=pdf.getNumPages()
    
    
    if num_pages == 1:
        #Distancia de arriba de la pagina a donde empieza la tabla en vertical.Last value used and working was 218, but in BCN was not working
        A= 295
        #Distancia de la izquierda de la pagina a donde empieza la table en horizontal.First value 25
        B= 55
        #Distancia de arriba de la pagina a donde acaba la tabla en vertical
        C=795
        #Distancia de la izquierda de la pagina a donde acaba la table en horizontal
        D=580
        #Array que contendrá los dataframes de todos los pilotos, un piloto por cada posicion ordenado por numero de coche
        entrylist_df=tabula.read_pdf(file_path, pages = str(1), multiple_tables = True,area = (A,B,C,D),guess=False)
        entrylist_df=entrylist_df[0]

        if entrylist_df.shape[1]==4:
            pass        
        else:
            del entrylist_df
            entrylist_df=tabula.read_pdf(file_path, pages = str(1), multiple_tables = True,area = (A+40,B,C,D),guess=False)
            entrylist_df=entrylist_df[0]
    if num_pages == 2:
        #Distancia de arriba de la pagina a donde empieza la tabla en vertical.Last value used and working was 218, but in BCN was not working
        A= 300
        #Distancia de la izquierda de la pagina a donde empieza la table en horizontal.First value 25
        B= 55
        #Distancia de arriba de la pagina a donde acaba la tabla en vertical
        C=820
        #Distancia de la izquierda de la pagina a donde acaba la table en horizontal
        D=580
        #Array que contendrá los dataframes de todos los pilotos, un piloto por cada posicion ordenado por numero de coche
        df_1=tabula.read_pdf(file_path, pages = str(1), multiple_tables = True,area = (A,B,C,D),guess=False)
        if df_1[0].shape[1] != 4 :
            del df_1
            df_1=tabula.read_pdf(file_path, pages = str(1), multiple_tables = True,area = (A+25,B,C,D),guess=False)
            df_2=tabula.read_pdf(file_path, pages = str(2), multiple_tables = True,area = (A-65,B,C-485,D),guess=False)
        else:
            df_2=tabula.read_pdf(file_path, pages = str(2), multiple_tables = True,area = (A-145,B,C-485,D),guess=False)
        
        entrylist_df=pd.concat([df_1[0],df_2[0]],axis=0)
    entrylist_df.columns=['num','driver','license','team']

    
    entrylist_df.index=range(0,len(entrylist_df))
    entrylist_df=entrylist_df.drop(index=0,axis=0)
    entrylist_df._set_value(7,'team','Virtuosi')
    entrylist_df._set_value(8, 'team', 'Virtuosi')
    #### FIA Name Creation####
    driver_FIA=[]
    helper_string=str()
    for elements in entrylist_df['driver']:
        elements=elements.split(" ")[1:]
        helper_string="".join(elements)[0:3].upper()
        if helper_string == 'MAN':
            helper_string = 'COR'
        if helper_string == "O’W":
            helper_string = 'OWA'
        driver_FIA.append(helper_string)
    entrylist_df['driver_FIA']=driver_FIA
    #### Team Short Name Creation####    
    team_short=[]
    team_short_str=""
    for elements in entrylist_df['team']:
        team_short_str=elements.split(" ")[0]
        if team_short_str == 'BWT':
            team_short_str='Arden'
        team_short.append(team_short_str)
    entrylist_df['team_short']=team_short

    print(entrylist_df['team'])
    print('entrylist finished!')
    print(len(entrylist_df))
    return entrylist_df

def Get_PracticeSectorAnalysis_File(file_path):
    pdf = PdfFileReader(open(file_path, 'rb'))
    num_pages = pdf.getNumPages()

    # If para saber si tabula necesita gestionar 2 pilotos por pagina o 1 piloto
    if num_pages < ((len(entrylist_df) / 2) + 1):
        drivers_per_page = 2
    else:
        drivers_per_page = 1

    print('drivers per page {}'.format(drivers_per_page))
    # Distancia de arriba de la pagina a donde empieza la tabla en vertical.Last value used and working was 218, but in BCN was not working
    A = 215
    # Distancia de la izquierda de la pagina a donde empieza la table en horizontal.First value 25
    B = 20
    # Distancia de arriba de la pagina a donde acaba la tabla en vertical
    C = 770
    # Distancia de la izquierda de la pagina a donde acaba la table en horizontal
    D = 300
    # Array que contendrá los dataframes de todos los pilotos, un piloto por cada posicion ordenado por numero de coche
    df_all = []

    for pages in range(1, num_pages + 1, 1):

        df_left = tabula.read_pdf(file_path, pages=str(pages), multiple_tables=True, area=(A, B, C, D), guess=False)
        df_right = tabula.read_pdf(file_path, pages=str(pages), multiple_tables=True, area=(A, D, C, D * 2),
                                   guess=False)

        generic_cols = ['lap', 's1', 'Speed_S1', 's2', 'Speed_S2', 's3', 'Speed_S3', 'laptime_ts']

        for elements in df_left:
            if elements.shape[1] == len(generic_cols):
                elements.columns = generic_cols
            else:

                elements = pd.DataFrame(np.empty((1, len(generic_cols))))
                elements[:] = np.nan
                elements.columns = generic_cols

        for elements in df_right:
            if elements.shape[1] == len(generic_cols):
                elements.columns = generic_cols
            else:

                elements = pd.DataFrame(np.empty((1, len(generic_cols))))
                elements[:] = np.nan
                elements.columns = generic_cols

        for dfs in df_left:
            dfs.columns = generic_cols
        for dfs in df_right:
            try:
                dfs.columns = generic_cols
            except:
                continue
        for i in range(0, len(df_left)):
            df_all.append(df_left[i])
            try:
                df_all.append(df_right[i])
            except:
                continue

    for dfs in df_all:
        dfs.index = list(range(1, dfs.shape[0] + 1))
    df_all = DeleteEmptyDataframesfromlist(df_all)
    print(df_all)
    print('finish practice sectoranalysis complete!')
    return df_all

def Get_QualySectorAnalysis_File(file_path):
    pdf = PdfFileReader(open(file_path, 'rb'))
    num_pages = pdf.getNumPages()

    # If para saber si tabula necesita gestionar 2 pilotos por pagina o 1 piloto
    if num_pages < ((len(entrylist_df) / 2) + 1):
        drivers_per_page = 2
    else:
        drivers_per_page = 1

    print('drivers per page {}'.format(drivers_per_page))
    # Distancia de arriba de la pagina a donde empieza la tabla en vertical.Last value used and working was 218, but in BCN was not working
    A = 215
    # Distancia de la izquierda de la pagina a donde empieza la table en horizontal.First value 25
    B = 20
    # Distancia de arriba de la pagina a donde acaba la tabla en vertical
    C = 770
    # Distancia de la izquierda de la pagina a donde acaba la table en horizontal
    D = 300
    # Array que contendrá los dataframes de todos los pilotos, un piloto por cada posicion ordenado por numero de coche
    df_all = []

    for pages in range(1, num_pages + 1, 1):

        df_left = tabula.read_pdf(file_path, pages=str(pages), multiple_tables=True, area=(A, B, C, D), guess=False)
        df_right = tabula.read_pdf(file_path, pages=str(pages), multiple_tables=True, area=(A, D, C, D * 2),
                                   guess=False)

        generic_cols = ['lap', 's1', 'Speed_S1', 's2', 'Speed_S2', 's3', 'Speed_S3', 'laptime_ts']

        for elements in df_left:
            if elements.shape[1] == len(generic_cols):
                elements.columns = generic_cols
            else:

                elements = pd.DataFrame(np.empty((1, len(generic_cols))))
                elements[:] = np.nan
                elements.columns = generic_cols

        for elements in df_right:
            if elements.shape[1] == len(generic_cols):
                elements.columns = generic_cols
            else:
                elements = pd.DataFrame(np.empty((1, len(generic_cols))))
                elements[:] = np.nan
                elements.columns = generic_cols




        for dfs in df_left:
            dfs.columns = generic_cols
        for dfs in df_right:
            try:
                dfs.columns = generic_cols
            except:
                continue
        for i in range(0,len(df_left)):
            df_all.append(df_left[i])
            try:
                df_all.append(df_right[i])
            except:
                continue


    for dfs in df_all:
        dfs.index = list(range(1, dfs.shape[0] + 1))
    df_all = DeleteEmptyDataframesfromlist(df_all)
    print(df_all)
    print('finish qualy sectoranalysis complete!')
    return df_all

def Get_RaceSectorAnalysis_File(file_path,season):
       
    pdf = PdfFileReader(open(file_path,'rb'))
    num_pages=pdf.getNumPages()
    
#If para saber si tabula necesita gestionar 2 pilotos por pagina o 1 piloto    
    if num_pages<((len(entrylist_df)/2)+1):
        drivers_per_page=2
    else:
        drivers_per_page=1
    if int(season) > 2017:
        print('drivers per page {}'.format(drivers_per_page))
        #Distancia de arriba de la pagina a donde empieza la tabla en vertical.Last value used and working was 218, but in BCN was not working
        A= 215
        #Distancia de la izquierda de la pagina a donde empieza la table en horizontal.First value 25
        B= 20
        #Distancia de arriba de la pagina a donde acaba la tabla en vertical
        C=770
        #Distancia de la izquierda de la pagina a donde acaba la table en horizontal
        D=300
    elif int(season) == 2016:
        print('drivers per page {}'.format(drivers_per_page))
        # Distancia de arriba de la pagina a donde empieza la tabla en vertical.Last value used and working was 218, but in BCN was not working
        A = 245
        # Distancia de la izquierda de la pagina a donde empieza la table en horizontal.First value 25
        B = 10
        # Distancia de arriba de la pagina a donde acaba la tabla en vertical
        C = 680
        # Distancia de la izquierda de la pagina a donde acaba la table en horizontal
        D = 300

    #Array que contendrá los dataframes de todos los pilotos, un piloto por cada posicion ordenado por numero de coche
    df_all=[]
    
    for pages in range(1,num_pages+1,1):
    
        df_left = tabula.read_pdf(file_path, pages = str(pages), multiple_tables = True,area = (A,B,C,D),guess=False)
        df_right = tabula.read_pdf(file_path, pages = str(pages), multiple_tables = True,area = (A,D,C,D*2),guess=False)
        
        generic_cols=['lap','s1','Speed_S1','s2','Speed_S2','s3','Speed_S3','laptime_ts']
        
        
        for elements in df_left:
            if elements.shape[1] == len(generic_cols):
                elements.columns = generic_cols
            else:
            
                elements=pd.DataFrame(np.empty((1,len(generic_cols))))
                elements[:]=np.nan
                elements.columns = generic_cols
            
        for elements in df_right:
            if elements.shape[1] == len(generic_cols):
                elements.columns = generic_cols
            else:
            
                elements=pd.DataFrame(np.empty((1,len(generic_cols))))
                elements[:]=np.nan
                elements.columns = generic_cols
            
        if (drivers_per_page == 1) and (len(df_left)==2) and (len(df_right)==2):
            df_left_up=df_left[0]
            df_left_down=df_left[1]
            df_right_up=df_right[0]
            df_right_down=df_right[1]
            df_left_up.columns = generic_cols
            df_left_down.columns = generic_cols
            df_right_up.columns = generic_cols
            df_right_down.columns = generic_cols
            df_driver_up=pd.concat([df_left_up,df_right_up])
            df_driver_down=pd.concat([df_left_down,df_right_down])
            df_all.append(df_driver_up)
            df_all.append(df_driver_down)
            
            del df_driver_up,df_driver_down
        elif  drivers_per_page == 1 and len(df_left)==2 and len(df_right)==1:
            df_left_up=df_left[0]
            df_left_down=df_left[1]
            df_driver_up=pd.concat([df_left_up,df_right[0]])
            df_driver_down=df_left_down
            df_driver_up.columns = generic_cols
            df_driver_down.columns = generic_cols
            df_all.append(df_driver_up)
            df_all.append(df_driver_down)
            del df_driver_up,df_driver_down
        elif drivers_per_page == 1: 
            df_driver_up=pd.concat([df_left[0],df_right[0]])
            df_driver_up.columns = generic_cols
            df_all.append(df_driver_up)
            del df_left,df_right
        if drivers_per_page == 2 and len(df_left)==2 and len(df_right)==2:
            df_left_up=df_left[0]
            df_left_down=df_left[1]
            df_right_up=df_right[0]
            df_right_down=df_right[1]
            df_left_up.columns = generic_cols
            df_left_down.columns = generic_cols
            df_right_up.columns = generic_cols
            df_right_down.columns = generic_cols
            df_all.append(df_left_up)
            df_all.append(df_right_up)
            df_all.append(df_left_down)
            df_all.append(df_right_down)
            del df_left_up,df_left_down,df_right_up,df_right_down
        elif  drivers_per_page == 2 and len(df_left)==2 and len(df_right)==1:           
            df_left_up=df_left[0]
            df_left_down=df_left[1]
            df_left_up.columns = generic_cols
            df_left_down.columns = generic_cols
            df_right[0].columns = generic_cols
            df_all.append(df_left_up)
            df_all.append(df_right[0])
            df_all.append(df_left_down)
            del df_left_up,df_right,df_left_down
        elif drivers_per_page == 2: 
            
            df_all.append(df_left[0])
            df_all.append(df_right[0])

    for dfs in df_all:        
        dfs.index=list(range(1,dfs.shape[0]+1))
    df_all=DeleteEmptyDataframesfromlist(df_all)
    print('finish racesectoranalysis complete!')
    return df_all

def Get_RaceHistoryChart_File(file_path,entrylist_df,season):
    global Car_Nr_Driver_Dict #Car_Nr_dict,Driver_Name_Dict,Car_Nr
    df_all=[]
    
        
    pdf = PdfFileReader(open(file_path,'rb'))
    num_pages=pdf.getNumPages()
    if int(season) > 2017:
        print('season mayor que 2017!!')
        #Distancia de arriba de la pagina a donde empieza la tabla en vertical
        A= 170
        #Distancia de la izquierda de la pagina a donde empieza la table en horizontal
        B= 35
        #Distancia de arriba de la pagina a donde acaba la tabla en vertical
        C=700
        #Distancia de la izquierda de la pagina a donde acaba la table en horizontal
        D=570
        #Columns per page
        Col_page=5
        #Area Width
        Area_Width=110
    elif int(season) < 2018:
        print('season menor que 2018!!!')
        # Distancia de arriba de la pagina a donde empieza la tabla en vertical
        A = 220
        # Distancia de la izquierda de la pagina a donde empieza la table en horizontal
        B = 29.5
        # Distancia de arriba de la pagina a donde acaba la tabla en vertical
        C = 700
        # Distancia de la izquierda de la pagina a donde acaba la table en horizontal
        D = 570
        # Columns per page
        Col_page = 5
        # Area Width
        Area_Width = 106

    #Array que contendrá los dataframes de todos los pilotos, un piloto por cada posicion ordenado por numero de coche
    df_all=[]
    #lista de numeros de coche, que será el indice:
    Car_Nr_Driver_Dict=dict(zip(entrylist_df['num'],entrylist_df['driver_FIA']))
    for pages in range(1,num_pages+1,1):
        
        df_1 = tabula.read_pdf(file_path, pages = str(pages), multiple_tables = True,area = (A,B,C,B+Area_Width),guess=False)
        df_2 = tabula.read_pdf(file_path, pages = str(pages), multiple_tables = True,area = (A,B+Area_Width,C,B+2*Area_Width),guess=False)
        df_3 = tabula.read_pdf(file_path, pages = str(pages), multiple_tables = True,area = (A,B+2*Area_Width,C,B+3*Area_Width),guess=False)
        df_4 = tabula.read_pdf(file_path, pages = str(pages), multiple_tables = True,area = (A,B+3*Area_Width,C,B+4*Area_Width),guess=False)
        df_5 = tabula.read_pdf(file_path, pages = str(pages), multiple_tables = True,area = (A,B+4*Area_Width,C,B+5*Area_Width),guess=False)
            
        histchart_cols=['num','gap','laptime']
            
        df_1=df_1[0]
        df_2=df_2[0]
        df_3=df_3[0]
        df_4=df_4[0]
        df_5=df_5[0]    
        
        df_all.append(df_1)
        df_all.append(df_2)
        df_all.append(df_3)
        df_all.append(df_4)
        df_all.append(df_5)
        
        del df_1,df_2,df_3,df_4,df_5
            
    df_all=DeleteEmptyDataframesfromlist(df_all)
    
    for items in df_all:
        
        items.columns=histchart_cols
        #delete all gaps
        del(items['gap'])
        items.index=list(range(1,items.shape[0]+1))
        items['driver']=entrylist_df['driver_FIA']
        for columns in items:
            items[columns]=pd.to_numeric(items[columns],errors='ignore')
        num_list=list(items['num'])
        if int(season) < 2018: #fix for issue seen in 2016 with old format. when gap has 3 digits 100.xx, number and gap are appearing in one column (num column) and it crashes
            i=0
            for nums in num_list:
                if '.' in str(nums):
                    split_car_nr_by_nth_digit=int(input('Choose how many characters the car number has in this string: ' + str(nums)))
                    num_list[i]=int(str(nums).split(".")[0][:split_car_nr_by_nth_digit]) #fix tool and asign numbers

                i=i+1
            items['num']=num_list
            for columns in items:
                items[columns] = pd.to_numeric(items[columns], errors='ignore')

            print(num_list)


        driver=[]
        for numbers in num_list:
            driver.append(Car_Nr_Driver_Dict[str(numbers)])
        items['driver']=driver

    for items in df_all:
        items['laptime_s']=items.laptime.apply(lambda x:convert2time(x))
    print('finish historychart complete!')
    return df_all  
        

# List of dataframes with the historychart per car. If the car does not 
# appeared, a dataframe has to be entered in the position that corresponds
# to the driver.
def historychart_per_car(RaceHistoryChart_df,car_list):
    historychart_per_car_df_list=[]
    temp_df=pd.DataFrame()
    for values in car_list:
        for items in RaceHistoryChart_df:
            temp_df=pd.concat([temp_df,items[items['num']==int(values)]])
#        temp_df.index=list(range(1,items.shape[0]+1))
        historychart_per_car_df_list.append(temp_df)
        temp_df=pd.DataFrame()

        
    return historychart_per_car_df_list

def merge_dataframes(df_list1,df_list2):
    merged_df_list=[]
    if len(df_list1) == len(df_list2):
        for i in range(0,len(df_list1)):
            merged_df_list.append(pd.concat([df_list1[i],df_list2[i]],axis=1,sort=False))
            
    return merged_df_list 

#Check sector analysis and cars_historychart and if there is one empty dataframe in
#cars_historychart, means that there and len of both lists is differente means 
#that an empty dataframe has to be added to the sector analysys
def check_dataframes(cars_historychart,RaceSectorAnalysis_df):
    counter_sector=0
    counter_history=0
#    counter=0
    for dfs in RaceSectorAnalysis_df:
        if dfs.shape[0] == 0 or dfs.shape[0] == 1:
            del RaceSectorAnalysis_df[counter_sector]
            counter_sector=counter_sector+1
            continue
        if dfs['laptime_ts'][len(dfs)] == 'INCOMPLETE' or dfs['laptime_ts'][len(dfs)] == 'INCOMPLET':
            RaceSectorAnalysis_df[counter_sector]=dfs.drop([len(dfs)],axis=0)
            counter_sector=counter_sector+1
            continue
        counter_sector=counter_sector+1
    while len(RaceSectorAnalysis_df) != len(cars_historychart):
        for items in cars_historychart:
            if items.shape[0] == 0:
                del cars_historychart[counter_history]
            counter_history=counter_history+1

    print(len(RaceSectorAnalysis_df))  
    print(len(cars_historychart)) 
     
    return RaceSectorAnalysis_df,cars_historychart

def randomlist_to_integerslist(lista):
    lista_new=[]
    pit_flag=[]
    counter=0
    for elem in lista:
#check if the elements are strings. If not they are converted to strings
        if elem is str:
            pass
        else:
            elem=str(elem)
        
        if elem.isdigit():
            lista_new.append(int(elem))
            pit_flag.append(0)
        elif elem is math.nan: ###########################
            lista_new.append(int(counter+1))
        else:
            b=""
            helper=list(elem)
            for digits in helper:
                if digits.isdigit():
                    b=b.__add__(digits)
                else:
                    pass
            pit_flag.append(1)
            if b != "":
                lista_new.append(int(b))
            
            else:
                pass
        counter=counter+1
    return lista_new,pit_flag

def flagcounter(lista):
    flagcounter=[]
    counter=0
    
    for elem in lista:       
        
        if elem == 1:
            counter=counter+1
        flagcounter.append(counter)

    return flagcounter

def get_track_filter_from_user():
    global db_track_filter
    db_track_filter=input('Write here the event and session that will be used as a filter to get the timestamp."Example: F2_19R06AUT_R1"')
    
    return db_track_filter



def get_ts_from_tracks(db_track_filter):
    global ts_pdftiming
    try:    
        ts_pdftiming=pd.read_sql_query('SELECT `ts` FROM `Tracks` WHERE `session` LIKE ' +  "'"+ db_track_filter + "'",engine).values              
        ts_pdftiming=np.int(ts_pdftiming)
        
    except TypeError:
        print('This track filter does not exist in the database!')
        db_track_filter=get_track_filter_from_user()
        get_ts_from_tracks(db_track_filter)
    
    return ts_pdftiming
            
def check_pdf_flag(naming_convention):
    list_pdf_flag=[]
    has_ones=False
    try:
        if "MNC" not in naming_convention:
            for items in ['P1','Q1','R1','R2']:
                temp_pdf_flag=pd.read_sql_query("SELECT `pdf_flag` FROM `Calendar` WHERE `session_id` LIKE '" + naming_convention[0:-2] + items +"'",engine).values [0][0]
                list_pdf_flag.append(temp_pdf_flag)
        else:
            for items in ['P1','Q1','Q2','R1','R2']:
                temp_pdf_flag=pd.read_sql_query("SELECT `pdf_flag` FROM `Calendar` WHERE `session_id` LIKE '" + naming_convention[0:-2] + items +"'",engine).values [0][0]
                list_pdf_flag.append(temp_pdf_flag)

            
    except:
        #Event does not exist in the calendar page. It is created in the calendar page
        if "MNC" not in naming_convention:
            list_pdf_flag=[0,0,0,0]
        else:
            list_pdf_flag = [0, 0, 0, 0, 0]


    finally:
        for items in list_pdf_flag:
            if items == '1':
                has_ones = True
                break
            else:
                continue

        print('PDF_Flag_has_one item is {}'.format(has_ones))
        print('list_pdf_flag is{}'.format(list_pdf_flag))
        return list_pdf_flag,has_ones
            
    
        

def create_database_df(RaceHistoryChart_df,RaceSectorAnalysis_df,naming_convention,session,group=""):
##RaceHistorychart per driver for calculating cumsum
    global dfs
    rawtiming_flag=getPartialTable('rawtiming_flag','Calendar','session_id',naming_convention).values[0][0]
    print(rawtiming_flag)
    if rawtiming_flag == '1':
        sc_laps_leader = sc_laps(naming_convention) #list of laps with SC inside rawtiming data
    else: #raw timing does not exist, see pirelli report from the race and enter the laps manually
        sc_laps_leader=input('Enter separated by commas and without spaces SC/VSC Laps:')
        sc_laps_leader=list(int(items) for items in sc_laps_leader.split(','))
        #no rawtiming exists. All laps all clear
    if session=='R1' or session=='R2':

        max_laps=len(RaceHistoryChart_df)
        RaceHistoryChart_df_all=pd.DataFrame()
        for items in RaceHistoryChart_df:
            RaceHistoryChart_df_all=pd.concat([RaceHistoryChart_df_all,items])
        RaceHistoryChart_drivers_df_list=[]
        for drivers in list(Car_Nr_Driver_Dict.values()):
            RaceHistoryChart_drivers_df_list.append(RaceHistoryChart_df_all[RaceHistoryChart_df_all.driver==drivers])
    ###########################################
        RaceHistoryChart_drivers_df_list=[i for i in RaceHistoryChart_drivers_df_list if len(i)>0]
        
        for items in RaceHistoryChart_drivers_df_list:
            items.reset_index(level=0,inplace=True)
        for items in RaceHistoryChart_drivers_df_list:
            items.rename(columns={"index":'position'},inplace=True)
    ############################################    
        
        for items in RaceHistoryChart_drivers_df_list:
            try:
                items['cumsum']=items['laptime_s'].cumsum()
            except IndexError:
                continue
        for items in RaceHistoryChart_drivers_df_list:        
            try:
                items['lap']=list(range(1,len(items)+1))
            except:
                pass
        RaceHistoryChart_df_all=pd.DataFrame()
        for items in RaceHistoryChart_drivers_df_list:        
            try:
                RaceHistoryChart_df_all=pd.concat([RaceHistoryChart_df_all,items])
            except TypeError:
                continue
        RaceHistoryChart_laps_df_list=[]
        for laps in list(range(1,max_laps+1)):
            RaceHistoryChart_laps_df_list.append(RaceHistoryChart_df_all[RaceHistoryChart_df_all.lap==laps])
        RaceHistory_laps_df_list_orderbyposition=[]
        for items in RaceHistoryChart_laps_df_list:
            RaceHistory_laps_df_list_orderbyposition.append(items.sort_values(by = 'cumsum'))
        for items in RaceHistory_laps_df_list_orderbyposition:
            
            try:
                items['position']=list(range(1,len(items)+1))
            except:
                pass
            items['gap']=items['cumsum']-items['cumsum'].min()
            items['gap'].values[0]=0
            items['interval']=items['gap'].diff()
            del items['cumsum']
            del items['lap']        
    ##list of dataframes with the historychart of every car. Later this list and
    ##racesectoranalysis df list will be concatenated per index.
        cars_historychart=historychart_per_car(RaceHistory_laps_df_list_orderbyposition,entrylist_df['num'])
        for items in cars_historychart:
            items.index=list(range(1,items.shape[0]+1))
        cars_historychart=[i for i in cars_historychart if len(i)>0]
        RaceSectorAnalysis_df,cars_historychart=check_dataframes(cars_historychart,RaceSectorAnalysis_df)
        counter=0
        for dfs in RaceSectorAnalysis_df:
            if dfs.shape[1] == 8:
                laps=[]
                pit_flag=[]
                [laps,pit_flag]=randomlist_to_integerslist(dfs['lap'])
                while len(laps) != len(dfs):
                    dfs=dfs.drop([len(dfs)],axis=0)
                dfs['lap']=laps
                while len(pit_flag) != len(laps):
                    del pit_flag[-1]
                dfs['InPit']=pit_flag
                dfs['pits']=flagcounter(pit_flag)


                del laps,pit_flag          
            else:
                del dfs,cars_historychart[counter]
                    
        db_df=merge_dataframes(cars_historychart,RaceSectorAnalysis_df)

       
    #    3 Lines not needed since we are using as a reference session_id, NOT ts
    #    from Tracks database. Now we are using session_id
    #    db_track_filter=naming_convention
    #    print(naming_convention)
    #    ts=get_ts_from_tracks(db_track_filter)
        
        for dfs in db_df:
            del dfs['laptime_ts']
            del dfs['laptime_s']

            dfs['session']=naming_convention
            if isinstance(sc_laps_leader, list):
                print(sc_laps_leader)
                try:
                    dfs['trackstatus'] = dfs.apply(lambda row: label_sc_laps(row, sc_laps_leader), axis=1)
                except:
                    pass

            else:
                dfs['trackstatus'] = 'AllClear'
            dfs.to_sql('PdfTiming',engine,if_exists='append')
            
            update_pdf_flag(naming_convention)
        print('finished!')
    else: #session is P1 or Q1 or Q2
        driver_counter=0
        if group == "A":
            driverlist_A=entrylist_df['driver_FIA'][entrylist_df['num'].astype(int) % 2 != 0]
            numlist_A=entrylist_df['num'][entrylist_df['num'].astype(int) % 2 != 0]
            print(driverlist_A,numlist_A)
        if group == "B":
            driverlist_B = entrylist_df['driver_FIA'][entrylist_df['num'].astype(int) % 2 == 0]
            numlist_B = entrylist_df['num'][entrylist_df['num'].astype(int) % 2 == 0]
            print(driverlist_B,numlist_B)

        for dfs in RaceSectorAnalysis_df:

            laps=[]
            pit_flag=[]
            [laps,pit_flag]=randomlist_to_integerslist(dfs['lap'])
            while len(laps) != len(dfs):
                dfs=dfs.drop([len(dfs)],axis=0)
            dfs['lap']=laps
            while len(pit_flag) != len(laps):
                del pit_flag[-1]
            if group == "A":
                dfs['driver'] = driverlist_A.values[driver_counter]
                dfs['num'] = numlist_A.values[driver_counter]
            elif group == "B":
                dfs['driver'] = driverlist_B.values[driver_counter]
                dfs['num'] = numlist_B.values[driver_counter]
            else:
                dfs['driver'] = entrylist_df['driver_FIA'].values[driver_counter]
                dfs['num'] = entrylist_df['num'].values[driver_counter]

            driver_counter = driver_counter +1
            dfs['InPit']=pit_flag
            dfs['pits']=flagcounter(pit_flag)
            dfs['trackstatus']='AllClear'
            del laps,pit_flag
        # db_df=RaceSectorAnalysis_df
        # for dfs in db_df:
            dfs['laps']=list(range(1,len(dfs)+1))
            dfs['lap']=dfs['laps']
            del dfs['laps']
            dfs['laptime']=dfs['laptime_ts']
            del dfs['laptime_ts']

            dfs['session']=naming_convention
            dfs.to_sql('PdfTiming',engine,if_exists='append')

            if group=="B":
                update_pdf_flag(naming_convention[0:-2]+"Q2")
            else:
                update_pdf_flag(naming_convention)
        print('finished!')
def sc_laps(naming_convention):

    rawtiming_dfs = getTotalTable('RawTiming','session', naming_convention)
    if 'SCDeployed' or 'VSCDeployed' in rawtiming_dfs['trackstatus'][rawtiming_dfs['position'] == 1].unique():
        sc_laps = rawtiming_dfs['lap'][rawtiming_dfs['position'] == 1 & rawtiming_dfs['trackstatus'].str.contains('Deployed')].values.tolist()
        print(sc_laps)
        return sc_laps
    else:
        return 0

def label_sc_laps (row,sc_laps):

    print(sc_laps)
    try:
        if int(row['lap']) in sc_laps:
            return 'SCDeployed'
        else:
            return 'AllClear'
    except:
        pass


    return sc_laps


def update_pdf_flag(naming_convention):
    row_id=pd.read_sql_query("SELECT `AI` FROM `Calendar` WHERE `session_id` = '" + naming_convention + "'",engine).values[0][0]
    Update_Query_UPDATE = "UPDATE `Calendar` SET "
    Update_Query_1 = "`pdf_flag` = '1'"
    Upadate_Query_ID = " WHERE `Calendar`.`AI` = "+str(row_id)
    update_query = Update_Query_UPDATE+Update_Query_1+Upadate_Query_ID
    sql.execute(update_query,engine)
    
    
def getPartialTable(DDBBColumn, DDBBTable, DDBBColumnFilter=None, DDBBValueFilter=None):
    db = create_engine(
        'mysql://mf6bshg8uxot8src:nvd3akv0rndsmc6v@nt71li6axbkq1q6a.cbetxkdyhwsb.us-east-1.rds.amazonaws.com:3306/ss0isbty55bwe8te')
    conn = db.connect()
    if (DDBBColumnFilter == None) and (DDBBValueFilter == None):
        if isinstance(DDBBColumn, list):
            DDBB_df = pd.read_sql_query("SELECT " + ",".join(DDBBColumn) + " FROM `" + DDBBTable + "'", db)

        if (str.lower(DDBBColumn) == 'all') or (str.lower(DDBBColumn) == '*'):
            DDBB_df = pd.read_sql_query("SELECT * FROM `" + DDBBTable + "`", db)

        else:
            DDBB_df = pd.read_sql_query("SELECT " + DDBBColumn + " FROM `" + DDBBTable + "'", db)

        conn.close()
        db.dispose()
        if len(DDBB_df) > 0:
            return DDBB_df
        else:
            print("No hay ningún dato en la BBDD perteneciente a esos filtros.")
    else:
        if isinstance(DDBBColumn, list):
            DDBB_df = pd.read_sql_query("SELECT " + ",".join(
                DDBBColumn) + " FROM `" + DDBBTable + "` WHERE `" + DDBBColumnFilter + "` LIKE '" + DDBBValueFilter + "'",
                                        db)

        elif (str.lower(DDBBColumn) == 'all') or (str.lower(DDBBColumn) == '*'):
            DDBB_df = pd.read_sql_query(
                "SELECT * FROM `" + DDBBTable + "` WHERE `" + DDBBColumnFilter + "` LIKE '" + DDBBValueFilter + "'", db)

        else:
            DDBB_df = pd.read_sql_query(
                "SELECT " + DDBBColumn + " FROM `" + DDBBTable + "` WHERE `" + DDBBColumnFilter + "` LIKE '" + DDBBValueFilter + "'",
                db)
        conn.close()
        db.dispose()
        if len(DDBB_df) > 0:
            return DDBB_df
        else:
            print("No hay ningún dato en la BBDD perteneciente a esos filtros.")



def getTotalTable(DDBBTable, DDBBColumnFilter, DDBBValueFilter):
    db = create_engine(
        'mysql://mf6bshg8uxot8src:nvd3akv0rndsmc6v@nt71li6axbkq1q6a.cbetxkdyhwsb.us-east-1.rds.amazonaws.com:3306/ss0isbty55bwe8te')
    conn = db.connect()
    if (DDBBColumnFilter == None) and (DDBBValueFilter == None):
        DDBB_df = pd.read_sql_query("SELECT * FROM `" + DDBBTable + "`", db)

    else:
        DDBB_df = pd.read_sql_query(
            "SELECT * FROM `" + DDBBTable + "` WHERE `" + DDBBColumnFilter + "` LIKE '" + DDBBValueFilter + "'", db)

    conn.close()
    db.dispose()

    if len(DDBB_df) > 0:
        return DDBB_df
    else:
        print("No hay ningún dato en la BBDD perteneciente a esos filtros.")


       
        
#############################     MAIN    ###############################   
                    
if __name__ == "__main__":

############# DASH APP RUN ###########

    
    app.run_server(debug=False)
    

#######################################     
    

    
  
    
    

            


