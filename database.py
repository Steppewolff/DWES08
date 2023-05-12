import pymysql.cursors
import pypyodbc
import random
import joblib
import json
import sqlalchemy as db
from dateutil.parser import parse 
import datetime
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']

class gimnas(object):

    def cargaUsuaris():
        #Conexion a la BBDD del servidor mySQL
        db = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            passwd='0Castorp0',
            db='dwes03',
            charset='utf8mb4',
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor)
        cursor=db.cursor()
        sql="SELECT * from clients"
        cursor.execute(sql)
        ResQuery=cursor.fetchall()
        db.close()
        return ResQuery

    def nouIdUsuari():
        #Conexion a la BBDD del servidor mySQL
        db = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            passwd='0Castorp0',
            db='dwes03',
            charset='utf8mb4',
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor)
        cursor=db.cursor()
        sql="SELECT MAX(idclient)+1 nouId from clients"
        cursor.execute(sql)
        ResQuery=cursor.fetchone()
        db.close()
        return ResQuery['nouId']

    def modificaUsuari(idclient,nom,llinatges,telefon):
        #Conexion a la BBDD del servidor mySQL
        db = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            passwd='0Castorp0',
            db='dwes03',
            charset='utf8mb4',
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor)
        cursor=db.cursor()
        sql="SELECT count(*) existe from clients WHERE idclient="+idclient
        cursor.execute(sql)
        result=cursor.fetchone()
        if result['existe']==1:
            sql="UPDATE clients SET nom='"+nom+"', llinatges='"+llinatges+"', telefon='"+str(telefon)+"' WHERE idclient="+idclient;
        else:
            sql="INSERT into clients values ("+idclient+",'"+nom+"','"+llinatges+"','"+str(telefon)+"')"
        cursor.execute(sql)
        db.close()

    def borraUsuari(idusuari):
        #Conexion a la BBDD del servidor mySQL
        db = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            passwd='0Castorp0',
            db='dwes03',
            charset='utf8mb4',
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor)
        cursor=db.cursor()
        sql="DELETE from clients WHERE idclient="+idusuari
        cursor.execute(sql)
        db.close()

    def cargaReservas(dia):
        #Conexion a la BBDD del servidor mySQL
        db = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            passwd='0Castorp0',
            db='dwes03',
            charset='utf8mb4',
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor)
        cursor=db.cursor()
        inici=datetime.datetime.strptime(dia,'%d-%m-%Y')
        final=inici+datetime.timedelta(days=4)
        sql="SELECT r.data,p.tipo,u.nom,u.llinatges from reserves r,pistes p,clients u WHERE "
        sql=sql+"u.idclient=r.idclient AND r.idpista=p.idpista AND "
        sql=sql+"data>='"+inici.strftime("%Y-%m-%d")+"' AND data<='"+final.strftime("%Y-%m-%d")+"';"
        cursor.execute(sql)
        ResQuery=cursor.fetchall()
        db.close()
        return ResQuery

    def cargaReservasAPI(dia):
        #Conexion a API de Google Calendar

        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

        try:
            service = build('calendar', 'v3', credentials=creds)

            # Call the Calendar API
            now = datetime.datetime.utcnow().isoformat() + 'Z'  # 'Z' indicates UTC time
            print("OK now************************************************", now)
            inici = datetime.datetime.strptime(dia,'%d-%m-%Y').isoformat() + 'Z'
            print("OK inici************************************************", inici)
            print("OK dia************************************************", dia)
            print("OK diaType************************************************", type(dia))            
            print("OK iniciType************************************************", type(inici))
            final = (datetime.datetime.strptime(dia,'%d-%m-%Y') + datetime.timedelta(days=4)).isoformat() + 'Z'
            print("OK diafinal************************************************", final)  
            print("OK finalType************************************************", type(final))   
            # final = datetime.datetime(days=4).strptime(diaFinal,'%d-%m-%Y').isoformat() + 'Z'         
            print('Getting the upcoming 10 events')

            # events_result = service.events().list(calendarId='primary', timeMin=inici, timeMax=final,
                                                # singleEvents=True, orderBy='startTime').execute()
            
            events_result = service.events().list(calendarId='primary', timeMin=inici, timeMax=final, singleEvents=True, orderBy='startTime').execute()
                                                
            events = events_result.get('items', [])
            # print("OK events************************************************", events)

            if not events:
                print('No upcoming events found.')
                return

            # Prints the start and name of the next 10 events
            for event in events:
                start = event['start'].get('dateTime', event['start'].get('date'))
                print(start, event['summary'])

            return events

        except HttpError as error:
            print('An error occurred: %s' % error)

    def reservaPista(data,idusuari,idpista):
        #Conexion a la BBDD del servidor mySQL
        db = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            passwd='0Castorp0',
            db='dwes03',
            charset='utf8mb4',
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor)
        cursor=db.cursor()
        sql="INSERT INTO reserves VALUES('"+data+"',"+idpista+","+idusuari+");"
        cursor.execute(sql)
        ResQuery=cursor.fetchall()
        db.close()

    def comprovaPista(dia,hora,idpista):
        #Conexion a la BBDD del servidor mySQL
        db = pymysql.connect(
            host='127.0.0.1',
            port=3306,
            user='root',
            passwd='0Castorp0',
            db='dwes03',
            charset='utf8mb4',
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor)
        cursor=db.cursor()
        fecha=dia+" "+str(hora)+":00:00"
        sql="SELECT count(*) p from reserves WHERE data='"+fecha+"' and idpista="+idpista+";"
        cursor.execute(sql)
        ResQuery=cursor.fetchone()
        reservado=ResQuery['p']
        print(reservado)
        db.close()
        return reservado
