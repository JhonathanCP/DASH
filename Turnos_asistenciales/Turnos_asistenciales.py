import dash
import os
from dash import dcc, html, Dash, DiskcacheManager, CeleryManager
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash import dash_table
import numpy as np
#from jupyter_dash import JupyterDash
#import pdfkit
# import base64
# from io import BytesIO
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import pyodbc
from urllib.parse import quote_plus
import io
import oracledb
from datetime import datetime
from dash.exceptions import PreventUpdate

# Definición de la función de conexión
def create_oracle_connection1():
    try:
        oracledb.init_oracle_client(lib_dir=r"C:\Users\kings\Downloads\Nueva carpeta\instantclient_23_4")
        connection = oracledb.connect(
            user="User_oper",
            password="TmLQL$Yq.1",
            dsn="10.56.1.76:1527/WNET"
        )
        print("Conexión exitosa.")
        return connection
    except oracledb.DatabaseError as e:
        error, = e.args
        print(f"Error al conectar: {error.message}")
        return None

# Reemplazar fetch_data por una función de ejemplo
def fetch_data1(cas,fec_ini,fec_fin,doc,limit=None):
    try:
        conn = create_oracle_connection1()
        if conn is None:
            raise ConnectionError("No se pudo establecer la conexión a la base de datos.")
        
                # Asegúrate de que las fechas estén en el formato correcto
        fec_ini = pd.to_datetime(fec_ini).strftime('%Y%m%d')
        fec_fin = pd.to_datetime(fec_fin).strftime('%Y%m%d')
        query = f"""
SELECT DISTINCT
t.COD_IPRESS,
t.IPRESS,
t.NOMBRED,
t.TIPDOCUMENTO,
t.NUMDOCUMENTO,
t.COLEGIOPROF,
t.NUMCOLEGIO,
t.APELLIDOS,
t.NOMBRES,
t.ESPECIALIDAD,
t.SERVICIO,
---dia 1 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='01'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia01_Ingreso,
--dia 1 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='01'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia01_Salida,

--dia 2 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='02'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia02_Ingreso,
--dia 2 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='02'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia02_Salida,
--dia 3 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='03'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088')  AND ROWNUM=1) AS dia03_Ingreso,
--dia 3 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='03'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia03_Salida,
--dia 4 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='04'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia04_Ingreso,
--dia 4 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='04'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia04_Salida,
--dia 5 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='05'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia05_Ingreso,
--dia 5 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='05'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia05_Salida,
--dia 6 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='06'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia06_Ingreso,
--dia 6 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='06'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia06_Salida,
--dia 7 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='07'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia07_Ingreso,
--dia 7 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='07'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia07_Salida,
--dia 8 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='08'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia08_Ingreso,
--dia 8 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='08'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia08_Salida,
--dia 9 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='09'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia09_Ingreso,
--dia 9 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='09'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia09_Salida,
--dia 10 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='10'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia10_Ingreso,
--dia 10 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='10'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia10_Salida,
--dia 11 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='11'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia11_Ingreso,
--dia 11 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='11'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia11_Salida,
--dia 12 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='12'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia12_Ingreso,
--dia 12 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='12'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia12_Salida,
--dia 13 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='13'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia13_Ingreso,
--dia 13 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='13'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia13_Salida,
--dia 14 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='14'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia14_Ingreso,
--dia 14 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='14'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia14_Salida,
--dia 15 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='15'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia15_Ingreso,
--dia 15 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='15'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia15_Salida,
--dia 16 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='16'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia16_Ingreso,
--dia 16 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='16'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia16_Salida,
--dia 17 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='17'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia17_Ingreso,
--dia 17 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='17'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia17_Salida,
--dia 18 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='18'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia18_Ingreso,
--dia 18 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='18'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia18_Salida,
--dia 19 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='19'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia19_Ingreso,
--dia 19 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='19'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia19_Salida,
--dia 20 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='20'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia20_Ingreso,
--dia 20 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='20'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia20_Salida,
--dia 21 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='21'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia21_Ingreso,
--dia 21 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='21'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia21_Salida,
--dia 22 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='22'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia22_Ingreso,
--dia 22 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='22'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia22_Salida,
--dia 23 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='23'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia23_Ingreso,
--dia 23 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='23'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia23_Salida,
--dia 24 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='24'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia24_Ingreso,
--dia 24 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='24'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia24_Salida,
--dia 25 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='25'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia25_Ingreso,
--dia 25 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='25'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia25_Salida,
--dia 26 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='26'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia26_Ingreso,
--dia 26 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='26'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia26_Salida,
--dia 27 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='27'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia27_Ingreso,
--dia 27 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='27'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia27_Salida,
--dia 28 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='28'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia28_Ingreso,
--dia 28 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='28'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia28_Salida,
--dia 29 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='29'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia29_Ingreso,
--dia 29 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='29'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia29_Salida,
--dia 30 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='30'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia30_Ingreso,
--dia 30 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='30'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia30_Salida,
--dia 31 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='31'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia31_Ingreso,
--dia 31 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='31'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia31_Salida
FROM
(SELECT
ca.cenasirenaescod                                                        AS COD_IPRESS,
ca.cenasides                                                              AS IPRESS,
(SELECT rd.redasisdes
 FROM cmras10 rd WHERE rd.redasiscod = ca.redasiscod)                     AS NOMBRED,
CASE WHEN a.tipdocidenpercod = '1' THEN
  'DNI'
ELSE
(SELECT td.tipdocidenperdescor
 FROM CBTDI10 td
 WHERE td.tipdocidenpercod = a.tipdocidenpercod)
END                                                                       AS TIPDOCUMENTO,
a.perasisdocidennum                                                       AS NUMDOCUMENTO,
DECODE((select cp.grupocupnomcor
        from CBGOC10 cp
        WHERE cp.grupocupcod = m.grupocupcod),'MEDICO','CMP',
        'NUTRICIONISTA','CNP','ODONTOLOGO','COP','OBSTETRIZ','COPOBS',
        'QUIMICO FARM.','CQFP','PSICOLOGO','CPSP','BIOLOGO','CBP',
        'TRAB.SOCIAL','CTSP','ENFERMERA(O)','CEP','TECN.MEDICO','CTMP',
        'TECN.MED.REHAB.','CTMP','TECN.MED.S.OCU.','CTMP',
        'TECN.MED.LENG.','CTMP','TECN.MED.LABOR.','CTMP',
        'TECN.MED.RADIOL','CTMP','TECN.MED.OPTOME','CTMP')                AS COLEGIOPROF, -- SE ADD los otros grupos relacinado a Tecnologos 201023
lpad(m.perasisprocolcod,6,0)                                              AS NUMCOLEGIO,
m.perasisproapepat ||' '|| m.perasisproapemat                             AS APELLIDOS,
m.perasisproprinom ||' '|| m.perasisprosegnom                             AS NOMBRES,
/*(SELECT ces.espprofdes FROM sgss.ctepr10 ces
 WHERE ces.espprofcod = m.perespcod1)                                     AS ESPECIALIDAD,*/
(select ser.servhosdes FROM
cmsho10 ser where ser.servhoscod =  a.servhoscod)                         AS ESPECIALIDAD,
a.oricenasicod,
a.cenasicod,
a.arehoscod,
a.servhoscod,
a.actcod,
a.actespcod,
a.properturhorini,
a.properturhorfin,
a.tipdocidenpercod,
a.perasisdocidennum,
to_char(a.properfec,'yyyy-mm') AS periodo,
to_char(a.properturhorini,'HH24:mi')||'-'||to_char(a.properturhorfin,'HH24:mi')   AS TURNO,
--(select ar.arehosdes from cmaho10  ar where  ar.arehoscod = a.arehoscod)          AS SERVICIO
DECODE((select ar.arehosdes from cmaho10  ar where  ar.arehoscod = a.arehoscod),
        'CONSULTA EXTERNA','UPSS - CONSULTA EXTERNA',
        'URGENCIAS / EMERGENCIA','UPSS - EMERGENCIA',
        'HOSPITALIZACION','UPSS - HOSPITALIZACION',
        'AYUDA AL DIAGNOSTICO', 'UPSS - AYUDA AL DIAGNOSTICO',
        'CENTRO QUIRURGICO', 'UPSS - CENTRO QUIRURGICO',
        'CENTRO OBSTETRICO','UPSS - CENTRO OBSTETRICO',
        'AREAS ADMINISTRATIVAS','UPSS - AREAS ADMINISTRATIVAS',
        'UNIDAD DE CUIDADOS INTENSIVOS','UPSS - UNIDAD DE CUIDADOS INTENSIVOS',
        'UNIDAD DE CUIDADOS INTERMEDIOS','UPSS - UNIDAD DE CUIDADOS INTERMEDIOS',
        'HOSPITALIZACION AMBULATORIA','UPSS - HOSPITALIZACION AMBULATORIA',
        'UNIDAD DE CUIDADOS CORONARIOS','UPSS - UNIDAD DE CUIDADOS CORONARIOS')     AS SERVICIO
FROM ctppe10 a
left outer join cmprs10 m on m.tipdocidenpercod  = a.tipdocidenpercod
                         and m.perasisdocidennum = a.perasisdocidennum
LEFT OUTER JOIN cmcas10 ca ON ca.oricenasicod = a.oricenasicod
                          AND ca.cenasicod    = a.cenasicod
WHERE
    a.oricenasicod = '1'
AND a.cenasicod    = '{cas}'
AND a.properfec    >= TO_DATE('{fec_ini}', 'yyyymmdd')
AND a.properfec    < TO_DATE('{fec_fin}', 'yyyymmdd') +1
AND a.properestregcod = '1'
AND ('{doc}' IS NULL OR '{doc}' = '' OR a.perasisdocidennum = '{doc}')
AND m.grupocupcod IN ('09','02','03','07','04','08','05','01','06','20','21','22','23','24','25','26') --Profesionales con Colegiatura indicada -- SE ADD los otros grupos relacinado a Tecnologos
ORDER BY to_char(properfec,'dd/mm/yyyy'),to_char(a.properturhorini, 'hh24:mi'),to_char(a.properturhorfin, 'hh24:mi'),a.tipdocidenpercod,a.perasisdocidennum, ESPECIALIDAD,SERVICIO
)t

        """
        if limit:
            query = f"""
SELECT * FROM (SELECT DISTINCT
t.COD_IPRESS,
t.IPRESS,
t.NOMBRED,
t.TIPDOCUMENTO,
t.NUMDOCUMENTO,
t.COLEGIOPROF,
t.NUMCOLEGIO,
t.APELLIDOS,
t.NOMBRES,
t.ESPECIALIDAD,
t.SERVICIO,
---dia 1 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='01'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia01_Ingreso,
--dia 1 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='01'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia01_Salida,

--dia 2 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='02'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia02_Ingreso,
--dia 2 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='02'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia02_Salida

FROM

(SELECT
ca.cenasirenaescod                                                        AS COD_IPRESS,
ca.cenasides                                                              AS IPRESS,
(SELECT rd.redasisdes
 FROM cmras10 rd WHERE rd.redasiscod = ca.redasiscod)                     AS NOMBRED,
CASE WHEN a.tipdocidenpercod = '1' THEN
  'DNI'
ELSE
(SELECT td.tipdocidenperdescor
 FROM CBTDI10 td
 WHERE td.tipdocidenpercod = a.tipdocidenpercod)
END                                                                       AS TIPDOCUMENTO,
a.perasisdocidennum                                                       AS NUMDOCUMENTO,
DECODE((select cp.grupocupnomcor
        from CBGOC10 cp
        WHERE cp.grupocupcod = m.grupocupcod),'MEDICO','CMP',
        'NUTRICIONISTA','CNP','ODONTOLOGO','COP','OBSTETRIZ','COPOBS',
        'QUIMICO FARM.','CQFP','PSICOLOGO','CPSP','BIOLOGO','CBP',
        'TRAB.SOCIAL','CTSP','ENFERMERA(O)','CEP','TECN.MEDICO','CTMP',
        'TECN.MED.REHAB.','CTMP','TECN.MED.S.OCU.','CTMP',
        'TECN.MED.LENG.','CTMP','TECN.MED.LABOR.','CTMP',
        'TECN.MED.RADIOL','CTMP','TECN.MED.OPTOME','CTMP')                AS COLEGIOPROF, -- SE ADD los otros grupos relacinado a Tecnologos 201023
lpad(m.perasisprocolcod,6,0)                                              AS NUMCOLEGIO,
m.perasisproapepat ||' '|| m.perasisproapemat                             AS APELLIDOS,
m.perasisproprinom ||' '|| m.perasisprosegnom                             AS NOMBRES,
/*(SELECT ces.espprofdes FROM sgss.ctepr10 ces
 WHERE ces.espprofcod = m.perespcod1)                                     AS ESPECIALIDAD,*/
(select ser.servhosdes FROM
cmsho10 ser where ser.servhoscod =  a.servhoscod)                         AS ESPECIALIDAD,
a.oricenasicod,
a.cenasicod,
a.arehoscod,
a.servhoscod,
a.actcod,
a.actespcod,
a.properturhorini,
a.properturhorfin,
a.tipdocidenpercod,
a.perasisdocidennum,
to_char(a.properfec,'yyyy-mm') AS periodo,
to_char(a.properturhorini,'HH24:mi')||'-'||to_char(a.properturhorfin,'HH24:mi')   AS TURNO,
--(select ar.arehosdes from cmaho10  ar where  ar.arehoscod = a.arehoscod)          AS SERVICIO
DECODE((select ar.arehosdes from cmaho10  ar where  ar.arehoscod = a.arehoscod),
        'CONSULTA EXTERNA','UPSS - CONSULTA EXTERNA',
        'URGENCIAS / EMERGENCIA','UPSS - EMERGENCIA',
        'HOSPITALIZACION','UPSS - HOSPITALIZACION',
        'AYUDA AL DIAGNOSTICO', 'UPSS - AYUDA AL DIAGNOSTICO',
        'CENTRO QUIRURGICO', 'UPSS - CENTRO QUIRURGICO',
        'CENTRO OBSTETRICO','UPSS - CENTRO OBSTETRICO',
        'AREAS ADMINISTRATIVAS','UPSS - AREAS ADMINISTRATIVAS',
        'UNIDAD DE CUIDADOS INTENSIVOS','UPSS - UNIDAD DE CUIDADOS INTENSIVOS',
        'UNIDAD DE CUIDADOS INTERMEDIOS','UPSS - UNIDAD DE CUIDADOS INTERMEDIOS',
        'HOSPITALIZACION AMBULATORIA','UPSS - HOSPITALIZACION AMBULATORIA',
        'UNIDAD DE CUIDADOS CORONARIOS','UPSS - UNIDAD DE CUIDADOS CORONARIOS')     AS SERVICIO
FROM ctppe10 a
left outer join cmprs10 m on m.tipdocidenpercod  = a.tipdocidenpercod
                         and m.perasisdocidennum = a.perasisdocidennum
LEFT OUTER JOIN cmcas10 ca ON ca.oricenasicod = a.oricenasicod
                          AND ca.cenasicod    = a.cenasicod
WHERE
    a.oricenasicod = '1'
AND a.cenasicod    = '{cas}'
AND a.properfec    >= TO_DATE('{fec_ini}', 'yyyymmdd')
AND a.properfec    < TO_DATE('{fec_fin}', 'yyyymmdd') +1
AND a.properestregcod = '1'
AND ('{doc}' IS NULL OR '{doc}' = '' OR a.perasisdocidennum = '{doc}')
AND m.grupocupcod IN ('09','02','03','07','04','08','05','01','06','20','21','22','23','24','25','26') --Profesionales con Colegiatura indicada -- SE ADD los otros grupos relacinado a Tecnologos
ORDER BY to_char(properfec,'dd/mm/yyyy'),to_char(a.properturhorini, 'hh24:mi'),to_char(a.properturhorfin, 'hh24:mi'),a.tipdocidenpercod,a.perasisdocidennum, ESPECIALIDAD,SERVICIO
)t) WHERE ROWNUM <={limit}

        """

        print("Consulta SQL generada:")
        print(query)  # Agrega esto para depurar la consulta SQL

        df = pd.read_sql(query, conn)
        print(f"Cantidad de filas obtenidas: {len(df)}")  # Agrega esto para ver cuántas filas se obtienen
    except Exception as e:
        print(f"Error: {e}")
        df = pd.DataFrame()
    finally:
        if conn:
            conn.close()  # Cerrar la conexión al final
    return df



def fetch_data2(cas,fec_ini,doc,limit=None):
    try:
        conn = create_oracle_connection1()
        if conn is None:
            raise ConnectionError("No se pudo establecer la conexión a la base de datos.")
        
                # Asegúrate de que las fechas estén en el formato correcto
        fec_ini = pd.to_datetime(fec_ini).strftime('%Y%m%d')
        query = f"""
SELECT DISTINCT
t.COD_IPRESS,
t.IPRESS,
t.NOMBRED,
t.TIPDOCUMENTO,
t.NUMDOCUMENTO,
t.COLEGIOPROF,
t.NUMCOLEGIO,
t.APELLIDOS,
t.NOMBRES,
t.ESPECIALIDAD,
t.SERVICIO,
---dia 1 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='01'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia01_Ingreso,
--dia 1 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='01'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia01_Salida,

--dia 2 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='02'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia02_Ingreso,
--dia 2 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='02'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia02_Salida,
--dia 3 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='03'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088')  AND ROWNUM=1) AS dia03_Ingreso,
--dia 3 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='03'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia03_Salida,
--dia 4 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='04'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia04_Ingreso,
--dia 4 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='04'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia04_Salida,
--dia 5 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='05'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia05_Ingreso,
--dia 5 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='05'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia05_Salida,
--dia 6 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='06'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia06_Ingreso,
--dia 6 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='06'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia06_Salida,
--dia 7 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='07'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia07_Ingreso,
--dia 7 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='07'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia07_Salida,
--dia 8 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='08'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia08_Ingreso,
--dia 8 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='08'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia08_Salida,
--dia 9 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='09'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia09_Ingreso,
--dia 9 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='09'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia09_Salida,
--dia 10 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='10'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia10_Ingreso,
--dia 10 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='10'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia10_Salida,
--dia 11 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='11'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia11_Ingreso,
--dia 11 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='11'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia11_Salida,
--dia 12 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='12'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia12_Ingreso,
--dia 12 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='12'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia12_Salida,
--dia 13 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='13'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia13_Ingreso,
--dia 13 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='13'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia13_Salida,
--dia 14 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='14'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia14_Ingreso,
--dia 14 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='14'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia14_Salida,
--dia 15 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='15'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia15_Ingreso,
--dia 15 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='15'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia15_Salida,
--dia 16 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='16'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia16_Ingreso,
--dia 16 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='16'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia16_Salida,
--dia 17 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='17'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia17_Ingreso,
--dia 17 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='17'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia17_Salida,
--dia 18 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='18'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia18_Ingreso,
--dia 18 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='18'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia18_Salida,
--dia 19 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='19'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia19_Ingreso,
--dia 19 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='19'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia19_Salida,
--dia 20 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='20'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia20_Ingreso,
--dia 20 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='20'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia20_Salida,
--dia 21 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='21'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia21_Ingreso,
--dia 21 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='21'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia21_Salida,
--dia 22 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='22'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia22_Ingreso,
--dia 22 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='22'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia22_Salida,
--dia 23 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='23'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia23_Ingreso,
--dia 23 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='23'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia23_Salida,
--dia 24 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='24'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia24_Ingreso,
--dia 24 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='24'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia24_Salida,
--dia 25 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='25'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia25_Ingreso,
--dia 25 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='25'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia25_Salida,
--dia 26 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='26'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia26_Ingreso,
--dia 26 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='26'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia26_Salida,
--dia 27 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='27'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia27_Ingreso,
--dia 27 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='27'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia27_Salida,
--dia 28 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='28'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia28_Ingreso,
--dia 28 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='28'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia28_Salida,
--dia 29 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='29'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia29_Ingreso,
--dia 29 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='29'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia29_Salida,
--dia 30 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='30'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia30_Ingreso,
--dia 30 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='30'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia30_Salida,
--dia 31 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='31'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia31_Ingreso,
--dia 31 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='31'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia31_Salida
FROM
(SELECT
ca.cenasirenaescod                                                        AS COD_IPRESS,
ca.cenasides                                                              AS IPRESS,
(SELECT rd.redasisdes
 FROM cmras10 rd WHERE rd.redasiscod = ca.redasiscod)                     AS NOMBRED,
CASE WHEN a.tipdocidenpercod = '1' THEN
  'DNI'
ELSE
(SELECT td.tipdocidenperdescor
 FROM CBTDI10 td
 WHERE td.tipdocidenpercod = a.tipdocidenpercod)
END                                                                       AS TIPDOCUMENTO,
a.perasisdocidennum                                                       AS NUMDOCUMENTO,
DECODE((select cp.grupocupnomcor
        from CBGOC10 cp
        WHERE cp.grupocupcod = m.grupocupcod),'MEDICO','CMP',
        'NUTRICIONISTA','CNP','ODONTOLOGO','COP','OBSTETRIZ','COPOBS',
        'QUIMICO FARM.','CQFP','PSICOLOGO','CPSP','BIOLOGO','CBP',
        'TRAB.SOCIAL','CTSP','ENFERMERA(O)','CEP','TECN.MEDICO','CTMP',
        'TECN.MED.REHAB.','CTMP','TECN.MED.S.OCU.','CTMP',
        'TECN.MED.LENG.','CTMP','TECN.MED.LABOR.','CTMP',
        'TECN.MED.RADIOL','CTMP','TECN.MED.OPTOME','CTMP')                AS COLEGIOPROF, -- SE ADD los otros grupos relacinado a Tecnologos 201023
lpad(m.perasisprocolcod,6,0)                                              AS NUMCOLEGIO,
m.perasisproapepat ||' '|| m.perasisproapemat                             AS APELLIDOS,
m.perasisproprinom ||' '|| m.perasisprosegnom                             AS NOMBRES,
/*(SELECT ces.espprofdes FROM sgss.ctepr10 ces
 WHERE ces.espprofcod = m.perespcod1)                                     AS ESPECIALIDAD,*/
(select ser.servhosdes FROM
cmsho10 ser where ser.servhoscod =  a.servhoscod)                         AS ESPECIALIDAD,
a.oricenasicod,
a.cenasicod,
a.arehoscod,
a.servhoscod,
a.actcod,
a.actespcod,
a.properturhorini,
a.properturhorfin,
a.tipdocidenpercod,
a.perasisdocidennum,
to_char(a.properfec,'yyyy-mm') AS periodo,
to_char(a.properturhorini,'HH24:mi')||'-'||to_char(a.properturhorfin,'HH24:mi')   AS TURNO,
--(select ar.arehosdes from cmaho10  ar where  ar.arehoscod = a.arehoscod)          AS SERVICIO
DECODE((select ar.arehosdes from cmaho10  ar where  ar.arehoscod = a.arehoscod),
        'CONSULTA EXTERNA','UPSS - CONSULTA EXTERNA',
        'URGENCIAS / EMERGENCIA','UPSS - EMERGENCIA',
        'HOSPITALIZACION','UPSS - HOSPITALIZACION',
        'AYUDA AL DIAGNOSTICO', 'UPSS - AYUDA AL DIAGNOSTICO',
        'CENTRO QUIRURGICO', 'UPSS - CENTRO QUIRURGICO',
        'CENTRO OBSTETRICO','UPSS - CENTRO OBSTETRICO',
        'AREAS ADMINISTRATIVAS','UPSS - AREAS ADMINISTRATIVAS',
        'UNIDAD DE CUIDADOS INTENSIVOS','UPSS - UNIDAD DE CUIDADOS INTENSIVOS',
        'UNIDAD DE CUIDADOS INTERMEDIOS','UPSS - UNIDAD DE CUIDADOS INTERMEDIOS',
        'HOSPITALIZACION AMBULATORIA','UPSS - HOSPITALIZACION AMBULATORIA',
        'UNIDAD DE CUIDADOS CORONARIOS','UPSS - UNIDAD DE CUIDADOS CORONARIOS')     AS SERVICIO
FROM ctppe10 a
left outer join cmprs10 m on m.tipdocidenpercod  = a.tipdocidenpercod
                         and m.perasisdocidennum = a.perasisdocidennum
LEFT OUTER JOIN cmcas10 ca ON ca.oricenasicod = a.oricenasicod
                          AND ca.cenasicod    = a.cenasicod
WHERE
    a.oricenasicod = '1'
AND a.cenasicod    = '{cas}'
AND a.properfec    >= TO_DATE('{fec_ini}', 'yyyymmdd')
AND a.properfec    < TO_DATE('{fec_ini}', 'yyyymmdd') +1
AND a.properestregcod = '1'
AND ('{doc}' IS NULL OR '{doc}' = '' OR a.perasisdocidennum = '{doc}')
AND m.grupocupcod IN ('09','02','03','07','04','08','05','01','06','20','21','22','23','24','25','26') --Profesionales con Colegiatura indicada -- SE ADD los otros grupos relacinado a Tecnologos
ORDER BY to_char(properfec,'dd/mm/yyyy'),to_char(a.properturhorini, 'hh24:mi'),to_char(a.properturhorfin, 'hh24:mi'),a.tipdocidenpercod,a.perasisdocidennum, ESPECIALIDAD,SERVICIO
)t

        """
        if limit:
            query = f"""
SELECT * FROM (SELECT DISTINCT
t.COD_IPRESS,
t.IPRESS,
t.NOMBRED,
t.TIPDOCUMENTO,
t.NUMDOCUMENTO,
t.COLEGIOPROF,
t.NUMCOLEGIO,
t.APELLIDOS,
t.NOMBRES,
t.ESPECIALIDAD,
t.SERVICIO,
---dia 1 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='01'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia01_Ingreso,
--dia 1 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='01'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia01_Salida,

--dia 2 turno ini
(SELECT to_char(p01.properturhorini,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='02'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia02_Ingreso,
--dia 2 turno Fin
(SELECT to_char(p01.properturhorfin,'hh24:mi') FROM sgss.ctppe10 p01
 WHERE t.oricenasicod=p01.oricenasicod and t.cenasicod=p01.cenasicod and t.arehoscod=p01.arehoscod and t.servhoscod=p01.servhoscod
 and t.actcod=p01.actcod and t.actespcod=p01.actespcod and t.tipdocidenpercod=p01.tipdocidenpercod and t.perasisdocidennum=p01.perasisdocidennum
AND t.periodo = to_char(p01.properfec,'yyyy-mm') and t.properturhorini=p01.properturhorini and t.properturhorfin= p01.properturhorfin AND to_char(p01.properfec,'dd')='02'
AND p01.estprogcitcod = '2' AND p01.actespcod NOT IN ('119','088') AND ROWNUM=1) AS dia02_Salida

FROM

(SELECT
ca.cenasirenaescod                                                        AS COD_IPRESS,
ca.cenasides                                                              AS IPRESS,
(SELECT rd.redasisdes
 FROM cmras10 rd WHERE rd.redasiscod = ca.redasiscod)                     AS NOMBRED,
CASE WHEN a.tipdocidenpercod = '1' THEN
  'DNI'
ELSE
(SELECT td.tipdocidenperdescor
 FROM CBTDI10 td
 WHERE td.tipdocidenpercod = a.tipdocidenpercod)
END                                                                       AS TIPDOCUMENTO,
a.perasisdocidennum                                                       AS NUMDOCUMENTO,
DECODE((select cp.grupocupnomcor
        from CBGOC10 cp
        WHERE cp.grupocupcod = m.grupocupcod),'MEDICO','CMP',
        'NUTRICIONISTA','CNP','ODONTOLOGO','COP','OBSTETRIZ','COPOBS',
        'QUIMICO FARM.','CQFP','PSICOLOGO','CPSP','BIOLOGO','CBP',
        'TRAB.SOCIAL','CTSP','ENFERMERA(O)','CEP','TECN.MEDICO','CTMP',
        'TECN.MED.REHAB.','CTMP','TECN.MED.S.OCU.','CTMP',
        'TECN.MED.LENG.','CTMP','TECN.MED.LABOR.','CTMP',
        'TECN.MED.RADIOL','CTMP','TECN.MED.OPTOME','CTMP')                AS COLEGIOPROF, -- SE ADD los otros grupos relacinado a Tecnologos 201023
lpad(m.perasisprocolcod,6,0)                                              AS NUMCOLEGIO,
m.perasisproapepat ||' '|| m.perasisproapemat                             AS APELLIDOS,
m.perasisproprinom ||' '|| m.perasisprosegnom                             AS NOMBRES,
/*(SELECT ces.espprofdes FROM sgss.ctepr10 ces
 WHERE ces.espprofcod = m.perespcod1)                                     AS ESPECIALIDAD,*/
(select ser.servhosdes FROM
cmsho10 ser where ser.servhoscod =  a.servhoscod)                         AS ESPECIALIDAD,
a.oricenasicod,
a.cenasicod,
a.arehoscod,
a.servhoscod,
a.actcod,
a.actespcod,
a.properturhorini,
a.properturhorfin,
a.tipdocidenpercod,
a.perasisdocidennum,
to_char(a.properfec,'yyyy-mm') AS periodo,
to_char(a.properturhorini,'HH24:mi')||'-'||to_char(a.properturhorfin,'HH24:mi')   AS TURNO,
--(select ar.arehosdes from cmaho10  ar where  ar.arehoscod = a.arehoscod)          AS SERVICIO
DECODE((select ar.arehosdes from cmaho10  ar where  ar.arehoscod = a.arehoscod),
        'CONSULTA EXTERNA','UPSS - CONSULTA EXTERNA',
        'URGENCIAS / EMERGENCIA','UPSS - EMERGENCIA',
        'HOSPITALIZACION','UPSS - HOSPITALIZACION',
        'AYUDA AL DIAGNOSTICO', 'UPSS - AYUDA AL DIAGNOSTICO',
        'CENTRO QUIRURGICO', 'UPSS - CENTRO QUIRURGICO',
        'CENTRO OBSTETRICO','UPSS - CENTRO OBSTETRICO',
        'AREAS ADMINISTRATIVAS','UPSS - AREAS ADMINISTRATIVAS',
        'UNIDAD DE CUIDADOS INTENSIVOS','UPSS - UNIDAD DE CUIDADOS INTENSIVOS',
        'UNIDAD DE CUIDADOS INTERMEDIOS','UPSS - UNIDAD DE CUIDADOS INTERMEDIOS',
        'HOSPITALIZACION AMBULATORIA','UPSS - HOSPITALIZACION AMBULATORIA',
        'UNIDAD DE CUIDADOS CORONARIOS','UPSS - UNIDAD DE CUIDADOS CORONARIOS')     AS SERVICIO
FROM ctppe10 a
left outer join cmprs10 m on m.tipdocidenpercod  = a.tipdocidenpercod
                         and m.perasisdocidennum = a.perasisdocidennum
LEFT OUTER JOIN cmcas10 ca ON ca.oricenasicod = a.oricenasicod
                          AND ca.cenasicod    = a.cenasicod
WHERE
    a.oricenasicod = '1'
AND a.cenasicod    = '{cas}'
AND a.properfec    >= TO_DATE('{fec_ini}', 'yyyymmdd')
AND a.properfec    < TO_DATE('{fec_ini}', 'yyyymmdd') +1
AND a.properestregcod = '1'
AND ('{doc}' IS NULL OR '{doc}' = '' OR a.perasisdocidennum = '{doc}')
AND m.grupocupcod IN ('09','02','03','07','04','08','05','01','06','20','21','22','23','24','25','26') --Profesionales con Colegiatura indicada -- SE ADD los otros grupos relacinado a Tecnologos
ORDER BY to_char(properfec,'dd/mm/yyyy'),to_char(a.properturhorini, 'hh24:mi'),to_char(a.properturhorfin, 'hh24:mi'),a.tipdocidenpercod,a.perasisdocidennum, ESPECIALIDAD,SERVICIO
)t) WHERE ROWNUM <={limit}

        """

        print("Consulta SQL generada:")
        print(query)  # Agrega esto para depurar la consulta SQL

        df = pd.read_sql(query, conn)
        print(f"Cantidad de filas obtenidas: {len(df)}")  # Agrega esto para ver cuántas filas se obtienen
    except Exception as e:
        print(f"Error: {e}")
        df = pd.DataFrame()
    finally:
        if conn:
            conn.close()  # Cerrar la conexión al final
    return df

def get_unique_options():
    connection = create_oracle_connection1()
    if connection:
        try:
            cursor = connection.cursor()
            query = "SELECT DISTINCT CENASIDES, CENASICOD FROM CMCAS10 WHERE ORICENASICOD ='1' AND ESTREGCOD='1'"
            cursor.execute(query)
            options = [{'label': row[0], 'value': row[1]} for row in cursor.fetchall()]
            cursor.close()
            return options
        except oracledb.DatabaseError as e:
            error, = e.args
            print(f"Error al ejecutar la consulta: {error.message}")
            return []
        finally:
            connection.close()
    return []


options = get_unique_options()


external_stylesheets = [
    dbc.themes.BOOTSTRAP,
    'https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css',  # Bootstrap CSS
    'https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.1/css/all.min.css'  # Bootstrap Icons
]

# Crear la aplicación Dash
app = dash.Dash(__name__, suppress_callback_exceptions=True, external_stylesheets=external_stylesheets)

layout = dbc.Container([
    
    # Título
dbc.Row([
    # Título y logotipo en la misma fila
    dbc.Col([
        html.Div(style={'height': '12px'}),
        html.H1("SUSALUD - TURNOS ASISTENCIALES", style={'color': '#0064AF', 'fontSize': '28px', 'textAlign': 'Left', 'font-weight': 'bold', 'fontFamily': 'Calibri'}),
        html.H2("Fuente: ESSI. V.1.0.0", style={'color': '#0064AF', 'fontSize': '12px'}),
    ], width=10),  # Ajusta el ancho según sea necesario
    dbc.Col([
        html.Img(src="/assets/Logotipo sin Slogan_Horizontal_Color Transparente.png", alt="Essalud", width="150"),
    ], width=2, className="d-none d-lg-flex align-items-center justify-content-end"),
]),
    html.Hr(style={'border': '2px solid #0064AF'}),
    dbc.Row([
        # Date Picker for Start Date
        dbc.Col(
    [
        dcc.Dropdown(
            id='cas-turnos',
            options=options,
            placeholder='Seleccione un centro asistencial',
            className='px-0 mx-0',
            optionHeight=53,
            style={'height': '45px', 'width': '100%'}
        ),
    ],
    className='px-0 ml-3',
    width=9, md=3, lg=2
),


        dbc.Col(
            [
                dcc.DatePickerSingle(
                    id='Fecha inicio',
                    display_format='DD/MM/YYYY',
                    placeholder='Fecha inicio',
                    className='date-picker-custom px-0 mx-0'
                )
            ],
            className='px-0 ml-2',
            width=6, md=4, lg=1
        ),
        # Date Picker for End Date
        dbc.Col(
            [
                dcc.DatePickerSingle(
                    id='Fecha fin',
                    placeholder='Fecha fin',
                    display_format='DD/MM/YYYY',
                    className='date-picker-custom px-0 mx-0',
                )
            ],
            className='px-0 mx-0 mb-2',
            width=1, md=4, lg=1
        ),
        dbc.Col(
            [
                dcc.Input(
                    id='DNI',
                    type='text',
                    inputMode='numeric',  # Permite solo números en dispositivos móviles
                    pattern='[0-9]*',     # Expresión regular para aceptar solo dígitos
                    placeholder='DNI',
                    className='px-0 mx-0',
                    style={'height': '45px', 'width': '100%'}
                ),
            ],
            className='px-0 mx-0 mb-2',
            width=9, md=3, lg=2
        ),
        dbc.Col(
            dbc.Button(
                html.I(className="fas fa-search"),
                id='submit-val1',
                style={'background-color': '#0064AF', 'border-color': '#0064AF', 'color': 'white', 'width': '58%', 'height': '45px', 'fontSize': '20px'},
            ),
            className='mb-2',
            width=3, md=1, lg=1
        ),
        dbc.Col(
            dcc.Loading(
                id="loading-download2",
                type="default",
                children=html.Div([

                    
                    dbc.Button( 
                        [html.I(className="fas fa-file-excel"), html.Span(" Descargar datos")],
                        id='download-btn1',
                        n_clicks=0,
                        style={'background-color': '#0064AF', 'border-color': '#0064AF', 'color': 'white', 'width': '60%', 'height': '45px'}
                    ),
                    dcc.Download(id="download-dataframe-csv1")
                ]),
                style={'margin-right': '105px'}
            ),
            width=9, md=2, lg=2
        ),
    ], className="mb-4"),

    # Salida de datos con dbc.Spinner
    dbc.Spinner(
        id="loading-output1",
        size="md",
        color="primary",
        type="border",
        fullscreen=False,
        children=html.Div(id='output-data-table-tab1')
    ),
    dbc.Alert(id="error-alert1", is_open=False, dismissable=True, color="danger"),
], fluid=True)



def register_callbacks(app):

    @app.callback(
        Output('output-data-table-tab1', 'children'),
        Input('submit-val1', 'n_clicks'),
        State('Fecha inicio', 'date'),
        State('Fecha fin', 'date'),
        State('DNI', 'value'),
        State('cas-turnos', 'value')
    )
    def update_output_tab1(n_clicks, start_date, end_date, doc, cas):
        if n_clicks is None:
            return html.Div("Ingrese el rango de fecha y el centro asistencial", style={'color': '#606060', 'fontSize': '20px'})

        if n_clicks > 0:
            try:
                # Asignar una cadena vacía a 'doc' si no se proporciona un valor
                doc = doc or ""
                
                if not start_date or not end_date:
                    return html.Div("Por favor, complete las fechas.")
                
                # Si 'doc' está vacío, ejecutar con end_date, si no, con start_date
                if doc == "":
                    df = fetch_data1(cas, start_date, start_date, doc, limit=10)
                else:
                    end_date = start_date
                    df = fetch_data1(cas, start_date, end_date, doc, limit=10)

                if df.empty:
                    return html.Div("No se encontraron datos para los criterios proporcionados.")

                return html.Div([
                    html.H4("Data de muestra:", style={'color': '#0064AF', 'width': '100%', 'height': '45px', 'fontSize': '20px', 'margin-bottom':'0px'}),
                    html.H2("*Solo refleja la estructura de la información a descargar", style={'color': '#0064AF', 'fontSize': '12px','margin-top':'0px'}),
                    dash_table.DataTable(
                        id='data-table',
                        columns=[{"name": i, "id": i} for i in df.columns],
                        data=df.to_dict('records'),
                        style_table={'overflowX': 'auto', 'width': '100%',},
                        page_size=15,
                        style_cell={
                                'textAlign': 'left',
                                'fontFamily': 'Calibri',
                                'padding': '5px',
                                'height': 'auto',
                                'maxWidth': '120px',
                                'whiteSpace': 'normal',
                                'color': '#606060',
                                'fontSize': '14px'
                        },
                        style_header={
                            'backgroundColor': '#0064AF',
                            'fontWeight': 'bold',
                            'color': 'white'
                        },
                        style_data_conditional=[{'if': {'row_index': 'odd'}, 'backgroundColor': 'rgb(244, 250, 253)'}],
                    )
                ])
            except Exception as e:
                return html.Div(f'Error: {e}')

    @app.callback(
        Output("download-dataframe-csv1", "data"),
        Output("error-alert1", "children"),
        Output("error-alert1", "is_open"),
        Input("download-btn1", "n_clicks"),
        State('Fecha inicio', 'date'),
        State('Fecha fin', 'date'),
        State('DNI', 'value'),
        State('cas-turnos', 'value'),
        prevent_initial_call=True
    )
    def download_csv(n_clicks, start_date, end_date, doc,cas):
        if n_clicks > 0:
            try:
                doc = doc or ""
                if not start_date or not end_date:
                    return dcc.send_data_frame(pd.DataFrame().to_csv, filename="data_empty.csv"), None, False
                
                # Calcular la diferencia en días entre las fechas
                date_diff = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days
                
                # Validar si la diferencia es mayor a 31 días
                if date_diff > 31:
                    error_message = "Error: El rango de fechas no puede ser mayor a 31 días."
                    return None, error_message, True
                
                # Si la diferencia es válida, continuar con la descarga
                df_complete = fetch_data1(cas, start_date, end_date, doc, limit=None)
                return dcc.send_data_frame(df_complete.to_csv, filename="data_complete.csv"), None, False
            
            except Exception as e:
                return dcc.send_data_frame(pd.DataFrame().to_csv, filename="data_empty.csv"), None, False
