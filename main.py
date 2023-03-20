from fastapi import FastAPI, UploadFile
from typing import Union
from fastapi.responses import RedirectResponse
import numpy as np
import pandas as pd
import json
#from typing import Annotated

import parameters
#from flask import *
#import requests
#import pandasql
#from pandasql import sqldf
#from numpy import numpy
#pysqldf = lambda q: sqldf(q, globals())

app = FastAPI()

#titles = pd.read_csv(r"datasets/titles.csv")
    

@app.get("/")
def index():
    return "hello world"
    #return RedirectResponse("https://titles-pg-deploy.onrender.com/docs")


@app.post("/post_upload_titles")
async def post_upload_titles(fileAmazon: UploadFile, fileDisney: UploadFile, fileHulu: UploadFile, fileNetflix: UploadFile):
    df_amazon = pd.read_csv(fileAmazon.file)
    df_amazon["platform"] = "amazon"
    df_disney = pd.read_csv(fileDisney.file)
    df_disney["platform"] = "disney"
    df_hulu = pd.read_csv(fileHulu.file)
    df_hulu["platform"] = "hulu"
    df_netflix = pd.read_csv(fileNetflix.file)
    df_netflix["platform"] = "netflix"
    df_titles = pd.concat([df_amazon,df_disney,df_hulu,df_netflix])
    # procesasamiento -------------01--------------------
    # Generar campo id: Cada id se compondrá de la primera letra del nombre de la plataforma, 
    # seguido del show_id ya presente en los datasets (ejemplo para títulos de Amazon = as123)
    df_titles["id"] = df_titles["platform"].str.slice(0,1) + df_titles["show_id"]
    # procesasamiento -------------02--------------------
    # Los valores nulos del campo rating deberán reemplazarse por el string “G” 
    # (corresponde al maturity rating: “general for all audiences”
    df_titles["rating"] = df_titles["rating"].replace(np.nan,'G')
    # procesasamiento -------------03--------------------
    # De haber fechas, deberán tener el formato AAAA-mm-dd
    df_titles["date_added"] = pd.to_datetime(df_titles["date_added"]).dt.strftime('%Y-%m-%d')
    # procesasamiento -------------04--------------------
    #Los campos de texto deberán estar en minúsculas, sin excepciones
    df_titles['show_id'] = df_titles['show_id'].str.lower()
    df_titles['type'] = df_titles['type'].str.lower()
    df_titles['title'] = df_titles['title'].str.lower()
    df_titles['director'] = df_titles['director'].str.lower()
    df_titles['cast'] = df_titles['cast'].str.lower()
    df_titles['country'] = df_titles['country'].str.lower()
    df_titles['rating'] = df_titles['rating'].str.lower()
    df_titles['duration'] = df_titles['duration'].str.lower()
    df_titles['listed_in'] = df_titles['listed_in'].str.lower()
    df_titles['description'] = df_titles['description'].str.lower()
    df_titles['platform'] = df_titles['platform'].str.lower()
    df_titles['id'] = df_titles['id'].str.lower()
    # procesasamiento -------------05--------------------
    #El campo duration debe convertirse en dos campos: duration_int y duration_type. 
    # El primero será un integer y el segundo un string indicando la unidad de medición 
    # de duración: min (minutos) o season (temporadas)

    #la columna duration se divide en 2 columnas duration_int y duration_type
    df_titles[['duration_int', 'duration_type']] = df_titles['duration'].str.extract('(\d+)\s*(\w+)')

    #reemplazar seasons por season
    df_titles['duration_type'] = df_titles["duration_type"].str.replace("seasons", "season")

    #ADICIONAL -- reemplazar los nulos por ceros
    df_titles['duration_int'].fillna(value=0, inplace=True)
    #ADICIONAL -- convertir los valores a enteros
    df_titles['duration_int'] = df_titles['duration_int'].astype('int64')
    
    parameters.DF_TITLES = df_titles

    return "Uploaded files successfully..."

#---1---
#Película con mayor duración con filtros opcionales de AÑO, PLATAFORMA Y TIPO DE DURACIÓN. 
#(la función debe llamarse get_max_duration(year, platform, duration_type))
@app.get('/get_max_duration')
async def get_max_duration(year:Union[int,None] = None, platform:Union[str,None]='',duration_type:Union[str,None]=''): 
    titles=parameters.DF_TITLES
    if year == None :
        df_filter = titles[(titles['platform'].str.contains(platform)) & (titles['duration_type'].str.contains(duration_type))]
    else:
        df_filter = titles[(titles['release_year']==year) & (titles['platform'].str.contains(platform)) & (titles['duration_type'].str.contains(duration_type))]
    resultado = df_filter[df_filter['duration_int'] == df_filter['duration_int'].max()]
    return json.loads(resultado.to_json(orient='records'))
        
    
#---2---
#Cantidad de películas por plataforma con un puntaje mayor a XX en determinado año 
# (la función debe llamarse get_score_count(platform, scored, year))
@app.post("/post_upload_rating")
async def post_upload_rating(files: list[UploadFile]):
    #variable lista de dataframe cargados
    df_list=[]
    #recorrer los ficheros cargados para almacenarnos en la lista
    for f in files:
        df_list.append(pd.read_csv(f.file))
    #unir los ficheros de la lista en un solo dataframe
    df_rating = pd.concat(df_list, axis=0, ignore_index=True)
    parameters.DF_RATING = df_rating
    return 'files rating uploaded successfully'

@app.get('/get_score_count/{platform}/{score}/{year}')
async def get_score_count(platform:str, score:float, year:int):
    df_rating = parameters.DF_RATING
    #
    df_rating["timestamp"] = pd.to_datetime(df_rating["timestamp"]).dt.strftime('%Y')
    df_filter =df_rating[(df_rating['timestamp'].str.contains(str(year))) & (df_rating['movieId'].str.contains(platform[0], case= False))]
    #
    df_media_rating =df_filter.groupby(['movieId']).agg('mean') 
    df_result = df_media_rating[df_media_rating['rating']> score]
    return len(df_result)

#---3---
#Cantidad de películas por plataforma con filtro de PLATAFORMA. 
# (La función debe llamarse get_count_platform(platform))
@app.get('/get_count_platform/{platform}')
def get_count_platform(platform:str):
    titles=parameters.DF_TITLES
    df_filter = titles[titles['platform']==platform]
    resultado = df_filter['id'].count()
    return int(resultado)

#---4---
#Actor que más se repite según plataforma y año. 
# (La función debe llamarse get_actor(platform, year))
@app.get('/get_actor/{plataforma}/{year}')
def get_actor(platform:str, year:int):
    titles=parameters.DF_TITLES
    df_filter = titles[(titles['platform']==platform) & (titles['release_year']==year)]
    df_filter['cast'].fillna('vacio', inplace=True)
    df_filter_no_empty = df_filter[df_filter['cast']!='vacio']
    df_filter_no_space = df_filter_no_empty['cast'].str.replace(', ',',')
    lista=[]
    for i in df_filter_no_space:
        s=i.split(',')
        for j in range(len(s)):
            if s[j] not in lista:
                lista.append(s[j])
            else:
                pass 
    lista= list(set(lista))
    count = 0
    dict = {}
    for i in lista:
        #count = 0
        for j in df_filter_no_space:
            if i in j.split(','):
                count +=1
        dict[i] = count
    actor = max(dict, key = dict.get)
    return actor + ' con ' + str(dict[actor]) + ' apariciones'



