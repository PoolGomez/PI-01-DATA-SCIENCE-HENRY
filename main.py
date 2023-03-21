from fastapi import FastAPI, UploadFile
from typing import Union,List
from fastapi.responses import RedirectResponse, PlainTextResponse
import numpy as np
import pandas as pd
import json
import parameters

app = FastAPI()

@app.get("/")
def index():
    return RedirectResponse("https://pi-01-pg-titles.onrender.com/docs")




@app.post("/post_upload_titles",tags=["Paso 1: Carga de Ficheros"]) 
async def post_upload_titles(fileAmazon: UploadFile, fileDisney: UploadFile, fileHulu: UploadFile, fileNetflix: UploadFile):
    """
    cargar los ficheros amazon.csv, disney.csv, hulu.csv y netflix.csv que son necesarios y los almacena en una variable global
    """
    df_amazon = pd.read_csv(fileAmazon.file)
    df_amazon["platform"] = "amazon"
    df_disney = pd.read_csv(fileDisney.file)
    df_disney["platform"] = "disney"
    df_hulu = pd.read_csv(fileHulu.file)
    df_hulu["platform"] = "hulu"
    df_netflix = pd.read_csv(fileNetflix.file)
    df_netflix["platform"] = "netflix"
    df_titles = pd.concat([df_amazon,df_disney,df_hulu,df_netflix])
    parameters.DF_TITLES = df_titles
    return "Ficheros cargados correctamente..."

@app.post("/post_upload_rating",tags=["Paso 1: Carga de Ficheros"])
async def post_upload_rating(files: List[UploadFile]):
    # variable lista donde se almacenara los dataframes cargados
    df_list=[]
    # se recorre los ficheros cargados para almacenarnos en la lista
    for f in files:
        df_list.append(pd.read_csv(f.file))
    # se une los ficheros de la lista en un solo dataframe
    df_rating = pd.concat(df_list, axis=0, ignore_index=True)
    # guardamos el dataframe n una variable global
    parameters.DF_RATING = df_rating
    return 'files rating uploaded successfully'
# ETL
@app.get("/get_transformacion",tags=["Paso 2: Transformacion"]) 
async def get_transformacion():
    """
    Se realiza la transformacion de los datos con las consignas solicitadas
    """
    df_titles = parameters.DF_TITLES
     # CONSIGNA 1:
    # Se concatena la primera letra de la columna "platform" con la columna "show_id" y se asigna a una nueva columna "id"
    df_titles["id"] = df_titles["platform"].str.slice(0,1) + df_titles["show_id"]
    # CONSIGNA 2:
    # Se reemplaza los valores nulos de la columna "rating" con la letra"G"
    df_titles["rating"] = df_titles["rating"].replace(np.nan,'G')
    # CONSIGNA 3:
    # Se modifica el formato de fecha en AAAA-mm-dd
    df_titles["date_added"] = pd.to_datetime(df_titles["date_added"]).dt.strftime('%Y-%m-%d')
    # CONSIGNA 4:
    # Se modifica los campos de texto en minúsculas, sin excepciones
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
    # CONSIGNA 5:
    # El campo duration debe convertirse en dos campos: duration_int y duration_type. 
    # Se divide la columna "dutarion" en 2 columnas duration_int y duration_type
    df_titles[['duration_int', 'duration_type']] = df_titles['duration'].str.extract('(\d+)\s*(\w+)')
    # reemplazar seasons por season
    df_titles['duration_type'] = df_titles["duration_type"].str.replace("seasons", "season")
    # ADICIONAL -- reemplazar los nulos por ceros
    df_titles['duration_int'].fillna(value=0, inplace=True)
    # ADICIONAL -- convertir los valores a enteros
    df_titles['duration_int'] = df_titles['duration_int'].astype('int64')
    # Se asigna el dataframe a un parametro global para su uso posterior
    parameters.DF_TITLES = df_titles
    return "Proceso de Transformacion completado"

# DESARROLLO API
@app.get('/get_max_duration',tags=["Paso 3 : Desarrollo de API"])
async def get_max_duration(year:Union[int,None] = None, platform:Union[str,None]='',duration_type:Union[str,None]=''): 
    """
       CONSIGNA 1: \n
    Película con mayor duración con filtros opcionales de AÑO, PLATAFORMA Y TIPO DE DURACIÓN.(la función debe llamarse get_max_duration(year, platform, duration_type))
    """
    # capturamos el dataframe transformado desde la variable global
    titles=parameters.DF_TITLES
    # se valida si el usuario no ingresa un año en especifico por ser una variable opcional
    if year == None :
        df_filter = titles[(titles['platform'].str.contains(platform)) & (titles['duration_type'].str.contains(duration_type))]
    else:
        df_filter = titles[(titles['release_year']==year) & (titles['platform'].str.contains(platform)) & (titles['duration_type'].str.contains(duration_type))]
    # se optiene el valor maximo de la columna "duration_int" y lo utilizamos como un segundo filtro
    resultado = df_filter[df_filter['duration_int'] == df_filter['duration_int'].max()]
    # se retorna el registro completo de la pelicula en formato json
    return json.loads(resultado.to_json(orient='records'))
        
@app.get('/get_score_count/{platform}/{score}/{year}',tags=["Paso 3 : Desarrollo de API"])
async def get_score_count(platform:str, score:float, year:int):
    """
       CONSIGNA 2: \n
    Cantidad de películas por plataforma con un puntaje mayor a XX en determinado año (la función debe llamarse get_score_count(platform, scored, year))
    """
    df_rating = parameters.DF_RATING
    # se convierte la columna "timestamp" en formato Fecha %Y
    df_rating["timestamp"] = pd.to_datetime(df_rating["timestamp"]).dt.strftime('%Y')
    # se filtra el dataframe por año y plataforma
    df_filter =df_rating[(df_rating['timestamp'].str.contains(str(year))) & (df_rating['movieId'].str.contains(platform[0], case= False))]
    # se calcula el promedio de los rating por pelicula
    df_media_rating =df_filter.groupby(['movieId']).agg('mean') 
    # se filtra el dataframe con rating superiores al score dado en la variable
    df_result = df_media_rating[df_media_rating['rating']> score]
    # se retorna cantidad de registros que cumplen con los filtros
    return len(df_result)

@app.get('/get_count_platform/{platform}',tags=["Paso 3 : Desarrollo de API"])
def get_count_platform(platform:str):
    """
    CONSIGNA 3: \n
    Cantidad de películas por plataforma con filtro de PLATAFORMA.(La función debe llamarse get_count_platform(platform))
    """
    titles=parameters.DF_TITLES
    # se realiza el filtro por paltaforma
    df_filter = titles[titles['platform']==platform]
    # se obtiene la cantidad de registros depues de los filtros
    result = df_filter['id'].count()
    return int(result)

@app.get('/get_actor/{plataforma}/{year}',tags=["Paso 3 : Desarrollo de API"])
def get_actor(platform:str, year:int):
    """
    CONSIGNA 4: \n
    Actor que más se repite según plataforma y año.(La función debe llamarse get_actor(platform, year))
    """
    titles=parameters.DF_TITLES
    # se realiza un filtro con las variables obligatorias
    df_filter = titles[(titles['platform']==platform) & (titles['release_year']==year)]
    # se reemplaza los registros vacion de la columna "cast" con un string "vacio" para efectos de la consigna
    df_filter['cast'].fillna('vacio', inplace=True)
    # se elimina los registros con datos vacios en la columan "cast"
    df_filter_no_empty = df_filter[df_filter['cast']!='vacio']
    # se reemplaza el coma y espacio(", ") con solamente el coma(",") para no tener espacions en blanco
    df_filter_no_space = df_filter_no_empty['cast'].str.replace(', ',',')
    # se realiza la separacion los nombres de los actores y actrices utilizando for y guardandolos en un arreglo
    lista=[]
    for i in df_filter_no_space:
        s=i.split(',')
        for j in range(len(s)):
            if s[j] not in lista:
                lista.append(s[j])
            else:
                pass         
    lista= list(set(lista))
    # se realiza la elaboracion de un diccionario que almacena el nombre e los actores con la cantidad de veces que aparecen
    count = 0
    dict = {}
    for i in lista:
        for j in df_filter_no_space:
            if i in j.split(','):
                count +=1
        dict[i] = count
    # se obtiene el valor maximo de cantidad de apariciones
    actor = max(dict, key = dict.get)
    return actor + ' con ' + str(dict[actor]) + ' apariciones'

# EDA
@app.get('/get_eda',tags=["Paso 4 : EDA"])
async def get_eda():
    """
    Se realiza los cambios: \n
    - quitar duplicados en el dataframe rating porque hay usuarios que calificaron mas de una vez la misma pelicula con puntaje diferente.\n
    - se elimina las columnas ['rating','show_id','type','director','cast','country','date_added','release_year','duration','description','platform','duration_int','duration_type'] en titles.\n
    - se elimina la columna timestamp en rating.\n
    - se renombra la columna id por movieId en titles.\n
    - se realiza cambios en la columna listed_in de titles.\n
    """
    titles = parameters.DF_TITLES
    rating = parameters.DF_RATING
    #quitar duplicados
    rating = rating.drop_duplicates(['userId','movieId'],keep='last')

    # se realiza modificaciones y se elimina los datos que no son importantes
    titles = titles.drop(['rating','show_id','type','director','cast','country','date_added','release_year','duration','description','platform','duration_int','duration_type'],1)
    rating = rating.drop('timestamp',1)
    titles.rename(columns={'id': 'movieId'}, inplace=True)
    # se hace la el reemplazo de las comas con espacios y separamos por coma
    titles['listed_in'] = titles['listed_in'].str.replace(', ',',')
    titles['listed_in'] =titles['listed_in'].str.split(',')
    parameters.DF_TITLES = titles
    parameters.DF_RATING = rating
    return {"proceso completado"}

@app.get('/recomendacion/{userId}/{movieId}',tags=["Paso 5 : Sistema Recomendacion"])
async def get_recomendacion(userId:int, movieId:str):
    """
    Sistema de Recomendacion:\n
    designa si una pelicula es recomendable o no basado en las calificaciones del usuario frente a otra peliculas con generos similares
    """
    # se realiza la agregacion de los generos en columnas nuevas individualmente a una copia del dataframe titles
    titles = parameters.DF_TITLES
    rating = parameters.DF_RATING
    titles_copy = titles.copy()
    for index, row in titles.iterrows():
        for genre in row['listed_in']:
            titles_copy.at[index, genre] = 1
    # se asigna 0 a los campos nulos
    titles_copy = titles_copy.fillna(0)
    # se obtiene las peliculas calificadas por el usuario
    user_rating_title = rating[rating['userId']==userId]
    # se asigna los generos con 0 y 1 a las peliculas califidadas por el usuario 
    title_user = titles_copy[titles_copy['movieId'].isin(user_rating_title['movieId'].tolist())]
    # limpiar informacion innecesaria
    title_user = title_user.reset_index(drop = True)
    table_genres = title_user.drop(['movieId','title','listed_in'], 1)
    # se crea la matriz que refleja las preferencias del usuario en funcion al genero
    title_user = table_genres.transpose().dot(user_rating_title['rating'])
    # se extrae los generos de la tabla original
    genres = titles_copy.set_index(titles_copy['movieId'])
    # se borra informacion innecesaria
    genres = genres.drop(['movieId','title','listed_in'], 1)
    genres.shape
    # se calcula el promedio ponderado de las peliculas relacionadas con las peliculas de calificadas por el usuario
    recom = ((genres * title_user).sum(axis=1))/(title_user.sum())
    # se valida la pelicula dada por el usuario para verificar si tiene un promedio ponderado superior a cero
    result=''
    if(recom.filter(like=movieId).values[0] > 0):
        result = "recomendado"
    else:
        result = "no recomendado"
    return result

@app.get('/clear_parameters')
async def get_clear_parameters():
    parameters.DF_RATING = None
    parameters.DF_TITLES = None
    return "parameters clear"

