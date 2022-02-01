# NO TOCAR
from faker import Faker #Faker es una librería que genera datos de prueba
import keyboard
import time
import random
from datetime import datetime
import copy
from math import sin, cos, asin, sqrt, degrees, radians
from kafka import KafkaProducer
import json

Earth_radius_km = 6371.0
RADIUS = Earth_radius_km

'''
El objetivo es que los usuarios reciban una alerta si un AMIGO se acerca en su radio de interacción.
Es decir, el programa genera un usuario random, al que se le asignan de 1 a 10 (no incluido) amigos.
Estos amigos son con los que tenemos que comparar la posición
'''

faker = Faker('es_ES') #genera datos en español
users={} #defino el diccionario vacío que completaremos
#Constantes definidas:
USERS_TOTAL=100
lat_min=39.4
lat_max=39.5
lon_min=-0.3
lon_max=-0.4
vehicles=["Bike","Train","Car", "Walking"]

def haversine(angle_radians):
    return sin(angle_radians / 2.0) ** 2

def inverse_haversine(h):
    return 2 * asin(sqrt(h)) # radians

def distance_between_points(lat1, lon1, lat2, lon2):
    # all args are in degrees
    # WARNING: loss of absolute precision when points are near-antipodal
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    dlat = lat2 - lat1
    dlon = radians(lon2 - lon1)
    h = haversine(dlat) + cos(lat1) * cos(lat2) * haversine(dlon)
    return RADIUS * inverse_haversine(h)

def distance_calc(user):
    global users
    #dist_max = distance_between_points(lat_min, lon_min, lat_max, lon_max)  # me devuelve km, cogiendo el mín y máx, vemos que hasta 14km nos puede salir la alerta
    #como la distancia máxima con las cordinadas dadas minimas y máximas da un radio de 14km, creemos que es mejor avisar a las personas que estén realmente cerca de las mismas, sino estaríamos avisando a todos los usuarios, ya que solo se generarán usuarios dentro de este radio
    dist_max = 2 #distancia máxima de 2km
    for user in users:
        user_lat = users[user]['position']['lat']
        user_lon = users[user]['position']['lon']
        user_frnds = users[user]['friends']
        #queremos crear u diccionario de diccionarios que nos guarde las distancias en km entre los amigos si es menor o igual que la distancia máxima de 14km

        alert_frnds_user = {} #nos creamos otro diccionario para asignar los amigos de un usuario y la distancia a la que están
        alert_lat_frnds_user = {} #nos creamos otro diccionario para asignar los amigos de un usuario y la latitud a la que están
        alert_lon_frnds_user = {} #nos creamos otro diccionario para asignar los amigos de un usuario y la longitud a la que están

        for friend in user_frnds:
            #como en cada paso actualizamos la lat y lon de cada user, podemos calcular la nueva distancia entre un user y sus friends (los friends a su vez son también users)
            print(friend)
            frnd_lat = users[friend]['position']['lat']
            frnd_lon = users[friend]['position']['lon']
            dist_frnd = round(distance_between_points(user_lat, user_lon, frnd_lat, frnd_lon),2)
            if dist_frnd <= dist_max: #comprobamos que la distancia es < 2km que es lo que nos hemos definido
                print('Distancia: ',dist_frnd)
                alert_frnds_user[friend] = dist_frnd   #añadimos el id del friend y la distancia entre el user y su friend
                alert_lat_frnds_user[friend] = frnd_lat #añadimos la lat del friend
                alert_lon_frnds_user[friend] = frnd_lon #añadimos la lon del friend
        #print(alert_frnds_user)
        users[user]['distance_frnds'] = alert_frnds_user #añadimos el contenido obtenido anteriormente
        users[user]['lat_frnds'] = alert_lat_frnds_user #añadimos el contenido obtenido anteriormente
        users[user]['lon_frnds'] = alert_lon_frnds_user #añadimos el contenido obtenido anteriormente

def initiate_data():
    global users #utiliza global porque es una variable global de todo el programa no es local de la función
    for i in range(0,USERS_TOTAL):
        user={}
        user["id"]=faker.ssn()
        user["name"]=faker.first_name()
        user["last_name"]=faker.last_name()
        user["friends"]=[]
        user["position"]={"lat":random.uniform(39.4, 39.5),"lon":random.uniform(-0.3, -0.4)}
        user["transport"]=random.choice(vehicles)
        user["age"]=round(random.uniform(16, 85),2)
        user["gender"]=random.choice(["man","woman"])
        user["weight"]=random.uniform(60, 110)
        user["height"]=random.uniform(150, 210)
        user["bodyfat"]=random.uniform(3, 45)
        user["bloodpressure_sist"]=random.uniform(90, 180)
        user["bloodpressure_diast"]=random.uniform(70, 120)
        user["cholesterol"]=random.uniform(150, 300)
        user["smoker"]=random.choice(["0","1"])
        user["drinking"]=random.uniform(0,7)
        user["disability"]=random.choice(["0","1"])
        user["previouspatology"]=random.choice(["0","1"])
        user["cp"]=random.randint(46001, 46025)
        user["time"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        users[user["id"]]=user   
    num=0
    for element in users.items(): #genera una lista aleatoria de amigos de 0 a 10 (no incluido)
        print(f"Generating friends of {num} of {len(users)}")
        for i in range(0,random.randint(1,10)):
            friend=random.choice(list(users.values()))
            if friend["id"]!=element[0]:
                users[element[0]]["friends"].append(friend["id"]) #a cada user le añade en el campo friends una lista de amigos
            else:
                print("No friend of yourself") 
        num=num+1

    print("DATA GENERATED")        


def generate_step(): #generación aleatoria de la posición de los usuarios
    global users
    global old
    old = copy.deepcopy(users)
    if len(users)>0:
        print("STEP")
        for element in users.items():
            users[element[0]]["time"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            lat=users[element[0]]["position"]["lat"]
            lon=users[element[0]]["position"]["lon"]
            users[element[0]]["position"]["lon"]=lon+random.uniform(0.001, 0.005)
            users[element[0]]["position"]["lat"]=lat+random.uniform(0.001, 0.005)
            if lat>lat_max or lat<lat_min:
                users[element[0]]["position"]["lat"]=random.uniform(39.4, 39.5)
                users[element[0]]["transport"]=random.choice(vehicles)
            if lon>lon_max or lon<lon_min:
                users[element[0]]["position"]["lon"]=random.uniform(-0.3, -0.4)
    else:
        print('Inicializamos datos')
        initiate_data()
        print('Fin de la inicialización de los datos')

def serializer(users): #serializamos lo que nos devuelve para utilizar kafka
    return json.dumps(users).encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers = 'localhost:9092',
    value_serializer = serializer
) #añadimos esta línea para obtener el productor de kafka para la ingesta de datos

# NO TOCAR

#Este código es el productor de kafka:
oldval={}
while True:
    try:
        if keyboard.is_pressed('q'):  # if key 'q' is pressed 
            print('You Exited the data generator')
            break  
        else:
            generate_step()
            #añadimos nuestro código
            #print('Los usuarios: ', users)
            distance_calc(users)
            #una vez hemos calculad la distancia, el mensaje debe ser personalizado para cada usuario:
            #cont = 1
            for user in users:
                '''cuando recorremos el diccionario users solo me mete en user el id del user, pero no mete el diccionario
                entero del user. Para recuperar toda la info, necesitamos poner: users[user][campo que necesitemos recuperar]'''
                print(users[user])
                #creamos un topic para cada usuario
                #topic = "user"+str(cont)
                topic = "topic_users"
                producer.send(topic, users[user])
                #cont += 1

            #print(old)

            time.sleep(60)

    except Exception as err:
        print(f"Unexpected {err}, {type(err)}")
        break  

