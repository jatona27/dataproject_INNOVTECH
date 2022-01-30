from kafka import KafkaConsumer
import json
import mysql.connector

if __name__ == '__main__':
    #Consumidor de Kafka
    consumer = KafkaConsumer(
        'topic_users',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest'
    )
    #Conectamos a la BBDD mysql:
    #Para conectarlo a local:
    #connection = mysql.connector.connect(host='127.0.0.1',
                                        # port=3306,
                                       #  database='db_users',
                                        # user='root',
                                        # password='r00tpass')

    #Para conectarlo a la db remota:
    connection = mysql.connector.connect(host='db4free.net',
                                         port=3306,
                                         database='db_users_mda',
                                         user='user_mda',
                                         password='r00tpass')

    sql_get_user = """SELECT id FROM users WHERE id = %s"""
    sql_get_user_friend = """SELECT id,friend FROM friends_distance WHERE id = %s AND friend = %s"""
    sql_insert_user = """INSERT INTO users (id,name,last_name,transport,age,gender,cp) Values (%s,%s,%s,%s,%s,%s,%s)"""
    sql_insert_user_friends = """INSERT INTO user_friends (id,friend) Values (%s,%s)"""
    sql_insert_friends_distance = """INSERT INTO friends_distance (id,friend,time,distance,lat,lon) Values (%s,%s,%s,%s,%s,%s)"""
    sql_update_friends_distance = """UPDATE friends_distance SET time = %s, distance = %s, lat = %s,lon = %s WHERE user = %s AND friend = %s"""

    for topic_users in consumer:
        data = json.loads(topic_users.value) #recupero todos los datos

        id = data["id"] #recupero el id
        cursor = connection.cursor(buffered=True)  #abrimos la conexión
        data_tupla = (id,)
        cursor.execute(sql_get_user, data_tupla)  #cerramos la conexión una vez recuperados los datos
        records = cursor.fetchall()

        if cursor.rowcount == 0: #si ejecuto la select de búsqueda del usuario y no recupero registros, tendré que registrar ese usuario
            cursor.close()
            name = data["name"]
            last_name = data["last_name"]
            transport = data["transport"]
            age = str(data["age"])
            gender = data["gender"]
            cp = str(data["cp"])
            insert_data_user = (id, name, last_name, transport, age, gender, cp)
            cursor = connection.cursor(buffered=True) #abrimos la conexión para registrar los datos
            cursor.execute(sql_insert_user, insert_data_user) #insertamos el usuario
            connection.commit() #hacemos el commit para que se guarden los datos
            cursor.close()

            #friends = data["friends"]
            #for friend in friends: #hago un for porque hay varios amigos
            #    cursor = connection.cursor(buffered=True)  # abrimos la conexión para registrar los datos
            #    insert_data_friends = (id, friend)
            #    cursor.execute(sql_insert_user_friends, insert_data_friends)  # insertamos el amigo
            #    connection.commit()  # hacemos el commit para que se guarden los datos
            #    cursor.close()

        else: #si está dado de alta el usuario, rellenamos sus datos
            time = data["time"]
            distance_frnds = data["distance_frnds"]
            print(data)
            lat_frnds = data["lat_frnds"]
            lon_frnds = data["lon_frnds"]
            for friend in distance_frnds:
                distance = distance_frnds[friend]
                print(lat_frnds)
                lat = lat_frnds[friend]
                lon = lon_frnds[friend]

                cursor = connection.cursor(buffered=True)  # abrimos la conexión
                data_tupla = (id,friend)
                cursor.execute(sql_get_user_friend, data_tupla)  # cerramos la conexión una vez recuperados los datos
                records = cursor.fetchall()

                if cursor.rowcount == 0:  # si ejecuto la select de búsqueda del usuario y no recupero registros, tendré que registrar ese usuario
                    cursor.close()
                    cursor = connection.cursor(buffered=True)  #abrimos la conexión para registrar los datos
                    insert_data_distance = (id, friend, time, distance,lat,lon)
                    cursor.execute(sql_insert_friends_distance, insert_data_distance)  #insertamos el amigo
                    connection.commit()  #hacemos el commit para que se guarden los datos
                    cursor.close()
                else:
                    cursor.close()
                    cursor = connection.cursor(buffered=True)  # abrimos la conexión para registrar los datos
                    update_data_distance = (time, distance, lat, lon, id, friend)
                    cursor.execute(sql_update_friends_distance, update_data_distance)  #ejecutamos el update en caso de que exista la tupla id y friend del usuario
                    connection.commit()  # hacemos el commit para que se guarden los datos
                    cursor.close()

        cursor.close()








