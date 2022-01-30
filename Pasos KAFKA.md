#Pasos KAFKA: 

Start the ZooKeeper and Kafka container.

```sh
$ docker-compose up -d
```

### Create Topic

Solo hay que crear un topic, por tnto creamos 

```sh
$docker-compose up -d
$ docker-compose exec kafka kafka-topics --create --topic topic_users --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server host.docker.internal:9092
```



Luego arrancamos en dos ventanas de terminal sepradas: python datagenerator.py y python consumer_kafka_python.py



Create a topic named `stream-in`, one partition and one replica.

```sh
$ docker-compose exec kafka kafka-topics --create --topic stream-in --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server host.docker.internal:9092
```

Create a topic named `stream-out`, one partition and one replica.

```sh
$ docker-compose exec kafka kafka-topics --create --topic stream-out --partitions 1 --replication-factor 1 --if-not-exists --bootstrap-server host.docker.internal:9092
```

Para comprobar los topics:
```sh
$ docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

Para borrar un topic:
```sh
$ docker-compose exec kafka kafka-topics --delete  --topic myTopic --bootstrap-server localhost:9092
```

Para empezar a producir debemos meternos en el productor:
```sh
$ docker-compose exec kafka kafka-console-producer --topic stream-in --broker-list localhost:9092
```
Abrimos otra pestaña del terminal y ejecutamos el consumidor:
```sh
$ docker-compose exec kafka kafka-console-consumer --topic stream-out --from-beginning --bootstrap-server localhost:9092
```


Instalación de la base de datos mysql: 

password: r00tpass





