#########################################
# AUTOR: Omar Suárez López
# ASIGNATURA: Big Data
# PRACTICA: Storm
# CURSO: 2017/2018
#########################################


Anotaciones sobre la práctica:
------------------------------

+ Dentro del código Java se comenta la arquitectura de las diferentes topologias utilizadas.






Pasos para ejecutar el código:
------------------------------


1) Introducimos en el código la lista de países que queremos consultar separados por comas, por ejemplo:
	"United Kingdom, Germany"

NOTA: Ahora mismo el código está preparado para iterar 100 veces (1 por segundo) haciendo el query DRPC 
de manera que los resultados se mostrarán durante 100 segundos.

1) Generar el JAR del proyecto utilizando Gradle:
	gradle clean build

2) Ejecutar el cluster de Storm:
	java_storm_cluster.sh

3) Comprobar que tenemos 4 nodos funcionando:
	hdfs dfsadmin -report

4) Nos conectamos al node1 para configurar Kafka mediante ssh
	ssh node1

5) Creamos el topic de Kafka
	kafka-topics.sh --create --topic compras-trident --partitions 1 --replication 1 --zookeeper namenode:2181

6) Comprobamos que el topic se ha creado:
	kafka-topics.sh --list --zookeeper namenode:2181


7) Arrancamos ahora el simulador de streaming (InvoiceDataProducer):
	+ Entramos en el directorio donde está situado
	./production_data.sh


8) Nos vamos al directorio donde está el proyecto y el JAR y lanzamos la topología de Trident:
	storm jar trident-kafka.jar trident.TridentKafkaDRPC


9) La topología debería comenzar a cargarse y en unos minutos deberíamos ver la lista de
los 5 usuarios de los países consultados con mayor montante de compras y de cancelaciones.
		
