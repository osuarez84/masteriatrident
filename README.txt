#########################################
# AUTOR: Omar Su�rez L�pez
# ASIGNATURA: Big Data
# PRACTICA: Storm
# CURSO: 2017/2018
#########################################


Anotaciones sobre la pr�ctica:
------------------------------

+ Dentro del c�digo Java se comenta la arquitectura de las diferentes topologias utilizadas.






Pasos para ejecutar el c�digo:
------------------------------


1) Introducimos en el c�digo la lista de pa�ses que queremos consultar separados por comas, por ejemplo:
	"United Kingdom, Germany"

NOTA: Ahora mismo el c�digo est� preparado para iterar 100 veces (1 por segundo) haciendo el query DRPC 
de manera que los resultados se mostrar�n durante 100 segundos.

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
	+ Entramos en el directorio donde est� situado
	./production_data.sh


8) Nos vamos al directorio donde est� el proyecto y el JAR y lanzamos la topolog�a de Trident:
	storm jar trident-kafka.jar trident.TridentKafkaDRPC


9) La topolog�a deber�a comenzar a cargarse y en unos minutos deber�amos ver la lista de
los 5 usuarios de los pa�ses consultados con mayor montante de compras y de cancelaciones.
		
