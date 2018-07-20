package trident;

import java.io.IOException;

import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Debug;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.FirstN;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.operation.builtin.TupleCollectionGet;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.UUID;

public class TridentKafkaDRPC {

	public static StormTopology buildTopology (LocalDRPC drpc) throws IOException {
		
		// Creamos el spout para leer de kafka
		ZkHosts hosts = new ZkHosts("namenode:2181");
		TridentKafkaConfig spoutConfig = new TridentKafkaConfig(hosts, "compras-trident");
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConfig.forceFromStart = true;
		
		// Generamos el spout con la configiracion anterior
		TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(spoutConfig);		
		
		
		// Instanciamos la topologia
		TridentTopology topology = new TridentTopology();
		
		
		// 1) Creamos el pipeline para las tuplas del streaming de datos
		// de las compras

		// NOTA
		// cuando trabajamos con varias topologias que utilizan un mismo spout 
		// ,debemos de poner nombres diferentes en los
		// spouts, en este caso: spout1 y spout2. 
		// Si no se hace esto pueden surgir errores de runtime!!
		
		
		// Descripcion de la topologia:
		// 1 - Leemos del spout de Kafka
		// 2 - emitimos los campos de cada compra
		// 3 - calculamos el montante total de cada compra y lo emitimos
		// 4 - nos quedamos con los campos de interes
		// 5 - eliminamos los campos vacios
		// 6 - Nos quedamos solamente con las compras (positivas)
		// 7 - Agrupamos por paises y usuario
		// 8 - vamos acumulando en memoria el total de compras por pais y usuario
		
		// ###################################
		// topologia para procesar las compras
		// ###################################
		TridentState totalSell = topology
				.newStream("spout1", kafkaSpout)
				.each(new Fields("str"), new TridentUtility.SplitInvoice(), new Fields("invoiceNo", "stockCode", "description", "quantity", "invoiceDate", "unitPrice", "customerID", "country"))
				.each(new Fields("quantity", "unitPrice"), new TridentUtility.TotalAmount(), new Fields("totalAmount"))
				.project(new Fields("customerID", "country", "totalAmount"))
				.each(new Fields("customerID"), new TridentUtility.FilterEmptyCustomer())
				.each(new Fields("totalAmount"), new TridentUtility.FilterSell())
				.groupBy(new Fields("country", "customerID"))
				.persistentAggregate(new MemoryMapState.Factory(), new Fields("totalAmount"), new Sum(), new Fields("sumAmountSell"));
		
		// #########################################
		// topologia para procesar las cancelaciones
		// #########################################
		
		// Descripcion de la topologia:
		// 1 - Leemos del spout de Kafka
		// 2 - emitimos los campos de cada compra
		// 3 - calculamos el montante total de cada compra y lo emitimos
		// 4 - nos quedamos con los campos de interes
		// 5 - eliminamos los campos vacios
		// 6 - Nos quedamos solamente con las cancelaciones (negativas)
		// 7 - Agrupamos por paises y usuario
		// 8 - vamos acumulando en memoria el total de cancelaciones por pais y usuario
		
		TridentState totalCancel = topology
				.newStream("spout2", kafkaSpout)
				.each(new Fields("str"), new TridentUtility.SplitInvoice(), new Fields("invoiceNo", "stockCode", "description", "quantity", "invoiceDate", "unitPrice", "customerID", "country"))
				.each(new Fields("quantity", "unitPrice"), new TridentUtility.TotalAmount(), new Fields("totalAmount"))
				.project(new Fields("customerID", "country", "totalAmount"))
				.each(new Fields("customerID"), new TridentUtility.FilterEmptyCustomer())
				.each(new Fields("totalAmount"), new TridentUtility.FilterCancel())
				.groupBy(new Fields("country", "customerID"))
				.persistentAggregate(new MemoryMapState.Factory(), new Fields("totalAmount"), new Sum(), new Fields("sumAmountCancel"));
		
		
		// 2) Creamos el acceso RPC para las queries del usuario

		// #####################################
		// RPC para hacer el query sobre compras
		// #####################################
		
		// Descripcion de la topologia del RPC:
		// 1 - obtenemos los paises de la lista de consulta
		// 2 - obtenemos los posibles valores de pais y usuario disponibles
		// 3 - obtenemos el montante total de compras de los paises y usuarios 
		// 4 - filtramos y nos quedamos solamente con los que el usuario necesita
		// 5 - agrupamos por pais
		// 6 - obtenemos el top 5 de usuarios por pais
		topology
			.newDRPCStream("status_sell", drpc)
			.each(new Fields("args"), new TridentUtility.Split(), new Fields("country_query"))
			.stateQuery(totalSell, new TupleCollectionGet(), new Fields("country", "customerID"))
			.stateQuery(totalSell, new Fields("country", "customerID"), new MapGet(), new Fields("sumAmountSell"))
			.each(new Fields("country", "country_query"), new TridentUtility.SelectedCountries())
			.groupBy(new Fields("country"))
			.aggregate(new Fields("customerID","sumAmountSell"), new FirstN.FirstNSortedAgg(5, "sumAmountSell", true), new Fields("customerID","sumAmountSell"));
			
			
		// ###########################################
		// RPC para hacer el query sobre cancelaciones
		// ###########################################
		// Descripcion de la topologia del RPC:
		// 1 - obtenemos los paises de la lista de consulta
		// 2 - obtenemos los posibles valores de pais y usuario disponibles
		// 3 - obtenemos el montante total de cancelaciones de los paises y usuarios 
		// 4 - filtramos y nos quedamos solamente con los que el usuario necesita
		// 5 - agrupamos por pais
		// 6 - obtenemos el top 5 de usuarios por pais
		topology
			.newDRPCStream("status_cancel", drpc)
			.each(new Fields("args"), new TridentUtility.Split(), new Fields("country_query"))
			.stateQuery(totalCancel, new TupleCollectionGet(), new Fields("country", "customerID"))
			.stateQuery(totalCancel, new Fields("country", "customerID"), new MapGet(), new Fields("sumAmountCancel"))
			.each(new Fields("country", "country_query"), new TridentUtility.SelectedCountries())
			.groupBy(new Fields("country"))
			.aggregate(new Fields("customerID","sumAmountCancel"), new FirstN.FirstNSortedAgg(5, "sumAmountCancel", false), new Fields("customerID","sumAmountCancel"));
		
		
		// Devolvemos la topologia
		return topology.build();
		
	}
	
	
	
	public static void main (String[] args) throws Exception {
		// Configuramos el lanzamiento de la topologia
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		LocalCluster cluster = new LocalCluster();
		
		// Configuramos el DRPC
		LocalDRPC drpc = new LocalDRPC();
		
		// Cargamos la topologia
		cluster.submitTopology("trident-kafka-topology", conf, buildTopology(drpc));
		
		// Preparamos la lista de paises
		String country_list = new String("United Kingdom");
		
		// Presentamos los datos durante 100 segundos
		for (int i = 0; i < 100; i++) {
			System.out.println("Top 5 clientes compras: " + drpc.execute("status_sell", country_list));
			System.out.println("Top 5 clientes cancelaciones: " + drpc.execute("status_cancel", country_list));
			Thread.sleep(1000);
		}
		
		// Cerramos el cluster y el drpc
		drpc.shutdown();
		cluster.shutdown();
	}
	
	
	
}
