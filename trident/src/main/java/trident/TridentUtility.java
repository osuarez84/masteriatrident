package trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TridentUtility {

	
	// Clase para parsear las lineas del csv y emitir la tupla
	// con los datos de interes.
	public static class SplitInvoice extends BaseFunction {
		private static final long serialVersionUID = 2L;
		
		public void execute (TridentTuple tuple, TridentCollector collector) {
			String invoiceStream = tuple.getString(0);
			String[] parts = invoiceStream.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			
			// Guardamos los datos ya parseados
			String invoiceNo = parts[0];
			String stockCode = parts[1];
			String description = parts[2];
			int quantity = Integer.parseInt(parts[3]);
			String invoiceData = parts[4];
			double unitPrice = Double.parseDouble(parts[5]);
			String customerID = parts[6];
			String country = parts[7];
			
			// emitimos la tupla con los campos necesarios
			collector.emit(new Values(invoiceNo, stockCode, description, quantity,
					invoiceData, unitPrice, customerID, country));
			
		}
	}
	
	
	// Clase para ayudar a depurar, nos imprime los campos filtrados
	public static class Print extends BaseFilter {
		private static final long serialVersionUID = 1L;
		
		@Override
		public boolean isKeep(TridentTuple tuple) {
			System.out.println(tuple);
			return true;
		}


	}
	
	
	// Clase para filtrar los campos vacios
	public static class FilterEmptyCustomer extends BaseFilter {
		private static final long serialVersionUID = 6L;
		
		@Override
		public boolean isKeep(TridentTuple tuple) {
			String o = tuple.getString(0);
			if(o==null || o.isEmpty()){
				return false;
			}
			return true;

		}

	}
	
	// Clase para calcular el montante de compra o cancelacion
	public static class TotalAmount extends BaseFunction {

		private static final long serialVersionUID = 3L;
		
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			
			double total = Math.floor((tuple.getInteger(0) * tuple.getDouble(1)) * 100 ) / 100;
			
			collector.emit(new Values(total));
			
			
		}
		
	}
	
	
	// Clase para parsear y emitir la lista de paises para el query
	public static class Split extends BaseFunction {
		private static final long serialVersionUID = 4L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			// TODO Auto-generated method stub
			String countries = tuple.getString(0);
			for(String country : countries.split(",")) {
				collector.emit(new Values(country));
			}
		}
		
		
	}
	
	// Clase para filtrar las compras (positivo)
	public static class FilterSell extends BaseFilter {

		private static final long serialVersionUID = 5L;

		@Override
		public boolean isKeep(TridentTuple tuple) {
			if (tuple.getDouble(0) > 0) {
				return true;
			}
			return false;
		}
		
	}
	
	// Clase para filtrar las cancelaciones (negativo)
	public static class FilterCancel extends BaseFilter {

		/**
		 * 
		 */
		private static final long serialVersionUID = 7L;

		@Override
		public boolean isKeep(TridentTuple tuple) {
			if(tuple.getDouble(0) < 0) {
				return true;
			}
			return false;
		}
		
	}
	
	
	// Clase para filtrar los paises que el usuario quiere consultar
	public static class SelectedCountries extends BaseFilter {

		/**
		 * 
		 */
		private static final long serialVersionUID = 8L;

		@Override
		public boolean isKeep(TridentTuple tuple) {
			if (tuple.getString(1).contains(tuple.getString(0))) {
				return true;
			}
			return false;
		}
		
	}
	
}
