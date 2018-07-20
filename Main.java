import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;

public class Main {
	
	private static void metricDistributionPerRegion(Queue<Operation> oplist, List<Region> regions, String metric, String fileName) {
		
		double jfi = Utilities.JFI(metric, oplist, regions);
		
		PrintWriter pw = null;
		
		try {
			
			pw = new PrintWriter(fileName);
			
			pw.println("region\tmetric");
			
			for (Region r : regions) {
				
				int index = regions.indexOf(r)+1;
				
				double metricValue = -1;
				
				if (metric.toLowerCase().equals("touches")) {
					metricValue = r.getUpdateTouches()+r.getSearchTouches();
				} else if (metric.toLowerCase().equals("load")) {
					metricValue = r.getUpdateLoad()+r.getSearchLoad();
				}
				
				pw.println(index+"\t"+metricValue);
				
			}
			
			pw.println();
			pw.print("JFI: "+jfi);
			
		} catch (FileNotFoundException err) { 
			err.printStackTrace(); 
		} finally {
			pw.close();
		}
		
	}
	
	public static void main(String[] args) {
		
		int num_attr = 3;
		int num_mach = 64;
		int num_max_guids = 500;		
		int update_sample_size = 524288; // 2^19
		int search_sample_size = 524288; // 2^19
		int window_size = 67108864; // 2^27
		
		String axis = "A1";
		String dist = "uniform";
		String metric = "touches";
		
		double deviation = 0.15;
		double mean = 0.5;
		double lambda = 1;
		double low = 0;
		double high = 1;
		
		Map<String, Double> distParams = new HashMap<String, Double>();
		switch(dist.toLowerCase()) {	
			case "normal":
			case "gaussian":
				distParams.put("deviation", deviation);
				distParams.put("mean", mean);
				break;
			case "exponential":
				distParams.put("lambda", lambda);
				break;
			case "uniform":
			default:
				distParams.put("low", low);
				distParams.put("high", high);
				break;
		}
		
		double e = 1/64d;
		int period = 65536; // 2^16

		Map<Integer, Map<String, Double>> guids = new TreeMap<Integer, Map<String, Double>>();
		
		// Generates update & search loads
		System.out.println("["+LocalTime.now()+"] Generating training sample...");
		Utilities.generateOperations(update_sample_size, search_sample_size, num_attr, num_max_guids, guids, dist, distParams, "./samples/training_sample.json", new Random());
		System.out.println("["+LocalTime.now()+"] Done!");
		System.out.println("["+LocalTime.now()+"] Generating testing sample...");
		distParams.put("low", 0.3);
		distParams.put("high", 0.7);
		Utilities.generateOperations(update_sample_size, search_sample_size, num_attr, num_max_guids, guids, dist, distParams, "./samples/testing_sample.json", new Random());
		System.out.println("["+LocalTime.now()+"] Done!");
								
//		HeuristicV1 heuristic1 = new HeuristicV1(num_attr, num_mach);
		HeuristicV2 heuristic2 = new HeuristicV2(num_attr, num_mach, axis, metric);
		HeuristicV3 heuristic3 = new HeuristicV3(num_attr, num_mach, axis, metric, window_size, e);
		
		/* TRAINING HEURISTICS */
		
		// Reading operations from training sample
		System.out.println("["+LocalTime.now()+"] Reading training sample file...");
		Queue<Operation> oplist = Utilities.readOperationsFile("./samples/training_sample.json");
		System.out.println("["+LocalTime.now()+"] Done!");
		
		for (Operation op : oplist) {
			heuristic3.insertGK(op);
		}
		
		// Partitions region
//		System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 1...");
//		List<Region> regions1 = heuristic1.partition(oplist);
//		System.out.println("["+LocalTime.now()+"] Done!");
		System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 2...");
		List<Region> regions2 = heuristic2.partition(oplist);
		System.out.println("["+LocalTime.now()+"] Done!");
		System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 3...");
		List<Region> regions3 = heuristic3.partitionGK();
		System.out.println("["+LocalTime.now()+"] Done!");
		
		/* TESTING HEURISTICS */
		
		// Reading operations from testing sample
		System.out.println("["+LocalTime.now()+"] Reading testing sample file...");
		oplist = Utilities.readOperationsFile("./samples/testing_sample.json");
		System.out.println("["+LocalTime.now()+"] Done!");
		
		Map<Double, Double> old_quantiles = new TreeMap<Double, Double>();
		for (Map.Entry<Double, Double> entry : heuristic3.getQuantiles().entrySet()) {
			old_quantiles.put(entry.getKey(), entry.getValue());
		}
		
		Queue<Operation> suboplist = new LinkedList<Operation>();
		int count = 0;
		while (!oplist.isEmpty()) {
			
			if(count != 0 && count % period == 0) {
				
				System.out.println("["+LocalTime.now()+"] Checking whether heuristic 3 quantiles are still valid...");
				
				heuristic3.updateQuantiles();
				
				Map<Double, Double> new_quantiles = new TreeMap<Double, Double>();
				for (Map.Entry<Double, Double> entry : heuristic3.getQuantiles().entrySet()) {
					new_quantiles.put(entry.getKey(), entry.getValue());
				}
				
				boolean must_repartition = false;
				for (Map.Entry<Double, Double> entry : old_quantiles.entrySet()) {
					double phi = entry.getKey();
					double old_quant = entry.getValue();
					double new_quant = new_quantiles.get(phi);
					
					double diff = Math.abs(new_quant - old_quant)/old_quant;
					if ( diff >= 0.1 ) {
						must_repartition = true;
						System.out.println("Must repartition!");
						break;
					}
				}
				
				if (must_repartition) {
					System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 3...");
					regions3 = heuristic3.partitionGK();
					old_quantiles = new_quantiles;
				} else {
					System.out.println("No need to repartition :)");
				}
				
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Calculating metric distribution per region...");
//				metricDistributionPerRegion(suboplist, regions1, metric, "./heuristic1/heuristic1_dist_"+LocalTime.now()+".txt");
				metricDistributionPerRegion(suboplist, regions2, metric, "./heuristic2/heuristic2_dist_"+LocalTime.now()+".txt");
				metricDistributionPerRegion(suboplist, regions3, metric, "./heuristic3/heuristic3_dist_"+LocalTime.now()+".txt");
				System.out.println("["+LocalTime.now()+"] Done!");

				count = 0;
				suboplist = new LinkedList<Operation>();
				
			} else {
				
				Operation op = oplist.poll();
				heuristic3.insertGK(op);
				suboplist.add(op);
				
				count++;
				
			}
			
		}

	}

}