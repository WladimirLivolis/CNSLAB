import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;

public class Main {
	
	private static void metricDistributionPerRegion(Queue<Operation> oplist, List<Region> regions, String metric, String fileName, ArrayList<Double> jfiList) {
		
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
			jfiList.add(jfi);
			
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
		int update_sample_size = 524288; // 2^19 operations
		int search_sample_size = 524288; // 2^19 operations
		int window_size = 67108864; // 2^26 observations
		
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
								
		HeuristicV2 heuristic2 = new HeuristicV2(num_attr, num_mach, axis, metric);
		HeuristicV3 heuristic3 = new HeuristicV3(num_attr, num_mach, axis, metric, window_size, e);
		
		/* TRAINING HEURISTICS */
		
		// Reading operations from training sample
		System.out.println("["+LocalTime.now()+"] Reading training sample file...");
		Queue<Operation> oplist = Utilities.readOperationsFile("./samples/training_sample.json");
		System.out.println("["+LocalTime.now()+"] Done!");
		
		Queue<Operation> op_window = new LinkedList<Operation>();
		
		boolean flag = false;
		
		for (Operation op : oplist) {
			boolean window_moved = heuristic3.insertGK(op);
			if (window_moved) { flag = true; }
			if (flag) { op_window.poll(); }
			op_window.add(op);
		}
		
		System.out.println("No. of observations seen so far: "+heuristic3.numberOfObservationsSeenSoFar()+" observations");
		System.out.println("Size of operations window: "+op_window.size()+" operations");
		
		// Partitions region
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
		
		ArrayList<Double> jfi_list_h2 = new ArrayList<Double>();
		ArrayList<Double> jfi_list_h3 = new ArrayList<Double>();
		
		Queue<Operation> suboplist = new LinkedList<Operation>();
		boolean window_moved = false;
		double threshold = 0;
		while (!oplist.isEmpty()) {
			
			if(window_moved) {
				
				System.out.println("No. of new operations since last window movement: "+suboplist.size()+" operations");
				
				System.out.println("["+LocalTime.now()+"] Checking whether heuristic 2 quantiles are still valid...");
				Map<Double, Double> old_quantiles_h2 = heuristic2.getQuantiles();
				Map<Double, Double> new_quantiles_h2 = heuristic2.findQuantiles(op_window);
				
				boolean must_repartition = false;
				for (Map.Entry<Double, Double> entry : old_quantiles_h2.entrySet()) {
					double phi = entry.getKey();
					double old_quant = entry.getValue();
					double new_quant = new_quantiles_h2.get(phi);
					
					double diff = Math.abs(new_quant - old_quant)/old_quant;
					if ( diff >= threshold ) {
						must_repartition = true;
						System.out.println("Must repartition!");
						break;
					}
				}
				
				if (must_repartition) {
					System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 2...");
					regions2 = heuristic2.partition(op_window);
					old_quantiles_h2 = new_quantiles_h2;
				} else {
					System.out.println("No need to repartition :)");
				}
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Checking whether heuristic 3 quantiles are still valid...");
				Map<Double, Double> old_quantiles_h3 = heuristic3.getQuantiles();
				Map<Double, Double> new_quantiles_h3 = heuristic3.findQuantiles();
				
				must_repartition = false;
				for (Map.Entry<Double, Double> entry : old_quantiles_h3.entrySet()) {
					double phi = entry.getKey();
					double old_quant = entry.getValue();
					double new_quant = new_quantiles_h3.get(phi);
					
					double diff = Math.abs(new_quant - old_quant)/old_quant;
					if ( diff >= threshold ) {
						must_repartition = true;
						System.out.println("Must repartition!");
						break;
					}
				}
				
				if (must_repartition) {
					System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 3...");
					regions3 = heuristic3.partitionGK();
					old_quantiles_h3 = new_quantiles_h3;
				} else {
					System.out.println("No need to repartition :)");
				}
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Calculating metric distribution per region...");
				metricDistributionPerRegion(suboplist, regions2, metric, "./heuristic2/heuristic2_dist_"+LocalTime.now()+".txt", jfi_list_h2);
				metricDistributionPerRegion(suboplist, regions3, metric, "./heuristic3/heuristic3_dist_"+LocalTime.now()+".txt", jfi_list_h3);
				System.out.println("["+LocalTime.now()+"] Done!");

				suboplist = new LinkedList<Operation>();
				window_moved = false;
				
			} else {
				
				Operation op = oplist.poll();
				
				window_moved = heuristic3.insertGK(op);
				
				suboplist.add(op);
				
				op_window.poll();
				op_window.add(op);
								
			}
			
		}
		
		// Writes log file with jfi data for both heuristics 2 & 3
		PrintWriter pw2 = null, pw3 = null;
		
		try {
			
			pw2 = new PrintWriter("heuristic2_jfi_"+LocalTime.now()+".txt");
			pw3 = new PrintWriter("heuristic3_jfi_"+LocalTime.now()+".txt");
			
			for (double jfi : jfi_list_h2) {
				pw2.println((jfi_list_h2.indexOf(jfi)+1)+"\t"+jfi);
			}
			for (double jfi : jfi_list_h3) {
				pw3.println((jfi_list_h3.indexOf(jfi)+1)+"\t"+jfi);
			}
			
			
		} catch (FileNotFoundException err) {
			err.printStackTrace();
		} finally {
			pw2.close();
			pw3.close();
		}

	}

}