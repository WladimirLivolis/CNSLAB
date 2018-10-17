import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
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
			
			pw.println("# region\tmetric");
			
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
	
	private static void testHeuristicsAgainstNewOperations(String sampleFile, Queue<Operation> op_window, List<Region> regions1, List<Region> regions2, List<Region> regions3, List<Region> regions4, List<Region> regions5, HeuristicV1 heuristic1, HeuristicV2 heuristic2, HeuristicV3 heuristic3, ArrayList<Double> jfi_list_h1, ArrayList<Double> jfi_list_h2, ArrayList<Double> jfi_list_h3, ArrayList<Double> jfi_list_h4, ArrayList<Double> jfi_list_h5, String metric, int num_machines, String axis, Map<Integer, Integer> messagesPerMachineReplicateAll, Map<Integer, Integer> messagesPerMachineQueryAll, Map<Integer, Integer> messagesPerMachineHyperspace0, Map<Integer, Integer> messagesPerMachineHyperspace1, Map<Integer, Integer> messagesPerMachineHyperspace2) {
		
		// Reading operations from testing sample
		System.out.println("["+LocalTime.now()+"] Reading testing sample file...");
		Queue<Operation> oplist = Utilities.readOperationsFile(sampleFile);
		System.out.println("["+LocalTime.now()+"] Done!");
		
		Queue<Operation> suboplist = new LinkedList<Operation>();
		boolean window_moved = false;
		int numberOfObservationsSeenSoFar = heuristic3.numberOfObservationsSeenSoFar();
		
		while (!oplist.isEmpty()) {
			
			if(window_moved) {
				
				System.out.println("No. of new operations since last window movement: "+suboplist.size()+" operations");
				System.out.println("No. of observations since last window movement: "+(heuristic3.numberOfObservationsSeenSoFar()-numberOfObservationsSeenSoFar)+" observations");
				numberOfObservationsSeenSoFar = heuristic3.numberOfObservationsSeenSoFar();
				
				System.out.println("["+LocalTime.now()+"] Calculating metric distribution per region...");
				metricDistributionPerRegion(suboplist, regions1, metric, "./heuristic1/heuristic1_dist_"+LocalDateTime.now()+".txt", jfi_list_h1);
				metricDistributionPerRegion(suboplist, regions2, metric, "./heuristic2/heuristic2_dist_"+LocalDateTime.now()+".txt", jfi_list_h2);
				metricDistributionPerRegion(suboplist, regions3, metric, "./heuristic3/heuristic3_dist_"+LocalDateTime.now()+".txt", jfi_list_h3);
				metricDistributionPerRegion(suboplist, regions4, metric, "./heuristic4/heuristic4_dist_"+LocalDateTime.now()+".txt", jfi_list_h4);
				metricDistributionPerRegion(suboplist, regions5, metric, "./heuristic5/heuristic5_dist_"+LocalDateTime.now()+".txt", jfi_list_h5);
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Calculating no. of exchange messages between controller and each machine...");
				Map<Integer, Integer> replicateAll = Utilities.messagesCounterReplicateAll(num_machines, suboplist);
				Map<Integer, Integer> queryAll = Utilities.messagesCounterQueryAll(num_machines, axis, suboplist);
				Map<Integer, Integer> hyperspace0 = Utilities.messagesCounterHyperspace(num_machines, suboplist, regions3);
				Map<Integer, Integer> hyperspace1 = Utilities.messagesCounterHyperspace(num_machines, suboplist, regions3);
				Map<Integer, Integer> hyperspace2 = Utilities.messagesCounterHyperspace(num_machines, suboplist, regions3);
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Repartitioning using Heuristic 1...");
				Utilities.copyRegionsRanges(regions1, heuristic1.partition(op_window));
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Repartitioning using Heuristic 2...");
				Utilities.copyRegionsRanges(regions2, heuristic2.partition(op_window));
				System.out.println("["+LocalTime.now()+"] Done!");
							
				System.out.println("["+LocalTime.now()+"] Repartitioning using Heuristic 3...");
				Utilities.copyRegionsRanges(regions3, heuristic3.partitionGK());
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Calculating no. of exchange messages because of a repartition...");
				Utilities.checkGUIDsAfterRepartition(null, regions2, 0);
				Utilities.checkGUIDsAfterRepartition(hyperspace0, Utilities.copyRegionsWithGUIDs(regions3), 0);
				Utilities.checkGUIDsAfterRepartition(hyperspace1, Utilities.copyRegionsWithGUIDs(regions3), 1);
				Utilities.checkGUIDsAfterRepartition(hyperspace2, regions3, 2);
				System.out.println("["+LocalTime.now()+"] Done!");

				for (int machine = 1; machine <= num_machines; machine++) {
					messagesPerMachineReplicateAll.put(machine, messagesPerMachineReplicateAll.get(machine)+replicateAll.get(machine));
					messagesPerMachineQueryAll.put(machine, messagesPerMachineQueryAll.get(machine)+queryAll.get(machine));
					messagesPerMachineHyperspace0.put(machine, messagesPerMachineHyperspace0.get(machine)+hyperspace0.get(machine));
					messagesPerMachineHyperspace1.put(machine, messagesPerMachineHyperspace1.get(machine)+hyperspace1.get(machine));
					messagesPerMachineHyperspace2.put(machine, messagesPerMachineHyperspace2.get(machine)+hyperspace2.get(machine));
				}
				
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
		
	}
	
	public static String printRegions(List<Region> regions) {
		StringBuilder str = new StringBuilder("{ ");
		for (Region region : regions) {
			str.append(region.getName());
			str.append(" = [ ");
			for (Map.Entry<String, Range> pair : region.getPairs().entrySet()) {
				str.append("(");
				str.append(pair.getKey());
				str.append(",[");
				str.append(pair.getValue().getLow());
				str.append(",");
				str.append(pair.getValue().getHigh());
				str.append("]) ");	
			}
			str.append("] ");
		}
		str.append("}");
		return str.toString();
	}
	
	public static void main(String[] args) {
		
		int num_attr = 3;
		int num_mach = 64;
		int num_max_guids = 8192; // 2^13		
		int update_sample_size = 16384; // 2^14 operations
		int search_sample_size = 16384; // 2^14 operations
		int window_size = 33554432; // 2^25 observations (touches)
		
		String axis = "A1";
		String dist = "uniform";
		String metric = "touches";
		
		double deviation = 0.1;
		double mean = 0.5;
		double lambda = 1;
		double low = 0;
		double high = 1;
		
		Map<String, Map<Integer, Map<String, Double>>> distParams = new HashMap<String, Map<Integer, Map<String, Double>>>();
		Map<Integer, Map<String, Double>> operationDistParams = new TreeMap<Integer, Map<String, Double>>();
		Map<String, Double> attrDistParams = new HashMap<String, Double>();
		
		Map<String, Map<Integer, String>> distribution = new HashMap<String, Map<Integer, String>>();
		Map<Integer, String> operationDist = new TreeMap<Integer, String>();
		
		switch(dist.toLowerCase()) {	
			case "normal":
			case "gaussian":
				attrDistParams.put("deviation", deviation);
				attrDistParams.put("mean", mean);
				break;
			case "exponential":
				attrDistParams.put("lambda", lambda);
				break;
			case "uniform":
			default:
				attrDistParams.put("low", low);
				attrDistParams.put("high", high);
				break;
		}
		
		for (int i = 1; i <= num_attr; i++) { 
			operationDistParams.put(i, attrDistParams);
			operationDist.put(i, dist);
		}
		distParams.put("update", operationDistParams);
		distParams.put("search", operationDistParams);
		distribution.put("update", operationDist);
		distribution.put("search", operationDist);
		
		double e = 1/64d;
		
		int numOfTestingSamples = 4;

		Map<Integer, Map<String, Double>> guids = new TreeMap<Integer, Map<String, Double>>();
		
		List<String> sampleFileNames = new ArrayList<String>(numOfTestingSamples+1);
		sampleFileNames.add("./samples/training_sample_"+LocalDateTime.now()+".json");
		for (int i = 1; i <= numOfTestingSamples; i++) {
			sampleFileNames.add("./samples/testing_sample_"+i+"_"+LocalDateTime.now()+".json");
		}
		
		/* GENERATING SAMPLES */
		
		System.out.println("["+LocalTime.now()+"] Generating training sample...");
		Utilities.generateOperations(update_sample_size, search_sample_size, num_attr, num_max_guids, guids, distribution, distParams, sampleFileNames.get(0), new Random());
		System.out.println("["+LocalTime.now()+"] Done!");

		ArrayList<String> possibleDistributions = new ArrayList<String>();
		possibleDistributions.add("uniform");
		possibleDistributions.add("normal");
		possibleDistributions.add("exponential");
		
		// 4 random distributions
		for (int i = 1; i <= numOfTestingSamples; i++) {
			System.out.println("["+LocalTime.now()+"] Generating testing sample "+i);
			
			Random rnd = new Random();
			
			distribution = new HashMap<String, Map<Integer, String>>();
			distParams = new HashMap<String, Map<Integer, Map<String, Double>>>();
			Utilities.generateRandomDistribution(num_attr, possibleDistributions, distribution, distParams, rnd);
			
			for (Map.Entry<String, Map<Integer, String>> op : distribution.entrySet()) {
				System.out.println("Operation: "+op.getKey());
				for (Map.Entry<Integer, String> attr : op.getValue().entrySet()) {
					System.out.println("Attr: "+attr.getKey());
					System.out.println("Dist: "+attr.getValue());
					for (Map.Entry<String, Double> param : distParams.get(op.getKey()).get(attr.getKey()).entrySet()) {
						System.out.println(param.getKey()+": "+param.getValue());
					}
				}
			}
			
			Utilities.generateOperations(update_sample_size, search_sample_size, num_attr, num_max_guids, guids, distribution, distParams, sampleFileNames.get(i), rnd);
			
			System.out.println("["+LocalTime.now()+"] Done!");
		}
		
		HeuristicV1 heuristic1 = new HeuristicV1(num_attr, num_mach);
		HeuristicV2 heuristic2 = new HeuristicV2(num_attr, num_mach, axis, metric);
		HeuristicV3 heuristic3 = new HeuristicV3(num_attr, num_mach, axis, metric, window_size, e);
		
		/* TRAINING HEURISTICS */
		
		// Reading operations from training sample
		System.out.println("["+LocalTime.now()+"] Reading training sample file...");
		Queue<Operation> oplist = Utilities.readOperationsFile(sampleFileNames.get(0));
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
		System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 1 (CNS)...");
		List<Region> regions1 = new ArrayList<Region>();
		regions1.addAll(heuristic1.partition(oplist));
		System.out.println("["+LocalTime.now()+"] Done!");
		System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 2 (Quantiles)...");
		List<Region> regions2 = new ArrayList<Region>();
		regions2.addAll(heuristic2.partition(oplist));
		System.out.println("["+LocalTime.now()+"] Done!");
		System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 3 (Quantiles+GK)...");
		List<Region> regions3 = new ArrayList<Region>();
		regions3.addAll(heuristic3.partitionGK());
		System.out.println("["+LocalTime.now()+"] Done!");
		System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 4 (Replicate All)...");
		List<Region> regions4 = Utilities.buildNewRegions(num_attr);
		System.out.println(printRegions(regions4));
		System.out.println("["+LocalTime.now()+"] Done!");
		System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 5 (Query All)...");
		List<Region> regions5 = new ArrayList<Region>();
		for (int r = 1; r <= num_mach; r++) {
			Map<String, Range> pairs = new HashMap<String, Range>();
			for (int i = 1; i <= num_attr; i++) {
				if (("A"+i).equals(axis)) {
					pairs.put("A"+i, new Range((r-1)/(double)num_mach, r/(double)num_mach));
				} else { 
					pairs.put("A"+i, new Range(0.0, 1.0));
				}
			}
			Region region = new Region("R"+r, pairs);
			regions5.add(region);
		}
		System.out.println(printRegions(regions5));
		System.out.println("["+LocalTime.now()+"] Done!");

		
		ArrayList<Double> jfi_list_h1 = new ArrayList<Double>();
		ArrayList<Double> jfi_list_h2 = new ArrayList<Double>();
		ArrayList<Double> jfi_list_h3 = new ArrayList<Double>();
		ArrayList<Double> jfi_list_h4 = new ArrayList<Double>();
		ArrayList<Double> jfi_list_h5 = new ArrayList<Double>();
		
		System.out.println("["+LocalTime.now()+"] Calculating metric distribution per region...");
		metricDistributionPerRegion(oplist, regions1, metric, "./heuristic1/heuristic1_dist_"+LocalDateTime.now()+".txt", jfi_list_h1);
		metricDistributionPerRegion(oplist, regions2, metric, "./heuristic2/heuristic2_dist_"+LocalDateTime.now()+".txt", jfi_list_h2);
		metricDistributionPerRegion(oplist, regions3, metric, "./heuristic3/heuristic3_dist_"+LocalDateTime.now()+".txt", jfi_list_h3);
		metricDistributionPerRegion(oplist, regions4, metric, "./heuristic4/heuristic4_dist_"+LocalDateTime.now()+".txt", jfi_list_h4);
		metricDistributionPerRegion(oplist, regions5, metric, "./heuristic5/heuristic5_dist_"+LocalDateTime.now()+".txt", jfi_list_h5);
		System.out.println("["+LocalTime.now()+"] Done!");
				
		/* TESTING HEURISTICS */
		
		Map<Integer, Integer> messagesPerMachineReplicateAll = new TreeMap<Integer, Integer>();		
		Map<Integer, Integer> messagesPerMachineQueryAll = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> messagesPerMachineHyperspace0 = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> messagesPerMachineHyperspace1 = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> messagesPerMachineHyperspace2 = new TreeMap<Integer, Integer>();
		for (int machine = 1; machine <= num_mach; machine++) {
			messagesPerMachineReplicateAll.put(machine, 0);
			messagesPerMachineQueryAll.put(machine, 0);
			messagesPerMachineHyperspace0.put(machine, 0);
			messagesPerMachineHyperspace1.put(machine, 0);
			messagesPerMachineHyperspace2.put(machine, 0);
		}
		
		// tests heuristics
		testHeuristicsAgainstNewOperations(sampleFileNames.get(1), op_window, regions1, regions2, regions3, regions4, regions5, heuristic1, heuristic2, heuristic3, jfi_list_h1, jfi_list_h2, jfi_list_h3, jfi_list_h4, jfi_list_h5, metric, num_mach, axis, messagesPerMachineReplicateAll, messagesPerMachineQueryAll, messagesPerMachineHyperspace0, messagesPerMachineHyperspace1, messagesPerMachineHyperspace2);
		testHeuristicsAgainstNewOperations(sampleFileNames.get(2), op_window, regions1, regions2, regions3, regions4, regions5, heuristic1, heuristic2, heuristic3, jfi_list_h1, jfi_list_h2, jfi_list_h3, jfi_list_h4, jfi_list_h5, metric, num_mach, axis, messagesPerMachineReplicateAll, messagesPerMachineQueryAll, messagesPerMachineHyperspace0, messagesPerMachineHyperspace1, messagesPerMachineHyperspace2);
		testHeuristicsAgainstNewOperations(sampleFileNames.get(3), op_window, regions1, regions2, regions3, regions4, regions5, heuristic1, heuristic2, heuristic3, jfi_list_h1, jfi_list_h2, jfi_list_h3, jfi_list_h4, jfi_list_h5, metric, num_mach, axis, messagesPerMachineReplicateAll, messagesPerMachineQueryAll, messagesPerMachineHyperspace0, messagesPerMachineHyperspace1, messagesPerMachineHyperspace2);
		testHeuristicsAgainstNewOperations(sampleFileNames.get(4), op_window, regions1, regions2, regions3, regions4, regions5, heuristic1, heuristic2, heuristic3, jfi_list_h1, jfi_list_h2, jfi_list_h3, jfi_list_h4, jfi_list_h5, metric, num_mach, axis, messagesPerMachineReplicateAll, messagesPerMachineQueryAll, messagesPerMachineHyperspace0, messagesPerMachineHyperspace1, messagesPerMachineHyperspace2);
		
		// Writes log files to generate graphs with gnuplot
		PrintWriter pw_h1 = null, pw_h2 = null, pw_h3 = null, pw_h4 = null, pw_h5 = null, pw_replicateAll = null, pw_queryAll = null, pw_hyperspace0 = null, pw_hyperspace1 = null, pw_hyperspace2 = null;
		
		try {
			
			pw_h1 = new PrintWriter("./output/heuristic1_jfi_"+LocalDateTime.now()+".txt");
			pw_h2 = new PrintWriter("./output/heuristic2_jfi_"+LocalDateTime.now()+".txt");
			pw_h3 = new PrintWriter("./output/heuristic3_jfi_"+LocalDateTime.now()+".txt");
			pw_h4 = new PrintWriter("./output/heuristic4_jfi_"+LocalDateTime.now()+".txt");
			pw_h5 = new PrintWriter("./output/heuristic5_jfi_"+LocalDateTime.now()+".txt");
			
			pw_h1.println("# repartitions\tJFI");
			pw_h2.println("# repartitions\tJFI");
			pw_h3.println("# repartitions\tJFI");
			pw_h4.println("# repartitions\tJFI");
			pw_h5.println("# repartitions\tJFI");
			
			for (int i = 0; i < jfi_list_h1.size(); i++) {
				pw_h1.println((i+1)+"\t"+jfi_list_h1.get(i));
				pw_h2.println((i+1)+"\t"+jfi_list_h2.get(i));
				pw_h3.println((i+1)+"\t"+jfi_list_h3.get(i));
				pw_h4.println((i+1)+"\t"+jfi_list_h4.get(i));
				pw_h5.println((i+1)+"\t"+jfi_list_h5.get(i));
			}
			
			pw_replicateAll = new PrintWriter("./output/replicateAll_"+LocalDateTime.now()+".txt");
			pw_queryAll = new PrintWriter("./output/queryAll_"+LocalDateTime.now()+".txt");
			pw_hyperspace0 = new PrintWriter("./output/hyperspace0_"+LocalDateTime.now()+".txt");
			pw_hyperspace1 = new PrintWriter("./output/hyperspace1_"+LocalDateTime.now()+".txt");
			pw_hyperspace2 = new PrintWriter("./output/hyperspace2_"+LocalDateTime.now()+".txt");
			
			pw_replicateAll.println("# machine\tmessages");
			pw_queryAll.println("# machine\tmessages");
			pw_hyperspace0.println("# machine\tmessages");
			pw_hyperspace1.println("# machine\tmessages");
			pw_hyperspace2.println("# machine\tmessages");
			
			for (int machine = 1; machine <= num_mach; machine++) {
				pw_replicateAll.println(machine+"\t"+messagesPerMachineReplicateAll.get(machine));
				pw_queryAll.println(machine+"\t"+messagesPerMachineQueryAll.get(machine));
				pw_hyperspace0.println(machine+"\t"+messagesPerMachineHyperspace0.get(machine));
				pw_hyperspace1.println(machine+"\t"+messagesPerMachineHyperspace1.get(machine));
				pw_hyperspace2.println(machine+"\t"+messagesPerMachineHyperspace2.get(machine));
			}
			
			
		} catch (FileNotFoundException err) {
			err.printStackTrace();
		} finally {
			pw_h1.close();
			pw_h2.close();
			pw_h3.close();
			pw_h4.close();
			pw_h5.close();
			pw_replicateAll.close();
			pw_queryAll.close();
			pw_hyperspace0.close();
			pw_hyperspace1.close();
			pw_hyperspace2.close();
		}

	}

}