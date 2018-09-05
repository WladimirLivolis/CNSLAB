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
	
	private static void testHeuristicsAgainstNewOperations(String sampleFile, Queue<Operation> op_window, List<Region> regions2, List<Region> regions3, HeuristicV2 heuristic2, HeuristicV3 heuristic3, ArrayList<Double> jfi_list_h2, ArrayList<Double> jfi_list_h3, String metric, int num_machines, String axis, Map<Integer, Integer> messagesPerMachineReplicateAll, Map<Integer, Integer> messagesPerMachineQueryAll, Map<Integer, Integer> messagesPerMachineHyperspace, Map<Integer, Integer> messagesPerMachineHyperspaceV0) {
		
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
				metricDistributionPerRegion(suboplist, regions2, metric, "./heuristic2/heuristic2_dist_"+LocalDateTime.now()+".txt", jfi_list_h2);
				metricDistributionPerRegion(suboplist, regions3, metric, "./heuristic3/heuristic3_dist_"+LocalDateTime.now()+".txt", jfi_list_h3);
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Calculating no. of exchange messages between controller and each machine...");
				Map<Integer, Integer> replicateAll = Utilities.messagesCounterReplicateAll(num_machines, suboplist);
				Map<Integer, Integer> queryAll = Utilities.messagesCounterQueryAll(num_machines, axis, suboplist);
				Map<Integer, Integer> hyperspace = Utilities.messagesCounterHyperspace(num_machines, suboplist, regions3);
				System.out.println("["+LocalTime.now()+"] Done!");
				
				for (int machine = 1; machine <= num_machines; machine++) {
					messagesPerMachineHyperspaceV0.put(machine, messagesPerMachineHyperspaceV0.get(machine)+hyperspace.get(machine));
				}
				
				System.out.println("["+LocalTime.now()+"] Repartitioning using Heuristic 2...");
				Utilities.copyRegionsRanges(regions2, heuristic2.partition(op_window));
				System.out.println("["+LocalTime.now()+"] Done!");
							
				System.out.println("["+LocalTime.now()+"] Repartitioning using Heuristic 3...");
				Utilities.copyRegionsRanges(regions3, heuristic3.partitionGK());
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Calculating no. of exchange messages because of a repartition...");
				Utilities.checkGUIDsAfterRepartition(null, regions2, 0);
				Utilities.checkGUIDsAfterRepartition(hyperspace, regions3, 1);
				System.out.println("["+LocalTime.now()+"] Done!");

				for (int machine = 1; machine <= num_machines; machine++) {
					messagesPerMachineReplicateAll.put(machine, messagesPerMachineReplicateAll.get(machine)+replicateAll.get(machine));
					messagesPerMachineQueryAll.put(machine, messagesPerMachineQueryAll.get(machine)+queryAll.get(machine));
					messagesPerMachineHyperspace.put(machine, messagesPerMachineHyperspace.get(machine)+hyperspace.get(machine));
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
	
	public static void main(String[] args) {
		
		int num_attr = 3;
		int num_mach = 64;
		int num_max_guids = 8192; // 2^13		
		int update_sample_size = 16384; // 2^14 operations
		int search_sample_size = 16384; // 2^14 operations
		int window_size = 33554432; // 2^25 observations (touches)
		
		String axis = "A1";
		String dist = "normal";
		String metric = "touches";
		
		double deviation = 0.1;
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
		
		int numOfTestingSamples = 4;

		Map<Integer, Map<String, Double>> guids = new TreeMap<Integer, Map<String, Double>>();
		
		List<String> sampleFileNames = new ArrayList<String>(numOfTestingSamples+1);
		sampleFileNames.add("./samples/training_sample_"+LocalDateTime.now()+".json");
		for (int i = 1; i <= numOfTestingSamples; i++) {
			sampleFileNames.add("./samples/testing_sample_"+i+"_"+LocalDateTime.now()+".json");
		}
		
		/* GENERATING SAMPLES */
		
		System.out.println("["+LocalTime.now()+"] Generating training sample...");
		Utilities.generateOperations(update_sample_size, search_sample_size, num_attr, num_max_guids, guids, dist, distParams, sampleFileNames.get(0), new Random());
		System.out.println("["+LocalTime.now()+"] Done!");

		// 4 random uniforms
//		for (int i = 1; i <= numOfTestingSamples; i++) {
//			Random rnd = new Random();
//			double start = rnd.nextDouble(), end = rnd.nextDouble();
//			System.out.println("["+LocalTime.now()+"] Generating testing sample "+i+": Uniform ("+start+", "+end+")...");
//			distParams.put("low", start);
//			distParams.put("high", end);
//			Utilities.generateOperations(update_sample_size, search_sample_size, num_attr, num_max_guids, guids, dist, distParams, sampleFileNames.get(i), rnd);
//			System.out.println("["+LocalTime.now()+"] Done!");
//		}
		
		// 4 normal distributions
		for (int i = 1; i <= numOfTestingSamples; i++) {
			double new_mean = i*0.2;
			System.out.println("["+LocalTime.now()+"] Generating testing sample "+i+": Normal ("+new_mean+", "+deviation+")...");
			distParams.put("mean", new_mean);
			distParams.put("deviation", deviation);
			Utilities.generateOperations(update_sample_size, search_sample_size, num_attr, num_max_guids, guids, dist, distParams, sampleFileNames.get(i), new Random());
			System.out.println("["+LocalTime.now()+"] Done!");
		}
								
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
		System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 2...");
		List<Region> regions2 = new ArrayList<Region>();
		regions2.addAll(heuristic2.partition(oplist));
		System.out.println("["+LocalTime.now()+"] Done!");
		System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 3...");
		List<Region> regions3 = new ArrayList<Region>();
		regions3.addAll(heuristic3.partitionGK());
		System.out.println("["+LocalTime.now()+"] Done!");
		
		ArrayList<Double> jfi_list_h2 = new ArrayList<Double>();
		ArrayList<Double> jfi_list_h3 = new ArrayList<Double>();
		
		System.out.println("["+LocalTime.now()+"] Calculating metric distribution per region...");
		metricDistributionPerRegion(oplist, regions2, metric, "./heuristic2/heuristic2_dist_"+LocalTime.now()+".txt", jfi_list_h2);
		metricDistributionPerRegion(oplist, regions3, metric, "./heuristic3/heuristic3_dist_"+LocalTime.now()+".txt", jfi_list_h3);
		System.out.println("["+LocalTime.now()+"] Done!");
				
		/* TESTING HEURISTICS */
		
		Map<Integer, Integer> messagesPerMachineReplicateAll = new TreeMap<Integer, Integer>();		
		Map<Integer, Integer> messagesPerMachineQueryAll = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> messagesPerMachineHyperspace = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> messagesPerMachineHyperspaceV0 = new TreeMap<Integer, Integer>();
		for (int machine = 1; machine <= num_mach; machine++) {
			messagesPerMachineReplicateAll.put(machine, 0);
			messagesPerMachineQueryAll.put(machine, 0);
			messagesPerMachineHyperspace.put(machine, 0);
			messagesPerMachineHyperspaceV0.put(machine, 0);
		}
		
		// tests heuristics
		testHeuristicsAgainstNewOperations(sampleFileNames.get(1), op_window, regions2, regions3, heuristic2, heuristic3, jfi_list_h2, jfi_list_h3, metric, num_mach, axis, messagesPerMachineReplicateAll, messagesPerMachineQueryAll, messagesPerMachineHyperspace, messagesPerMachineHyperspaceV0);
		testHeuristicsAgainstNewOperations(sampleFileNames.get(2), op_window, regions2, regions3, heuristic2, heuristic3, jfi_list_h2, jfi_list_h3, metric, num_mach, axis, messagesPerMachineReplicateAll, messagesPerMachineQueryAll, messagesPerMachineHyperspace, messagesPerMachineHyperspaceV0);
		testHeuristicsAgainstNewOperations(sampleFileNames.get(3), op_window, regions2, regions3, heuristic2, heuristic3, jfi_list_h2, jfi_list_h3, metric, num_mach, axis, messagesPerMachineReplicateAll, messagesPerMachineQueryAll, messagesPerMachineHyperspace, messagesPerMachineHyperspaceV0);
		testHeuristicsAgainstNewOperations(sampleFileNames.get(4), op_window, regions2, regions3, heuristic2, heuristic3, jfi_list_h2, jfi_list_h3, metric, num_mach, axis, messagesPerMachineReplicateAll, messagesPerMachineQueryAll, messagesPerMachineHyperspace, messagesPerMachineHyperspaceV0);
		
		// Writes log files to generate graphs with gnuplot
		PrintWriter pw_h2 = null, pw_h3 = null, pw_replicateAll = null, pw_queryAll = null, pw_hyperspace = null, pw_hyperspace_v0 = null;
		
		try {
			
			pw_h2 = new PrintWriter("./output/heuristic2_jfi_"+LocalDateTime.now()+".txt");
			pw_h3 = new PrintWriter("./output/heuristic3_jfi_"+LocalDateTime.now()+".txt");
			
			pw_h2.println("# repartitions\tJFI");
			for (double jfi : jfi_list_h2) {
				pw_h2.println((jfi_list_h2.indexOf(jfi)+1)+"\t"+jfi);
			}
			pw_h3.println("# repartitions\tJFI");
			for (double jfi : jfi_list_h3) {
				pw_h3.println((jfi_list_h3.indexOf(jfi)+1)+"\t"+jfi);
			}
			
			pw_replicateAll = new PrintWriter("./output/replicateAll_"+LocalDateTime.now()+".txt");
			pw_queryAll = new PrintWriter("./output/queryAll_"+LocalDateTime.now()+".txt");
			pw_hyperspace = new PrintWriter("./output/hyperspace_"+LocalDateTime.now()+".txt");
			pw_hyperspace_v0 = new PrintWriter("./output/hyperspace_v0_"+LocalDateTime.now()+".txt");
			
			pw_replicateAll.println("# machine\tmessages");
			pw_queryAll.println("# machine\tmessages");
			pw_hyperspace.println("# machine\tmessages");
			pw_hyperspace_v0.println("# machine\tmessages");
			
			for (int machine = 1; machine <= num_mach; machine++) {
				pw_replicateAll.println(machine+"\t"+messagesPerMachineReplicateAll.get(machine));
				pw_queryAll.println(machine+"\t"+messagesPerMachineQueryAll.get(machine));
				pw_hyperspace.println(machine+"\t"+messagesPerMachineHyperspace.get(machine));
				pw_hyperspace_v0.println(machine+"\t"+messagesPerMachineHyperspaceV0.get(machine));
			}
			
			
		} catch (FileNotFoundException err) {
			err.printStackTrace();
		} finally {
			pw_h2.close();
			pw_h3.close();
			pw_replicateAll.close();
			pw_queryAll.close();
			pw_hyperspace.close();
			pw_hyperspace_v0.close();
		}

	}

}