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
	
	private static Map<Integer, Double> calculateConfidenceInterval(Map<Integer, List<Integer>> values, Map<Integer, Double> mean) {

		Map<Integer, Double> confidenceInterval = new TreeMap<Integer, Double>();

		for (Map.Entry<Integer, List<Integer>> e : values.entrySet()) {

			double squaredDifferenceSum = 0.0, variance = 0.0, standardDeviation = 0.0;

			for (int num : e.getValue()) {

				squaredDifferenceSum += (num - mean.get(e.getKey())) * (num - mean.get(e.getKey()));

			}

			variance = squaredDifferenceSum / e.getValue().size();
			standardDeviation = Math.sqrt(variance);

			// value for 95% confidence interval
			double confidenceLevel = 1.96;
			double delta = confidenceLevel * standardDeviation / Math.sqrt(e.getValue().size());

			confidenceInterval.put(e.getKey(), delta);

		}

		return confidenceInterval;

	}
	
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
	
	private static void testHeuristicsAgainstNewOperations(String sampleFile, Queue<Operation> op_window, List<Region> regions2, List<Region> regions3, HeuristicV2 heuristic2, HeuristicV3 heuristic3, ArrayList<Double> jfi_list_h2, ArrayList<Double> jfi_list_h3, String metric, int num_machines, String axis, Map<Integer, List<Integer>> messagesPerMachineReplicateAll, Map<Integer, List<Integer>> messagesPerMachineQueryAll, Map<Integer, List<Integer>> messagesPerMachineHyperspace) {
		
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
				metricDistributionPerRegion(suboplist, regions2, metric, "./heuristic2/heuristic2_dist_"+LocalTime.now()+".txt", jfi_list_h2);
				metricDistributionPerRegion(suboplist, regions3, metric, "./heuristic3/heuristic3_dist_"+LocalTime.now()+".txt", jfi_list_h3);
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Calculating no. of exchange messages between controller and each machine...");
				Map<Integer, Integer> replicateAll = Utilities.messagesCounterReplicateAll(num_machines, suboplist);
				Map<Integer, Integer> queryAll = Utilities.messagesCounterQueryAll(num_machines, axis, suboplist);
				Map<Integer, Integer> hyperspace = Utilities.messagesCounterHyperspace(num_machines, suboplist, regions3);
				for (int machine = 1; machine <= num_machines; machine++) {
					messagesPerMachineReplicateAll.get(machine).add(replicateAll.get(machine));
					messagesPerMachineQueryAll.get(machine).add(queryAll.get(machine));
					messagesPerMachineHyperspace.get(machine).add(hyperspace.get(machine));
				}
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Repartitioning using Heuristic 2...");
				regions2.clear(); // clear heuristic2 regions
				regions2.addAll(heuristic2.partition(op_window)); // new partitions are added to heuristic2 regions
				System.out.println("["+LocalTime.now()+"] Done!");
							
				System.out.println("["+LocalTime.now()+"] Repartitioning using Heuristic 3...");
				regions3.clear(); // clear heuristic3 regions
				regions3.addAll(heuristic3.partitionGK()); // new partitions are added to heuristic3 regions
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
		System.out.println("["+LocalTime.now()+"] Generating testing sample 1: Uniform (0.0,0.4)...");
		distParams.put("low", 0.0);
		distParams.put("high", 0.4);
		Utilities.generateOperations(update_sample_size, search_sample_size, num_attr, num_max_guids, guids, dist, distParams, "./samples/testing_sample1.json", new Random());
		System.out.println("["+LocalTime.now()+"] Done!");
		System.out.println("["+LocalTime.now()+"] Generating testing sample 2: Uniform (0.2,0.6)...");
		distParams.put("low", 0.2);
		distParams.put("high", 0.6);
		Utilities.generateOperations(update_sample_size, search_sample_size, num_attr, num_max_guids, guids, dist, distParams, "./samples/testing_sample2.json", new Random());
		System.out.println("["+LocalTime.now()+"] Done!");
		System.out.println("["+LocalTime.now()+"] Generating testing sample 3: Uniform (0.4,0.8)...");
		distParams.put("low", 0.4);
		distParams.put("high", 0.8);
		Utilities.generateOperations(update_sample_size, search_sample_size, num_attr, num_max_guids, guids, dist, distParams, "./samples/testing_sample3.json", new Random());
		System.out.println("["+LocalTime.now()+"] Done!");
		System.out.println("["+LocalTime.now()+"] Generating testing sample 4: Uniform (0.6,1.0)...");
		distParams.put("low", 0.6);
		distParams.put("high", 1.0);
		Utilities.generateOperations(update_sample_size, search_sample_size, num_attr, num_max_guids, guids, dist, distParams, "./samples/testing_sample4.json", new Random());
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
		List<Region> regions2 = new ArrayList<Region>();
		regions2.addAll(heuristic2.partition(oplist));
		System.out.println("["+LocalTime.now()+"] Done!");
		System.out.println("["+LocalTime.now()+"] Partitioning using Heuristic 3...");
		List<Region> regions3 = new ArrayList<Region>();
		regions3.addAll(heuristic3.partitionGK());
		System.out.println("["+LocalTime.now()+"] Done!");
				
		/* TESTING HEURISTICS */
		
		ArrayList<Double> jfi_list_h2 = new ArrayList<Double>();
		ArrayList<Double> jfi_list_h3 = new ArrayList<Double>();
		
		Map<Integer, List<Integer>> messagesPerMachineReplicateAll = new TreeMap<Integer, List<Integer>>();		
		Map<Integer, List<Integer>> messagesPerMachineQueryAll = new TreeMap<Integer, List<Integer>>();
		Map<Integer, List<Integer>> messagesPerMachineHyperspace = new TreeMap<Integer, List<Integer>>();
		for (int machine = 1; machine <= num_mach; machine++) {
			messagesPerMachineReplicateAll.put(machine, new ArrayList<Integer>());
			messagesPerMachineQueryAll.put(machine, new ArrayList<Integer>());
			messagesPerMachineHyperspace.put(machine, new ArrayList<Integer>());
		}
		
		// tests heuristics
		testHeuristicsAgainstNewOperations("./samples/testing_sample1.json", op_window, regions2, regions3, heuristic2, heuristic3, jfi_list_h2, jfi_list_h3, metric, num_mach, axis, messagesPerMachineReplicateAll, messagesPerMachineQueryAll, messagesPerMachineHyperspace);
		testHeuristicsAgainstNewOperations("./samples/testing_sample2.json", op_window, regions2, regions3, heuristic2, heuristic3, jfi_list_h2, jfi_list_h3, metric, num_mach, axis, messagesPerMachineReplicateAll, messagesPerMachineQueryAll, messagesPerMachineHyperspace);
		testHeuristicsAgainstNewOperations("./samples/testing_sample3.json", op_window, regions2, regions3, heuristic2, heuristic3, jfi_list_h2, jfi_list_h3, metric, num_mach, axis, messagesPerMachineReplicateAll, messagesPerMachineQueryAll, messagesPerMachineHyperspace);
		testHeuristicsAgainstNewOperations("./samples/testing_sample4.json", op_window, regions2, regions3, heuristic2, heuristic3, jfi_list_h2, jfi_list_h3, metric, num_mach, axis, messagesPerMachineReplicateAll, messagesPerMachineQueryAll, messagesPerMachineHyperspace);
		
		// Calculates the average number of exchange messages per machine
		Map<Integer, Double> meanPerMachineReplicateAll = new TreeMap<Integer, Double>();
		Map<Integer, Double> meanPerMachineQueryAll = new TreeMap<Integer, Double>();
		Map<Integer, Double> meanPerMachineHyperspace = new TreeMap<Integer, Double>();
		for (int machine = 1; machine <= num_mach; machine++) {
			
			double sum = 0;
			List<Integer> messagesList = messagesPerMachineReplicateAll.get(machine);
			for (int i : messagesList) { sum += i; }
			meanPerMachineReplicateAll.put(machine, sum/(double)messagesList.size());
			
			sum = 0;
			messagesList = messagesPerMachineQueryAll.get(machine);
			for (int i : messagesList) { sum += i; }
			meanPerMachineQueryAll.put(machine, sum/(double)messagesList.size());
			
			sum = 0;
			messagesList = messagesPerMachineHyperspace.get(machine);
			for (int i : messagesList) { sum += i; }
			meanPerMachineHyperspace.put(machine, sum/(double)messagesList.size());
			
		}
		
		// Calculates confidence interval for exchange messages per machine
		Map<Integer, Double> confidenceIntervalReplicateAll = calculateConfidenceInterval(messagesPerMachineReplicateAll, meanPerMachineReplicateAll);
		Map<Integer, Double> confidenceIntervalQueryAll = calculateConfidenceInterval(messagesPerMachineQueryAll, meanPerMachineQueryAll);
		Map<Integer, Double> confidenceIntervalHyperspace = calculateConfidenceInterval(messagesPerMachineHyperspace, meanPerMachineHyperspace);
		
		// Writes log files to generate graphs with gnuplot
		PrintWriter pw_h2 = null, pw_h3 = null, pw_replicateAll = null, pw_queryAll = null, pw_hyperspace = null;
		
		try {
			
			pw_h2 = new PrintWriter("heuristic2_jfi_"+LocalTime.now()+".txt");
			pw_h3 = new PrintWriter("heuristic3_jfi_"+LocalTime.now()+".txt");
			
			pw_h2.println("# region\tJFI");
			for (double jfi : jfi_list_h2) {
				pw_h2.println((jfi_list_h2.indexOf(jfi)+1)+"\t"+jfi);
			}
			pw_h3.println("# region\tJFI");
			for (double jfi : jfi_list_h3) {
				pw_h3.println((jfi_list_h3.indexOf(jfi)+1)+"\t"+jfi);
			}
			
			pw_replicateAll = new PrintWriter("replicateAll_"+LocalTime.now()+".txt");
			pw_queryAll = new PrintWriter("queryAll_"+LocalTime.now()+".txt");
			pw_hyperspace = new PrintWriter("hyperspace_"+LocalTime.now()+".txt");
			
			pw_replicateAll.println("# machine\tmean\tCI");
			pw_queryAll.println("# machine\tmean\tCI");
			pw_hyperspace.println("# machine\tmean\tCI");
			
			for (int machine = 1; machine <= num_mach; machine++) {
				pw_replicateAll.println(machine+"\t"+meanPerMachineReplicateAll.get(machine)+"\t"+confidenceIntervalReplicateAll.get(machine));
				pw_queryAll.println(machine+"\t"+meanPerMachineQueryAll.get(machine)+"\t"+confidenceIntervalQueryAll.get(machine));
				pw_hyperspace.println(machine+"\t"+meanPerMachineHyperspace.get(machine)+"\t"+confidenceIntervalHyperspace.get(machine));
			}
			
			
		} catch (FileNotFoundException err) {
			err.printStackTrace();
		} finally {
			pw_h2.close();
			pw_h3.close();
			pw_replicateAll.close();
			pw_queryAll.close();
			pw_hyperspace.close();
		}

	}

}