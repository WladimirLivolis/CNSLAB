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
	
	private static void metricDistributionPerRegion(Queue<Operation> oplist, List<Region> regions, String fileName, Map<String, List<Double>> jfiList) {
		
		Map<String, Double> jfi = Utilities.JFI_touches_and_guids(oplist, regions);
		
		PrintWriter pw = null;
		
		try {
			
			pw = new PrintWriter(fileName);
			
			pw.println("# region\ttouches\tGUIDs");
			
			for (Region r : regions) {
				
				int index = regions.indexOf(r)+1;
					
				double touches = r.getUpdateTouches()+r.getSearchTouches();
				double guids = r.getGUIDs().size();
				
				pw.println(index+"\t"+touches+"\t"+guids);
				
			}
			
			double jfi_touches = jfi.get("touches");
			double jfi_guids = jfi.get("guids");
						
			pw.println();
			pw.println("JFI (touches): "+jfi_touches);
			pw.print("JFI (guids): "+jfi_guids);
			jfiList.get("touches").add(jfi_touches);
			jfiList.get("guids").add(jfi_guids);
			
		} catch (FileNotFoundException err) { 
			err.printStackTrace(); 
		} finally {
			pw.close();
		}
		
	}
	
	private static void metricDistributionPerMachine(Queue<Operation> oplist, List<Machine> machines, String fileName, Map<String, List<Double>> jfiList) {
		
		Map<String, Double> jfi = Utilities.JFI(oplist, machines);
		
		PrintWriter pw = null;
		
		try {
			
			pw = new PrintWriter(fileName);
			
			pw.println("# machine\ttouches\tGUIDs");
			
			for (Machine m : machines) {
				
				int index = machines.indexOf(m)+1;
								
				double touches = m.getUpdateTouches()+m.getSearchTouches();
				double guids = m.getGUIDs().size();
				
				pw.println(index+"\t"+touches+"\t"+guids);
				
			}
			
			double jfi_touches = jfi.get("touches");
			double jfi_guids = jfi.get("guids");
			
			pw.println();
			pw.println("JFI (touches): "+jfi_touches);
			pw.print("JFI (guids): "+jfi_guids);
			jfiList.get("touches").add(jfi_touches);
			jfiList.get("guids").add(jfi_guids);
			
		} catch (FileNotFoundException err) { 
			err.printStackTrace(); 
		} finally {
			pw.close();
		}
		
	}
	
	private static void testHeuristicsAgainstNewOperations(String RHO, int experiment_number, List<String> sampleFileNames, List<Region> regions1, List<Region> regions2, List<Region> regions3, List<Region> regions4, List<Machine> machines5, List<Region> regions6, HeuristicV1 heuristic1, HeuristicV2 heuristic2, HeuristicV3 heuristic3, Map<String, List<Double>> jfi_list_h1, Map<String, List<Double>> jfi_list_h2, Map<String, List<Double>> jfi_list_h3, Map<String, List<Double>> jfi_list_h4, Map<String, List<Double>> jfi_list_h5, Map<String, List<Double>> jfi_list_h6, int num_machines, String axis, Map<Integer, Integer> messagesPerMachineReplicateAll, Map<Integer, Integer> messagesPerMachineQueryAll, Map<Integer, Integer> messagesPerMachineHyperspace0, Map<Integer, Integer> messagesPerMachineHyperspace1, Map<Integer, Integer> messagesPerMachineHyperspace2, Map<Integer, Integer> messagesPerMachineHyperdex, Map<Integer, Integer> messagesPerMachineCNS) {
		
		// Reading operations from sample files
		Queue<Operation> oplist = new LinkedList<Operation>();
		System.out.println("["+LocalTime.now()+"] Reading testing sample files...");
		for (String sampleFile : sampleFileNames) {
			oplist.addAll(Utilities.readOperationsFile(sampleFile));
		}
		System.out.println("["+LocalTime.now()+"] Done!");
		
		Queue<Operation> op_window = new LinkedList<Operation>(), suboplist = new LinkedList<Operation>();
		boolean window_moved = false, windowIsFull = false;
		int numberOfObservationsSeenSoFar = heuristic3.numberOfObservationsSeenSoFar();
		int period = 16384; // # of operations
		
		while (!oplist.isEmpty()) {
			
			if (suboplist.size() == period) {
				
				System.out.println("Size of operations window: "+op_window.size()+" operations");
				System.out.println("No. of new operations since last period: "+suboplist.size()+" operations");
				System.out.println("No. of observations since last period: "+(heuristic3.numberOfObservationsSeenSoFar()-numberOfObservationsSeenSoFar)+" observations");
				numberOfObservationsSeenSoFar = heuristic3.numberOfObservationsSeenSoFar();
				
				System.out.println("["+LocalTime.now()+"] Calculating metric distribution per region...");
				metricDistributionPerRegion(suboplist, regions1, "./heuristic1/heuristic1_dist_"+RHO+"_"+experiment_number+"_"+LocalDateTime.now()+".txt", jfi_list_h1);
				metricDistributionPerRegion(suboplist, regions2, "./heuristic2/heuristic2_dist_"+RHO+"_"+experiment_number+"_"+LocalDateTime.now()+".txt", jfi_list_h2);
				metricDistributionPerRegion(suboplist, regions3, "./heuristic3/heuristic3_dist_"+RHO+"_"+experiment_number+"_"+LocalDateTime.now()+".txt", jfi_list_h3);
				metricDistributionPerRegion(suboplist, regions4, "./heuristic4/heuristic4_dist_"+RHO+"_"+experiment_number+"_"+LocalDateTime.now()+".txt", jfi_list_h4);
				metricDistributionPerMachine(suboplist, machines5, "./heuristic5/heuristic5_dist_"+RHO+"_"+experiment_number+"_"+LocalDateTime.now()+".txt", jfi_list_h5);
				metricDistributionPerRegion(suboplist, regions6, "./heuristic6/heuristic6_dist_"+RHO+"_"+experiment_number+"_"+LocalDateTime.now()+".txt", jfi_list_h6);
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Calculating no. of exchange messages between controller and each machine...");
				Map<Integer, Integer> replicateAll = Utilities.messagesCounterReplicateAtAll(num_machines, suboplist);
				Map<Integer, Integer> queryAll = Utilities.messagesCounterQueryAll(num_machines, axis, suboplist);
				Map<Integer, Integer> hyperspace0 = Utilities.messagesCounterHyperspace(num_machines, suboplist, regions3);
				Map<Integer, Integer> hyperspace1 = Utilities.messagesCounterHyperspace(num_machines, suboplist, regions3);
				Map<Integer, Integer> hyperspace2 = Utilities.messagesCounterHyperspace(num_machines, suboplist, regions3);
				Map<Integer, Integer> hyperdex = Utilities.messagesCounterHyperspace(num_machines, suboplist, regions6);
				Map<Integer, Integer> cns = Utilities.messagesCounterHyperspace(num_machines, suboplist, regions1);
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Repartitioning using Heuristic 1...");
				Utilities.copyRegionsRanges(regions1, heuristic1.partition(op_window));
				System.out.println("["+LocalTime.now()+"] Done!");
				
				System.out.println("["+LocalTime.now()+"] Repartitioning using Heuristic 2...");
				Utilities.copyRegionsRanges(regions2, heuristic2.partition(op_window));
				quantilesPrinter(RHO, experiment_number, heuristic2.getQuantiles());
				System.out.println("["+LocalTime.now()+"] Done!");
							
				System.out.println("["+LocalTime.now()+"] Repartitioning using Heuristic 3...");
				Utilities.copyRegionsRanges(regions3, heuristic3.partitionGK());
				quantilesPrinter(RHO, experiment_number, heuristic3.getQuantiles());
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
					messagesPerMachineHyperdex.put(machine, messagesPerMachineHyperdex.get(machine)+hyperdex.get(machine));
					messagesPerMachineCNS.put(machine, messagesPerMachineCNS.get(machine)+cns.get(machine));
				}
				
				suboplist = new LinkedList<Operation>();
				window_moved = false;
				
			} else {
				
				Operation op = oplist.poll();
				
				window_moved = heuristic3.insertGK(op);
				
				if (window_moved) {	windowIsFull = true; }
				if (windowIsFull) { op_window.poll(); }
				op_window.add(op);
								
				suboplist.add(op);
								
			}
			
		}
		
	}
	
	public static void quantilesPrinter(String RHO, int experiment_number, Map<Double, Double> quantiles) {
		
		try {

			PrintWriter pw = new PrintWriter("./heuristic3/heuristic3_quant_"+RHO+"_"+experiment_number+"_"+LocalDateTime.now()+".txt");
			pw.println("phi\tquantile");	
	
			for (Map.Entry<Double, Double> entry : quantiles.entrySet()) {
				
				double phi = entry.getKey();
				double quant = entry.getValue();
				
				pw.println(phi+"\t"+quant);
				
			}
			
			pw.close();
			
		} catch (Exception err) {
			err.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		
		int num_attr = 3;
		int num_mach = 64;
		int num_max_guids = 8192; // 2^13 GUIDs
		
		int experiment_number = Integer.parseInt(args[0]);
		
		int sample_size = 262144; // 2^18 operations
//		double RHO = 0.5; // (# search operations) / (# operations)
		String rho_str = args[1];
		double RHO = Double.parseDouble(rho_str);
		
		int update_sample_size = (int)((1 - RHO)*sample_size);
		int search_sample_size = (int)(RHO*sample_size);
		
		System.out.println("Update sample size: "+update_sample_size);
		System.out.println("Search sample size: "+search_sample_size);
		
		int window_size;
		
		if (RHO == 0) {
			window_size = 16384; // 2^14 observations (touches)
		} else {
			window_size = 8388608; // 2^23 observations (touches)
		}
		
		String axis = args[2];
//		String axis = "A1";
		String metric = "touches";
	
		double e = 1/64d;
		
		int numOfEpochs = 4;

		Map<Integer, Map<String, Double>> guids = new TreeMap<Integer, Map<String, Double>>();
		
		List<String> sampleFileNames = new ArrayList<String>(numOfEpochs);
		for (int i = 1; i <= numOfEpochs; i++) {
			sampleFileNames.add("./samples/testing_sample_"+i+"_"+experiment_number+"_"+LocalDateTime.now()+".json");
		}
		
		/* GENERATING SAMPLES */
		
		ArrayList<String> possibleDistributions = new ArrayList<String>();
		possibleDistributions.add("uniform");
		possibleDistributions.add("normal");
		possibleDistributions.add("exponential");
		
		for (int i = 1; i <= numOfEpochs; i++) {
			System.out.println("["+LocalTime.now()+"] Generating testing sample "+i);
			
//			Random rnd = new Random(Integer.parseInt(args[3])+100*i);
			Random rnd = new Random();
			
			Map<String, Map<Integer, String>> distribution = new HashMap<String, Map<Integer, String>>();
			Map<String, Map<Integer, Map<String, Double>>>  distParams = new HashMap<String, Map<Integer, Map<String, Double>>>();
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
			
			Utilities.generateOperations(update_sample_size/numOfEpochs, search_sample_size/numOfEpochs, num_attr, num_max_guids, guids, distribution, distParams, sampleFileNames.get(i-1), rnd);
			
			System.out.println("["+LocalTime.now()+"] Done!");
		}
		
		/*
		// comment everything from here
		Map<String, Map<Integer, String>> distribution = Utilities.pickRandomDistribution(num_attr, possibleDistributions, new Random());
		for (int i = 1; i <= numOfEpochs; i++) {
			System.out.println("["+LocalTime.now()+"] Generating testing sample "+i);
			Map<String, Map<Integer, Map<String, Double>>>  distParams = new HashMap<String, Map<Integer, Map<String, Double>>>();
			Utilities.generateRandomDistribution(num_attr, distribution, distParams, new Random());
			
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
			
			Utilities.generateOperations(update_sample_size/numOfEpochs, search_sample_size/numOfEpochs, num_attr, num_max_guids, guids, distribution, distParams, sampleFileNames.get(i-1), new Random());
			
			System.out.println("["+LocalTime.now()+"] Done!");
		}
		// to here
		*/ 
		
		HeuristicV1 heuristic1 = new HeuristicV1(num_attr, num_mach);
		HeuristicV2 heuristic2 = new HeuristicV2(num_attr, num_mach, axis, metric);
		HeuristicV3 heuristic3 = new HeuristicV3(num_attr, num_mach, axis, metric, window_size, e);
		
		HeuristicV1_5 heuristic1_5 = new HeuristicV1_5(num_attr);
		
		/* PARTITIONING REGIONS */
		
		// Heuristic 1 (CNS)
		List<Region> regions1 = new ArrayList<Region>();
		regions1.addAll(heuristic1.getRegions()); 
		System.out.println("\nHeuristic 1 (CNS) regions:");
		System.out.println(Utilities.printRegions(regions1));
		
		// Heuristic 2 (Quantiles)
		List<Region> regions2 = new ArrayList<Region>();
		regions2.addAll(heuristic2.getRegions());
		System.out.println("\nHeuristic 2 (Quantiles) regions:");
		System.out.println(Utilities.printRegions(regions2));

		// Heuristic 3 (Quantiles+GK)
		List<Region> regions3 = new ArrayList<Region>();
		regions3.addAll(heuristic3.getRegions());
		System.out.println("\nHeuristic 3 (Quantiles+GK) regions:");
		System.out.println(Utilities.printRegions(regions3));

		// Heuristic 4 (Replicate-At-All)
		List<Region> regions4 = Utilities.buildNewRegions(num_attr);
		System.out.println("\nHeuristic 4 (Replicate-At-All) regions:");
		System.out.println(Utilities.printRegions(regions4));
		
		// Heuristic 5 (Query-All)
		List<Machine> machines5 = new ArrayList<Machine>();
		for (int i = 1; i <= num_mach; i++) { machines5.add(new Machine("machine"+i)); }
		System.out.println("\nHeuristic 5 (Query-All) does not work with regions");
		
		// Heuristic 6 (Hyperdex)
		List<Region> regions6 = new ArrayList<Region>();
		regions6.addAll(heuristic1_5.partition());
		System.out.println("\nHeuristic 6 (Hyperdex) regions:");
		System.out.println(Utilities.printRegions(regions6)+"\n");
				
		/* TESTING HEURISTICS */
		
		Map<String, List<Double>> jfi_list_h1 = new HashMap<String, List<Double>>();
		jfi_list_h1.put("touches", new ArrayList<Double>());
		jfi_list_h1.put("guids", new ArrayList<Double>());
		
		Map<String, List<Double>> jfi_list_h2 = new HashMap<String, List<Double>>();
		jfi_list_h2.put("touches", new ArrayList<Double>());
		jfi_list_h2.put("guids", new ArrayList<Double>());
		
		Map<String, List<Double>> jfi_list_h3 = new HashMap<String, List<Double>>();
		jfi_list_h3.put("touches", new ArrayList<Double>());
		jfi_list_h3.put("guids", new ArrayList<Double>());
		
		Map<String, List<Double>> jfi_list_h4 = new HashMap<String, List<Double>>();
		jfi_list_h4.put("touches", new ArrayList<Double>());
		jfi_list_h4.put("guids", new ArrayList<Double>());
		
		Map<String, List<Double>> jfi_list_h5 = new HashMap<String, List<Double>>();
		jfi_list_h5.put("touches", new ArrayList<Double>());
		jfi_list_h5.put("guids", new ArrayList<Double>());
		
		Map<String, List<Double>> jfi_list_h6 = new HashMap<String, List<Double>>();
		jfi_list_h6.put("touches", new ArrayList<Double>());
		jfi_list_h6.put("guids", new ArrayList<Double>());
		
		Map<Integer, Integer> messagesPerMachineReplicateAll = new TreeMap<Integer, Integer>();		
		Map<Integer, Integer> messagesPerMachineQueryAll = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> messagesPerMachineHyperspace0 = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> messagesPerMachineHyperspace1 = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> messagesPerMachineHyperspace2 = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> messagesPerMachineHyperdex = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> messagesPerMachineCNS = new TreeMap<Integer, Integer>();
		for (int machine = 1; machine <= num_mach; machine++) {
			messagesPerMachineReplicateAll.put(machine, 0);
			messagesPerMachineQueryAll.put(machine, 0);
			messagesPerMachineHyperspace0.put(machine, 0);
			messagesPerMachineHyperspace1.put(machine, 0);
			messagesPerMachineHyperspace2.put(machine, 0);
			messagesPerMachineHyperdex.put(machine, 0);
			messagesPerMachineCNS.put(machine, 0);
		}
		
		// tests heuristics
		testHeuristicsAgainstNewOperations(rho_str, experiment_number, sampleFileNames, regions1, regions2, regions3, regions4, machines5, regions6, heuristic1, heuristic2, heuristic3, jfi_list_h1, jfi_list_h2, jfi_list_h3, jfi_list_h4, jfi_list_h5, jfi_list_h6, num_mach, axis, messagesPerMachineReplicateAll, messagesPerMachineQueryAll, messagesPerMachineHyperspace0, messagesPerMachineHyperspace1, messagesPerMachineHyperspace2, messagesPerMachineHyperdex, messagesPerMachineCNS);

		// Writes log files to generate graphs with gnuplot
		PrintWriter pw_h1 = null, pw_h2 = null, pw_h3 = null, pw_h4 = null, pw_h5 = null, pw_h6 = null, pw_replicateAll = null, pw_queryAll = null, pw_hyperspace0 = null, pw_hyperspace1 = null, pw_hyperspace2 = null, pw_hyperdex = null, pw_cns = null;
		
		try {
			
			pw_h1 = new PrintWriter("./output/heuristic1_jfi_"+rho_str+"_"+experiment_number+".txt");
			pw_h2 = new PrintWriter("./output/heuristic2_jfi_"+rho_str+"_"+experiment_number+".txt");
			pw_h3 = new PrintWriter("./output/heuristic3_jfi_"+rho_str+"_"+experiment_number+".txt");
			pw_h4 = new PrintWriter("./output/heuristic4_jfi_"+rho_str+"_"+experiment_number+".txt");
			pw_h5 = new PrintWriter("./output/heuristic5_jfi_"+rho_str+"_"+experiment_number+".txt");
			pw_h6 = new PrintWriter("./output/heuristic6_jfi_"+rho_str+"_"+experiment_number+".txt");
			
			pw_h1.println("# repartition\tJFI (touches)\tJFI (guids)");
			pw_h2.println("# repartition\tJFI (touches)\tJFI (guids)");
			pw_h3.println("# repartition\tJFI (touches)\tJFI (guids)");
			pw_h4.println("# repartition\tJFI (touches)\tJFI (guids)");
			pw_h5.println("# repartition\tJFI (touches)\tJFI (guids)");
			pw_h6.println("# repartition\tJFI (touches)\tJFI (guids)");
			
			for (int i = 0; i < jfi_list_h1.get("touches").size(); i++) {
				pw_h1.println((i+1)+"\t"+jfi_list_h1.get("touches").get(i)+"\t"+jfi_list_h1.get("guids").get(i));
				pw_h2.println((i+1)+"\t"+jfi_list_h2.get("touches").get(i)+"\t"+jfi_list_h2.get("guids").get(i));
				pw_h3.println((i+1)+"\t"+jfi_list_h3.get("touches").get(i)+"\t"+jfi_list_h3.get("guids").get(i));
				pw_h4.println((i+1)+"\t"+jfi_list_h4.get("touches").get(i)+"\t"+jfi_list_h4.get("guids").get(i));
				pw_h5.println((i+1)+"\t"+jfi_list_h5.get("touches").get(i)+"\t"+jfi_list_h5.get("guids").get(i));
				pw_h6.println((i+1)+"\t"+jfi_list_h6.get("touches").get(i)+"\t"+jfi_list_h6.get("guids").get(i));
			}
			
			pw_replicateAll = new PrintWriter("./output/replicateAll_msgs_"+rho_str+"_"+experiment_number+".txt");
			pw_queryAll = new PrintWriter("./output/queryAll_msgs_"+rho_str+"_"+experiment_number+".txt");
			pw_hyperspace0 = new PrintWriter("./output/hyperspace0_msgs_"+rho_str+"_"+experiment_number+".txt");
			pw_hyperspace1 = new PrintWriter("./output/hyperspace1_msgs_"+rho_str+"_"+experiment_number+".txt");
			pw_hyperspace2 = new PrintWriter("./output/hyperspace2_msgs_"+rho_str+"_"+experiment_number+".txt");
			pw_hyperdex = new PrintWriter("./output/hyperdex_msgs_"+rho_str+"_"+experiment_number+".txt");
			pw_cns = new PrintWriter("./output/cns_msgs_"+rho_str+"_"+experiment_number+".txt");
			
			pw_replicateAll.println("# machine\tmessages");
			pw_queryAll.println("# machine\tmessages");
			pw_hyperspace0.println("# machine\tmessages");
			pw_hyperspace1.println("# machine\tmessages");
			pw_hyperspace2.println("# machine\tmessages");
			pw_hyperdex.println("# machine\tmessages");
			pw_cns.println("# machine\tmessages");
			
			for (int machine = 1; machine <= num_mach; machine++) {
				pw_replicateAll.println(machine+"\t"+messagesPerMachineReplicateAll.get(machine));
				pw_queryAll.println(machine+"\t"+messagesPerMachineQueryAll.get(machine));
				pw_hyperspace0.println(machine+"\t"+messagesPerMachineHyperspace0.get(machine));
				pw_hyperspace1.println(machine+"\t"+messagesPerMachineHyperspace1.get(machine));
				pw_hyperspace2.println(machine+"\t"+messagesPerMachineHyperspace2.get(machine));
				pw_hyperdex.println(machine+"\t"+messagesPerMachineHyperdex.get(machine));
				pw_cns.println(machine+"\t"+messagesPerMachineCNS.get(machine));
			}
			
			
		} catch (FileNotFoundException err) {
			err.printStackTrace();
		} finally {
			pw_h1.close();
			pw_h2.close();
			pw_h3.close();
			pw_h4.close();
			pw_h5.close();
			pw_h6.close();
			pw_replicateAll.close();
			pw_queryAll.close();
			pw_hyperspace0.close();
			pw_hyperspace1.close();
			pw_hyperspace2.close();
			pw_hyperdex.close();
			pw_cns.close();
		}

	}

}