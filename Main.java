import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;

public class Main {
	
	private static Map<Integer, Double> calculateConfidenceInterval(Map<Integer, List<Double>> load, Map<Integer, Double> mean) {
		
		Map<Integer, Double> confidenceInterval = new TreeMap<Integer, Double>();

		for (Map.Entry<Integer, List<Double>> e : load.entrySet()) {
			
			double squaredDifferenceSum = 0.0, variance = 0.0, standardDeviation = 0.0;
			
			for (double num : e.getValue()) {
			
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
	
	public static void main(String[] args) {
		
		Random rnd = new Random();
		
		int num_attr = 3;
		int num_mach = 64;
		int num_GUIDs = 500;
		int num_update_training_samples = 5000;
		int num_search_training_samples = 5000;
		int num_update_new_samples = 5000;
		int num_search_new_samples = 5000;
		int num_experiments = 100;
		
		boolean touches = true;
				
		List<PairAttributeRange> pairs = new ArrayList<PairAttributeRange>();
		pairs.add(new PairAttributeRange("A1", new Range(0.0, 1.0)));
		pairs.add(new PairAttributeRange("A2", new Range(0.0, 1.0)));
		pairs.add(new PairAttributeRange("A3", new Range(0.0, 1.0)));
		
		Region region = new Region("R1", pairs);		
		
		List<Region> regions = new ArrayList<Region>();
		regions.add(region);
		
		HeuristicV1 heuristic1 = new HeuristicV1(num_mach, regions);
		HeuristicV2 heuristic2 = new HeuristicV2(num_mach, regions);
		
		// Generates update & search loads for training
		List<GUID> GUIDs     = Utilities.generateGUIDs(num_GUIDs, num_attr, rnd); 
		Queue<Update> uplist = Utilities.generateUpdateLoad(num_attr, num_update_training_samples, GUIDs, rnd);
		Queue<Search> slist  = Utilities.generateSearchLoad(num_attr, num_search_training_samples, rnd);
		
		// Sort operations
		Queue<Operation> oplist = Utilities.sortOperations(slist, uplist, rnd);
		
		for (int h = 1; h <= 2; h++) {
			
			// Copy Oplist & GUIDs
			List<GUID> copyOfGUIDs = Utilities.copyGUIDs(GUIDs);
			Queue<Operation> copyOfOplist = Utilities.copyOplist(oplist, copyOfGUIDs);
						
			String fileName = "experiment1.txt";
			
			if (h==1) {
				fileName = "heuristic1.txt";
				regions = heuristic1.partition(oplist);
			} else {
				fileName = "heuristic2.txt";
				regions = heuristic2.partition(GUIDs, oplist);
			}
			
			if (touches) {
				System.out.println(Utilities.JFI(copyOfOplist, copyOfGUIDs, regions)+"\n");
			} else {
				System.out.println(Utilities.JFI(oplist, regions)+"\n");
			}
			
			Map<Integer, List<Double>> realLoad = new TreeMap<Integer, List<Double>>();
			
			ArrayList<Double> JFIs = new ArrayList<Double>(num_experiments);
			
			for (int i = 1; i <= num_experiments; i++) {
				
				rnd = new Random(i);
				
				// Generates new update & search loads
				List<GUID> newGUIDs     = Utilities.generateGUIDs(num_GUIDs, num_attr, rnd); 
				Queue<Update> newUplist = Utilities.generateUpdateLoad(num_attr, num_update_new_samples, newGUIDs, rnd);
				Queue<Search> newSlist  = Utilities.generateSearchLoad(num_attr, num_search_new_samples, rnd);
				
				// Sort operations
				Queue<Operation> newOplist = Utilities.sortOperations(newSlist, newUplist, rnd);
				
				// Calculates JFI
				if (touches) {
					JFIs.add(Utilities.JFI(newOplist, newGUIDs, regions));
				} else {
					JFIs.add(Utilities.JFI(newOplist, regions));
				}
				
				for (Region r : regions) {
					
					int index = regions.indexOf(r)+1;
					
					double totalLoad = 0;
					if (touches) {
						totalLoad = r.getUpdateTouches()+r.getSearchTouches();
					} else {
						totalLoad = r.getUpdateLoad()+r.getSearchLoad();
					}
					
					if (!realLoad.containsKey(index)) {
						ArrayList<Double> load = new ArrayList<Double>(num_experiments);
						load.add(totalLoad);
						realLoad.put(index, load);
					} else {
						realLoad.get(index).add(totalLoad);
					}
					
				}
				
			}
			
			Map<Integer, Double> meanLoad = new TreeMap<Integer, Double>();
	
			for (Map.Entry<Integer, List<Double>> e : realLoad.entrySet()) {
				
				List<Double> load = e.getValue();
				
				double sum = 0;
				for (double l : load) {
					sum += l;
				}
				
				double avg = sum / load.size();
				meanLoad.put(e.getKey(), avg);
				
			}
			
			Map<Integer, Double> CI = calculateConfidenceInterval(realLoad, meanLoad);
			
			double sum = 0.0;
			for (double d : JFIs)
				sum += d;
			double JFI_avg = sum/JFIs.size();
			
			File output1 = new File(fileName);
			FileWriter fw1 = null;
			BufferedWriter bw1 = null;
			
			try {
				
				fw1 = new FileWriter(output1);
				bw1 = new BufferedWriter(fw1);
				bw1.write("x,\tmean,\tdelta");
				
				for (Map.Entry<Integer, Double> e : CI.entrySet()) {
					
					bw1.newLine();
					bw1.write(e.getKey()+",\t"+meanLoad.get(e.getKey())+",\t"+e.getValue());
					
				}
				
				bw1.newLine();
				bw1.write("JFI:\t"+JFI_avg);				
				
			} catch (IOException e) { e.printStackTrace(); }
			
			finally { try { bw1.close(); fw1.close(); } catch (Exception e) {} }
		
		}
		
	}

}