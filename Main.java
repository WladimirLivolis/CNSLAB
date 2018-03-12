import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
		
		int num_attr = 1;
		int num_mach = 64;
		int num_max_guids = 500;
		int num_update_training_samples = 10000;
		int num_search_training_samples = 10000;
		int num_update_new_samples = 10000;
		int num_search_new_samples = 10000;
		int num_experiments = 100;
		
		String dist = "uniform";
		
		boolean touches = true;
				
		Map<String, Range> pairs = new HashMap<String, Range>(); // pairs attribute-range
		
		for (int i = 1; i <= num_attr; i++) {
			pairs.put("A"+i, new Range(0.0, 1.0));
		}
		
		Region region = new Region("R1", pairs);	
		
		List<Region> regions = new ArrayList<Region>();
		regions.add(region);
		
		HeuristicV1 heuristic1 = new HeuristicV1(num_mach, regions);
		HeuristicV2 heuristic2 = new HeuristicV2(num_mach, regions);
		HeuristicV2 heuristic3 = new HeuristicV2(num_mach, regions);
				
		// Generates update & search loads for training
		Queue<Update> uplist = Utilities.generateUpdateLoad(num_attr, num_update_training_samples, num_max_guids, dist, rnd);
		Queue<Search> slist  = Utilities.generateSearchLoad(num_attr, num_search_training_samples, dist, rnd);
		
		// Sort operations
		Queue<Operation> oplist = Utilities.sortOperations(slist, uplist, rnd);
		
		for (int h = 1; h <= 3; h++) {
						
			String fileName;
			
			if (h==1) {
				fileName = "heuristic1.txt";
				regions = heuristic1.partition(oplist);
			} else if (h==2) {
				fileName = "heuristic2.txt";
				regions = heuristic2.partition("touches", oplist);
			} else {
				fileName = "heuristic3.txt";
				regions = heuristic3.partition("load", oplist);
			}
			
			if (touches) {
				System.out.println(Utilities.JFI("touches", oplist, regions)+"\n");
			} else {
				System.out.println(Utilities.JFI("load", oplist, regions)+"\n");
			}
			
			Map<Integer, List<Double>> metricValuesPerRegion = new TreeMap<Integer, List<Double>>();
			
			ArrayList<Double> JFIs = new ArrayList<Double>(num_experiments);
			
			for (int i = 1; i <= num_experiments; i++) {
				
				rnd = new Random(i);
				
				// Generates new update & search loads
				Queue<Update> newUplist = Utilities.generateUpdateLoad(num_attr, num_update_new_samples, num_max_guids, dist, rnd);
				Queue<Search> newSlist  = Utilities.generateSearchLoad(num_attr, num_search_new_samples, dist, rnd);
				
				// Sort operations
				Queue<Operation> newOplist = Utilities.sortOperations(newSlist, newUplist, rnd);
				
				// Calculates JFI
				if (touches) {
					JFIs.add(Utilities.JFI("touches", newOplist, regions));
				} else {
					JFIs.add(Utilities.JFI("load", newOplist, regions));
				}
				
				for (Region r : regions) {
					
					int index = regions.indexOf(r)+1;
					
					double metricValue = 0;
					if (touches) {
						metricValue = r.getUpdateTouches()+r.getSearchTouches();
					} else {
						metricValue = r.getUpdateLoad()+r.getSearchLoad();
					}
					
					if (!metricValuesPerRegion.containsKey(index)) {
						ArrayList<Double> metricValues = new ArrayList<Double>(num_experiments);
						metricValues.add(metricValue);
						metricValuesPerRegion.put(index, metricValues);
					} else {
						metricValuesPerRegion.get(index).add(metricValue);
					}
					
				}
				
			}
			
			Map<Integer, Double> meansPerRegion = new TreeMap<Integer, Double>();
	
			for (Map.Entry<Integer, List<Double>> e : metricValuesPerRegion.entrySet()) {
				
				List<Double> metricValues = e.getValue();
				
				double sum = 0;
				for (double val : metricValues) {
					sum += val;
				}
				
				double mean = sum / metricValues.size();
				meansPerRegion.put(e.getKey(), mean);
				
			}
			
			Map<Integer, Double> CI = calculateConfidenceInterval(metricValuesPerRegion, meansPerRegion);
			
			double sum = 0.0;
			for (double d : JFIs)
				sum += d;
			double JFI_mean = sum/JFIs.size();
			
			File output1 = new File(fileName);
			FileWriter fw1 = null;
			BufferedWriter bw1 = null;
			
			try {
				
				fw1 = new FileWriter(output1);
				bw1 = new BufferedWriter(fw1);
				bw1.write("x,\tmean,\tdelta");
				
				for (Map.Entry<Integer, Double> e : CI.entrySet()) {
					
					bw1.newLine();
					bw1.write(e.getKey()+",\t"+meansPerRegion.get(e.getKey())+",\t"+e.getValue());
					
				}
				
				bw1.newLine();
				bw1.write("JFI:\t"+JFI_mean);				
				
			} catch (IOException e) { e.printStackTrace(); }
			
			finally { try { bw1.close(); fw1.close(); } catch (Exception e) {} }
		
		}
		
	}

}