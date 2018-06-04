import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;

public class Main {
	
	private static Map<Integer, Double> calculateConfidenceInterval(Map<Integer, List<Double>> values, Map<Integer, Double> mean) {
		
		Map<Integer, Double> confidenceInterval = new TreeMap<Integer, Double>();

		for (Map.Entry<Integer, List<Double>> e : values.entrySet()) {
			
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
	
	private static double calculateConfidenceIterval(ArrayList<Double> values, double mean) {
		
		double squaredDifferenceSum = 0.0, variance = 0.0, standardDeviation = 0.0;
		
		for (double num : values) {
		
			squaredDifferenceSum += (num - mean) * (num - mean);
		
		}
		
		variance = squaredDifferenceSum / values.size();
		standardDeviation = Math.sqrt(variance);
		
		// value for 95% confidence interval
	    double confidenceLevel = 1.96;
	    double delta = confidenceLevel * standardDeviation / Math.sqrt(values.size());
		
		return delta;
		
	}
	
	public static void main(String[] args) {
		
		int num_attr = 3;
		int num_mach = 64;
		int num_max_guids = 500;
		int num_update_training_samples = 500000;
		int num_search_training_samples = 500000;
		int num_update_new_samples = 500000;
		int num_search_new_samples = 500000;
		int num_experiments = 100;
		
		String dist = "uniform";
		
		boolean touches = true;
		
		// Generates update & search loads for training
		System.out.println("["+LocalTime.now()+"] Generating training load...");
		Utilities.generateOperations(num_update_training_samples, num_search_training_samples, num_attr, num_max_guids, dist, "training.json", new Random());
		
		// Generates update & search loads for our experiments
		System.out.println("["+LocalTime.now()+"] Generating experimental load...");
		for (int i = 1; i <= num_experiments; i++) {
			Utilities.generateOperations(num_update_new_samples, num_search_new_samples, num_attr, num_max_guids, dist, "./experiment_load/experiment"+i+".json", new Random());
		}
				
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
				
		Queue<Operation> oplist = Utilities.readOperationsFile("training.json");
		
		for (int h = 1; h <= 3; h++) {
			
			String fileName;
			
			if (h==1) {
				fileName = "heuristic1.txt";
				System.out.println("["+LocalTime.now()+"] Starting heuristic I...\n");
				regions = heuristic1.partition(oplist);
			} else if (h==2) {
				fileName = "heuristic2.txt";
				System.out.println("["+LocalTime.now()+"] Starting heuristic II...\n");
				if (touches) {
					regions = heuristic2.partition("touches", "A1", oplist);
				} else {
					regions = heuristic2.partition("load", "A1", oplist);
				}
			} else {
				fileName = "heuristic3.txt";
				System.out.println("["+LocalTime.now()+"] Starting heuristic III...\n");
				if (touches) {
					regions = heuristic3.partitionGK("touches", "A1", 1/512d, oplist);
				} else {
					regions = heuristic3.partitionGK("load", "A1", 1/8d, oplist);
				}
			}
			
			if (touches) {
				System.out.println("JFI: "+Utilities.JFI("touches", oplist, regions)+'\n');
			} else {
				System.out.println("JFI: "+Utilities.JFI("load", oplist, regions)+'\n');
			}
			
			Map<Integer, List<Double>> metricValuesPerRegion = new TreeMap<Integer, List<Double>>();
			
			ArrayList<Double> JFIs = new ArrayList<Double>(num_experiments);
			
			for (int i = 1; i <= num_experiments; i++) {
				
				Queue<Operation> newOplist = Utilities.readOperationsFile("./experiment_load/experiment"+i+".json");
				
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
			
			double JFI_confidence = calculateConfidenceIterval(JFIs, JFI_mean);

			PrintWriter pw;
			
			try {
				
				pw = new PrintWriter(fileName);
				
				pw.println("x,\tmean,\tdelta");
				
				for (Map.Entry<Integer, Double> e : CI.entrySet()) {
								
					pw.println(e.getKey()+",\t"+meansPerRegion.get(e.getKey())+",\t"+e.getValue());
					
				}
				
				pw.println();
				pw.println("JFI,\tdelta");
				pw.print(JFI_mean+"\t"+JFI_confidence);
				
				pw.close();
				
			} catch (FileNotFoundException e) { 
				e.printStackTrace(); 
			}
			
		}
		
	}

}