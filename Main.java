import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Main {
	
	private static ArrayList<Update> generateUpdateLoad(int AttrNum, int UpNum, Random rnd) {
		ArrayList<Update> updates = new ArrayList<Update>(UpNum);
		for (int i = 0; i < UpNum; i++) {
			Update up = new Update("U"+i);
			for (int j = 1; j <= AttrNum; j++) {
				double v = rnd.nextDouble();
				up.addAttr("A"+j, v);
			}
			updates.add(up);
		}
		return updates;
	}
	
	private static ArrayList<Search> generateSearchLoad(int AttrNum, int SNum, Random rnd) {
		ArrayList<Search> searches = new ArrayList<Search>(SNum);
		for (int i = 0; i < SNum; i++) {
			Search s = new Search("S"+i);
			for (int j = 1; j <= AttrNum; j++) {
				double v1 = rnd.nextDouble();
				double v2 = rnd.nextDouble();
				double low, high;
				if (v1 < v2) {
					low = v1;
					high = v2;
				} else {
					low = v2;
					high = v1;
				}
				s.addPair("A"+j, new Range(low, high));
			}
			searches.add(s);
		}
		return searches;
	}
	
	public static void main(String[] args) {
	
 /*
		
		Random rnd = new Random();
		//rnd.setSeed(0);
		
		int num_attr = 3;
		int num_mach = 16;
		int num_samples = 1000;
				
		List<PairAttributeRange> pairs = new ArrayList<PairAttributeRange>();
		pairs.add(new PairAttributeRange("A1", new Range(0.0, 1.0)));
		pairs.add(new PairAttributeRange("A2", new Range(0.0, 1.0)));
		pairs.add(new PairAttributeRange("A3", new Range(0.0, 1.0)));
		
		Region region = new Region("R1", pairs);		
		
		List<Region> regions = new ArrayList<Region>();
		regions.add(region);
		
		HeuristicV1 heuristic1 = new HeuristicV1(num_mach, regions);
		//HeuristicV2 heuristic2 = new HeuristicV2(num_mach, regions);
		
		// Generates update load
		
		ArrayList<Update> uplist = generateUpdateLoad(num_attr, num_samples, rnd);
		
		for (Update up : uplist) {
			
			System.out.println("Update Id: " + up.getId());
			System.out.println("Attributes: " + up.toString());
			
		}

		System.out.println("\n");
		
		// Generates search load
		
		ArrayList<Search> slist = generateSearchLoad(num_attr, num_samples, rnd);
		
		for (Search s : slist) {
			
			System.out.println("Search Id: " + s.getId());
			System.out.println("PairAttributes: " + s.toString());
			
		}
		
		regions = heuristic1.partition(uplist, slist);
		System.out.println("\nRegions = "+heuristic1.toString());
		//regions = heuristic2.partition(uplist, slist);	
		//System.out.println("\nRegions = "+heuristic2.toString());
		
		
		
		File output = new File("./output.txt"); // This file will receive info on the # of touches per region as well as JFI index
		FileWriter fw = null;
		BufferedWriter bw = null;

		try {
			
			// Generates new update & search loads
			ArrayList<Update> newUplist = generateUpdateLoad(num_attr, 20, rnd);
			ArrayList<Search> newSlist = generateSearchLoad(num_attr, 20, rnd);
						
			fw = new FileWriter(output);
			bw = new BufferedWriter(fw);
			bw.write("Region\t# of touches");
					
			// Checks touches
			for (Region r : regions) {
				
				List<Update> myUpdateLoad = r.getUpdateLoad(newUplist);
				
				List<Search> mySearchLoad = r.getSearchLoad(newSlist);
				
				System.out.println("\nRegion " + r.getName());
				
				for (Update up : myUpdateLoad)
					System.out.println(up.getId());
					
				
				for (Search s : mySearchLoad)
					System.out.println(s.getId());
				
				int touches = myUpdateLoad.size()+mySearchLoad.size();
				
				bw.newLine();
				bw.write(r.getName()+"\t"+touches);
								
			}
			
			double JFIndex = heuristic1.JFI(newUplist, newSlist, regions);
			
			bw.newLine();
			bw.write("# of Samples\tJFI");
			bw.newLine();
			bw.write(num_samples+"\t"+JFIndex);
			
		
		} catch (IOException e) { e.printStackTrace(); }
		
		finally { try { bw.close(); fw.close(); } catch (Exception e) {} }
	
	*/	
		
		/* **** First Experiment: touches per region **** */
		
		Random rnd = new Random();
		//rnd.setSeed(0);
		
		int num_attr = 3;
		int num_mach = 81;
		int num_training_samples = 10000;
		int num_new_samples = 10000;
		int num_experiments = 100;
				
		List<PairAttributeRange> pairs = new ArrayList<PairAttributeRange>();
		pairs.add(new PairAttributeRange("A1", new Range(0.0, 1.0)));
		pairs.add(new PairAttributeRange("A2", new Range(0.0, 1.0)));
		pairs.add(new PairAttributeRange("A3", new Range(0.0, 1.0)));
		
		Region region = new Region("R1", pairs);		
		
		List<Region> regions = new ArrayList<Region>();
		regions.add(region);
		
		//HeuristicV1 heuristic1 = new HeuristicV1(num_mach, regions);
		HeuristicV2 heuristic2 = new HeuristicV2(num_mach, regions);
		
		// Generates update & search loads for training
		ArrayList<Update> uplist = generateUpdateLoad(num_attr, num_training_samples, rnd);
		ArrayList<Search> slist  = generateSearchLoad(num_attr, num_training_samples, rnd);
		//ArrayList<Search> slist = new ArrayList<Search>();
		
		//regions = heuristic1.partition(uplist, slist);
		regions = heuristic2.partition(uplist, slist);	
		
		Map<String, List<Integer>> map1 = new HashMap<String, List<Integer>>();
		
		for (int i = 0; i < num_experiments; i++) {
			
			rnd = new Random();
			
			// Generates new update & search loads
			ArrayList<Update> newUplist = generateUpdateLoad(num_attr, num_new_samples, rnd);
			ArrayList<Search> newSlist  = generateSearchLoad(num_attr, num_new_samples, rnd);
			//ArrayList<Search> newSlist = new ArrayList<Search>();

			// Checks touches
			for (Region r : regions) {
				
				int index = regions.indexOf(r)+1;
				
				List<Update> myUpdateLoad = r.getUpdateLoad(newUplist);
				List<Search> mySearchLoad = r.getSearchLoad(newSlist);
				
				int touches = myUpdateLoad.size()+mySearchLoad.size();
				
				if (!map1.containsKey("R"+index)) {
					ArrayList<Integer> t = new ArrayList<Integer>(num_experiments);
					t.add(touches);
					map1.put("R"+index, t);
				} else {
					map1.get("R"+index).add(touches);
				}
				
			}
			
		}
		
		Map<String, Double> map2 = new HashMap<String, Double>();

		for (Map.Entry<String, List<Integer>> e : map1.entrySet()) {
			
			List<Integer> touches = e.getValue();
			
			Integer sum = 0;
			for (Integer touch : touches) {
				sum += touch;
			}
			
			double avg = sum.doubleValue() / touches.size();
			map2.put(e.getKey(), avg);
			
		}
		
		File output1 = new File("./experiment1.txt");
		FileWriter fw1 = null;
		BufferedWriter bw1 = null;
		
		try {
			
			fw1 = new FileWriter(output1);
			bw1 = new BufferedWriter(fw1);
			bw1.write("Region\t# of touches");
			
			for (Map.Entry<String, Double> e : map2.entrySet()) {
				
				bw1.newLine();
				bw1.write(e.getKey()+"\t"+e.getValue());
				
			}
			
			
		} catch (IOException e) { e.printStackTrace(); }
		
		finally { try { bw1.close(); fw1.close(); } catch (Exception e) {} }
	
		
		/* **** Second Experiment: JFI VS # of samples **** */

	/*	
		
		Random rnd = new Random();
		rnd.setSeed(0);
		
		int num_attr = 3;
		int num_mach = 16;
		int num_experiments = 30;
		
		int[] num_samples = {10, 25, 50, 100, 250, 500, 1000};
		
		ArrayList<PairAttributeRange> pairs = new ArrayList<PairAttributeRange>();
		pairs.add(new PairAttributeRange("A1", new Range(0.0, 1.0)));
		pairs.add(new PairAttributeRange("A2", new Range(0.0, 1.0)));
		pairs.add(new PairAttributeRange("A3", new Range(0.0, 1.0)));
						
		Map<Integer, List<Double>> map1 = new HashMap<Integer, List<Double>>(num_samples.length);
		
		for (int i = 0; i < num_experiments; i++) {
			
			for (int j = 0; j < num_samples.length; j++) {
				
				Region region = new Region("R1", pairs);

				List<Region> regions = new ArrayList<Region>();
				regions.add(region);
				
				HeuristicV1 heuristic1 = new HeuristicV1(num_mach, regions);
			
				// Generates update & search loads for training
				ArrayList<Update> uplist = generateUpdateLoad(num_attr, num_samples[j], rnd);
				ArrayList<Search> slist  = generateSearchLoad(num_attr, num_samples[j], rnd);
				
				regions = heuristic1.partition(uplist, slist);
				
				// Generates new update & search loads
				ArrayList<Update> newUplist = generateUpdateLoad(num_attr, num_samples[j], rnd);
				ArrayList<Search> newSlist  = generateSearchLoad(num_attr, num_samples[j], rnd);
				
				double JFIndex = heuristic1.JFI(newUplist, newSlist, regions);
				
				if (!map1.containsKey(num_samples[j])) {
					ArrayList<Double> jfis = new ArrayList<Double>(num_experiments);
					jfis.add(JFIndex);
					map1.put(num_samples[j], jfis);
				} else {
					map1.get(num_samples[j]).add(JFIndex);
				}
				
			}
			
		}
		
		Map<Integer, Double> map2 = new HashMap<Integer, Double>(num_samples.length);

		for (Map.Entry<Integer, List<Double>> e : map1.entrySet()) {
			
			List<Double> jfis = e.getValue();
			
			Double sum = 0.0;
			for (Double jfi : jfis) {
				sum += jfi;
			}
			
			double avg = sum.doubleValue() / jfis.size();
			map2.put(e.getKey(), avg);
			
		}
			
		File output2 = new File("./experiment2.txt");
		FileWriter fw2 = null;
		BufferedWriter bw2 = null;
		
		try {
			
			fw2 = new FileWriter(output2);
			bw2 = new BufferedWriter(fw2);
			bw2.write("# of Samples\tJFI");

			for (Map.Entry<Integer, Double> e : map2.entrySet()) {
				
				bw2.newLine();
				bw2.write("");
				bw2.write(e.getKey()+"\t"+e.getValue());
				
			}
			
			
		} catch (IOException e) { e.printStackTrace(); }
		
		finally { try { bw2.close(); fw2.close(); } catch (Exception e) {} }
	
	*/	
		
	}

}