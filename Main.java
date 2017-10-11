import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

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
				s.addPair("A"+j, new Range(v1, v2));
			}
			searches.add(s);
		}
		return searches;
	}
	
	private static void generateSearchLoadFile(int AttrNum, int SNum, Random rnd) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("search_load.txt")));
		bw.write(""+AttrNum);
		bw.write(" "+SNum);
		for (int i = 0; i < SNum; i++) {
			bw.write("\nS"+i);
			for (int j = 1; j <= AttrNum; j++) {
				double v1 = rnd.nextDouble();
				double v2 = rnd.nextDouble();
//				if (v1 > v2) {
//					bw.write("\nA"+j);
//					bw.write(" "+v1);
//					bw.write(" "+1.0);
//					bw.write("\nA"+j);
//					bw.write(" "+0.0);
//					bw.write(" "+v2);
//				} else {
					bw.write("\nA"+j);
					bw.write(" "+v1);
					bw.write(" "+v2);
//				}
			}
		}
		bw.close();
	}
	
	private static void checkSearchLoadDistribution() throws IOException {
		
		Random rnd = new Random(100);
		generateSearchLoadFile(1, 1000, rnd);
		List<Search> slist = readSearchLoadFile();
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("search_load_dist.txt")));
		
		for (double d = 0.0; d <= 1.0; d += 0.01) {
			
			int count = 0;
			for (Search s : slist) {
				double range_start = s.getPairs().get(0).getRange().getLow();
				double range_end   = s.getPairs().get(0).getRange().getHigh();
				
				if (range_start > range_end) { // [a,b] ^ a > b --> [a,1.0] ^ [0.0,b] 
										
					if ( (d >= range_start && d <= 1.0) || (d >= 0.0 && d <= range_end) ) { count++; }
					
				} else {
					
					if (d >= range_start && d <= range_end) { count++; }
					
				}
			}
			
			bw.write(d+",\t");
			bw.write(count+"\n");
						
		}
		
		bw.close();
		
	}
	
	private static ArrayList<Search> readSearchLoadFile() throws IOException {
		ArrayList<Search> searches = new ArrayList<Search>();
		
		BufferedReader br = new BufferedReader(new FileReader("search_load.txt"));
		String[] firstLine = br.readLine().split(" ");
		int attrNum = Integer.parseInt(firstLine[0]);
		int SNum = Integer.parseInt(firstLine[1]);
		
		for (int i = 0; i < SNum; i++) {
			Search s = new Search(br.readLine());
			for (int j = 0; j < attrNum; j++) {
				String[] attrLine = br.readLine().split(" ");
				s.addPair(attrLine[0], new Range(Double.parseDouble(attrLine[1]), Double.parseDouble(attrLine[2])));
			}
			searches.add(s);
		}
		
		br.close();
		
		return searches;
	}
	
	private static void generateUpdateLoadFile(int AttrNum, int UpNum, Random rnd) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("update_load.txt")));
		bw.write(""+AttrNum);
		bw.write(" "+UpNum);
		for (int i = 0; i < UpNum; i++) {
			bw.write("\nU"+i);
			for (int j = 1; j <= AttrNum; j++) {
				double v = rnd.nextDouble();
				bw.write("\nA"+j);
				bw.write(" "+v);
			}
		}
		bw.close();
	}
	
	private static ArrayList<Update> readUpdateLoadFile() throws IOException {
		ArrayList<Update> updates = new ArrayList<Update>();
		
		BufferedReader br = new BufferedReader(new FileReader("update_load.txt"));
		String[] firstLine = br.readLine().split(" ");
		int attrNum = Integer.parseInt(firstLine[0]);
		int upNum = Integer.parseInt(firstLine[1]);
		
		for (int i = 0; i < upNum; i++) {
			Update up = new Update(br.readLine());
			for (int j = 0; j < attrNum; j++) {
				String[] attrLine = br.readLine().split(" ");
				up.addAttr(attrLine[0], Double.parseDouble(attrLine[1]));
			}
			updates.add(up);
		}
		
		br.close();
		
		return updates;
	}
	
	public static Map<Integer, Double> calculateConfidenceInterval(Map<Integer, List<Integer>> load, Map<Integer, Double> mean) {
		
		Map<Integer, Double> confidenceInterval = new TreeMap<Integer, Double>();

		for (Map.Entry<Integer, List<Integer>> e : load.entrySet()) {
			
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
	
	public static void main(String[] args) {

	/*	
		
		Random rnd = new Random();
		rnd.setSeed(0);
		
		int num_attr = 1;
		int num_mach = 100;
		int num_update_training_samples = 0;
		int num_search_training_samples = 20;
		int num_update_new_samples = 0;
		int num_search_new_samples = 20;
				
		List<PairAttributeRange> pairs = new ArrayList<PairAttributeRange>();
		pairs.add(new PairAttributeRange("A1", new Range(0.0, 1.0)));
		//pairs.add(new PairAttributeRange("A2", new Range(0.0, 1.0)));
		//pairs.add(new PairAttributeRange("A3", new Range(0.0, 1.0)));
		
		Region region = new Region("R1", pairs);		
		
		List<Region> regions = new ArrayList<Region>();
		regions.add(region);
		
		HeuristicV1 heuristic1 = new HeuristicV1(num_mach, regions);
		//HeuristicV2 heuristic2 = new HeuristicV2(num_mach, regions);
		
		// Generates update load
		
		ArrayList<Update> uplist = generateUpdateLoad(num_attr, num_update_training_samples, rnd);
		
		for (Update up : uplist) {
			
			System.out.println("Update Id: " + up.getId());
			System.out.println("Attributes: " + up.toString());
			
		}

		System.out.println("\n");
		
		// Generates search load
		
		ArrayList<Search> slist = generateSearchLoad(num_attr, num_search_training_samples, rnd);
		
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
			ArrayList<Update> newUplist = generateUpdateLoad(num_attr, num_update_new_samples, rnd);
			ArrayList<Search> newSlist = generateSearchLoad(num_attr, num_search_new_samples, rnd);
						
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
			
			int num_samples = num_update_training_samples + num_search_training_samples;
			
			bw.newLine();
			bw.write("# of Samples\tJFI");
			bw.newLine();
			bw.write(num_samples+"\t"+JFIndex);
			
		
		} catch (IOException e) { e.printStackTrace(); }
		
		finally { try { bw.close(); fw.close(); } catch (Exception e) {} }
	*/
		/* **** First Experiment: touches per region **** */
	
		Random rnd = new Random(0);
		
		int num_attr = 3;
		int num_mach = 64;
		int num_update_training_samples = 5000;
		int num_search_training_samples = 5000;
		int num_update_new_samples = 5000;
		int num_search_new_samples = 5000;
		int num_experiments = 100;
				
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
		ArrayList<Update> uplist = generateUpdateLoad(num_attr, num_update_training_samples, rnd);
		ArrayList<Search> slist  = generateSearchLoad(num_attr, num_search_training_samples, rnd);
		
		for (int h = 1; h <= 2; h++) {
			
			String fileName = "experiment1.txt";
			
			if (h==1) {
				fileName = "heuristic1.txt";
				regions = heuristic1.partition(uplist, slist);
			} else {
				fileName = "heuristic2.txt";
				regions = heuristic2.partition(uplist, slist);
			}
			
			System.out.println(heuristic1.JFI(uplist, slist, regions)+"\n");
			
			Map<Integer, List<Integer>> realLoad = new TreeMap<Integer, List<Integer>>();
			
			ArrayList<Double> JFIs = new ArrayList<Double>(num_experiments);
			
			for (int i = 1; i <= num_experiments; i++) {
				
				rnd = new Random(i);
				
				// Generates new update & search loads
				ArrayList<Update> newUplist = generateUpdateLoad(num_attr, num_update_new_samples, rnd);
				ArrayList<Search> newSlist  = generateSearchLoad(num_attr, num_search_new_samples, rnd);
				
				// Calculates JFI
				JFIs.add(heuristic1.JFI(newUplist, newSlist, regions));
	
				// Checks touches
				for (Region r : regions) {
					
					int index = regions.indexOf(r)+1;
					
					int myUpdateLoad = r.getUpdateLoad(newUplist).size();
					int mySearchLoad = r.getSearchLoad(newSlist).size();
									
					int touches = myUpdateLoad+mySearchLoad;
					
					if (!realLoad.containsKey(index)) {
						ArrayList<Integer> load = new ArrayList<Integer>(num_experiments);
						load.add(touches);
						realLoad.put(index, load);
					} else {
						realLoad.get(index).add(touches);
					}
					
				}
				
			}
			
			Map<Integer, Double> meanLoad = new TreeMap<Integer, Double>();
	
			for (Map.Entry<Integer, List<Integer>> e : realLoad.entrySet()) {
				
				List<Integer> touches = e.getValue();
				
				Integer sum = 0;
				for (Integer touch : touches) {
					sum += touch;
				}
				
				double avg = sum.doubleValue() / touches.size();
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
		//	try { generateSearchLoadFile(1, 100000, new Random()); } catch (IOException e) {}
		//	try { checkSearchLoadDistribution(); } catch (IOException e) {}
		
	}

}