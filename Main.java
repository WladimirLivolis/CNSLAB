import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;

public class Main {
	
	private static List<GUID> generateGUIDs(int qty, int AttrNum, Random rnd) {
		List<GUID> GUIDs = new ArrayList<GUID>(qty);
		for (int i = 0; i < qty; i++) {
			GUID guid = new GUID("GUID"+i);
			for (int j = 0; j < AttrNum; j++) {
				double v = rnd.nextDouble();
				guid.set_attribute("A"+j, v);
			}
			GUIDs.add(guid);
		}
		return GUIDs;
	}
	
	private static Queue<Update> generateUpdateLoad(int AttrNum, int UpNum, List<GUID> GUIDs, Random rnd) {
		Queue<Update> updates = new LinkedList<Update>();
		for (int i = 0; i < UpNum; i++) {
			GUID guid = GUIDs.get(rnd.nextInt(GUIDs.size())); //pick one of the GUIDs already created
			Update up = new Update(guid);
			for (int j = 1; j <= AttrNum; j++) {
				double v = rnd.nextDouble();
				up.addAttr("A"+j, v);
			}
			updates.add(up);
		}
		return updates;
	}
	
	private static Queue<Search> generateSearchLoad(int AttrNum, int SNum, Random rnd) {
		Queue<Search> searches = new LinkedList<Search>();
		for (int i = 0; i < SNum; i++) {
			Search s = new Search();
			for (int j = 1; j <= AttrNum; j++) {
				double v1 = rnd.nextDouble();
				double v2 = rnd.nextDouble();
				s.addPair("A"+j, new Range(v1, v2));
			}
			searches.add(s);
		}
		return searches;
	}
	
//	private static void generateSearchLoadFile(int AttrNum, int SNum, Random rnd) throws IOException {
//		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("search_load.txt")));
//		bw.write(""+AttrNum);
//		bw.write(" "+SNum);
//		for (int i = 0; i < SNum; i++) {
//			bw.write("\n/");
//			for (int j = 1; j <= AttrNum; j++) {
//				double v1 = rnd.nextDouble();
//				double v2 = rnd.nextDouble();
//				bw.write("\nA"+j);
//				bw.write(" "+v1);
//				bw.write(" "+v2);
//			}
//		}
//		bw.close();
//	}
//	
//	private static ArrayList<Search> readSearchLoadFile() throws IOException {
//		ArrayList<Search> searches = new ArrayList<Search>();
//		
//		BufferedReader br = new BufferedReader(new FileReader("search_load.txt"));
//		String[] firstLine = br.readLine().split(" ");
//		int attrNum = Integer.parseInt(firstLine[0]);
//		int SNum = Integer.parseInt(firstLine[1]);
//		
//		for (int i = 0; i < SNum; i++) {
//			br.readLine();
//			Search s = new Search();
//			for (int j = 0; j < attrNum; j++) {
//				String[] attrLine = br.readLine().split(" ");
//				s.addPair(attrLine[0], new Range(Double.parseDouble(attrLine[1]), Double.parseDouble(attrLine[2])));
//			}
//			searches.add(s);
//		}
//		
//		br.close();
//		
//		return searches;
//	}
//	
//	private static void generateUpdateLoadFile(int AttrNum, int UpNum, Random rnd) throws IOException {
//		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("update_load.txt")));
//		bw.write(""+AttrNum);
//		bw.write(" "+UpNum);
//		for (int i = 0; i < UpNum; i++) {
//			bw.write("\n/");
//			bw.write("\nGUID"+i);
//			for (int j = 1; j <= AttrNum; j++) {
//				double v = rnd.nextDouble();
//				bw.write("\nA"+j);
//				bw.write(" "+v);
//			}
//		}
//		bw.close();
//	}
//	
//	private static ArrayList<Update> readUpdateLoadFile() throws IOException {
//		ArrayList<Update> updates = new ArrayList<Update>();
//		
//		BufferedReader br = new BufferedReader(new FileReader("update_load.txt"));
//		String[] firstLine = br.readLine().split(" ");
//		int attrNum = Integer.parseInt(firstLine[0]);
//		int upNum = Integer.parseInt(firstLine[1]);
//		
//		for (int i = 0; i < upNum; i++) {
//			br.readLine();
//			Update up = new Update(br.readLine());
//			for (int j = 0; j < attrNum; j++) {
//				String[] attrLine = br.readLine().split(" ");
//				up.addAttr(attrLine[0], Double.parseDouble(attrLine[1]));
//			}
//			updates.add(up);
//		}
//		
//		br.close();
//		
//		return updates;
//	}
	
//	private static void checkSearchLoadDistribution() throws IOException {
//
//		Random rnd = new Random(100);
//		generateSearchLoadFile(1, 1000, rnd);
//		List<Search> slist = readSearchLoadFile();
//
//		BufferedWriter bw = new BufferedWriter(new FileWriter(new File("search_load_dist.txt")));
//
//		for (double d = 0.0; d <= 1.0; d += 0.01) {
//
//			int count = 0;
//			for (Search s : slist) {
//				double range_start = s.getPairs().get(0).getRange().getLow();
//				double range_end   = s.getPairs().get(0).getRange().getHigh();
//
//				if (range_start > range_end) { // [a,b] ^ a > b --> [a,1.0] ^ [0.0,b] 
//
//					if ( (d >= range_start && d <= 1.0) || (d >= 0.0 && d <= range_end) ) { count++; }
//
//				} else {
//
//					if (d >= range_start && d <= range_end) { count++; }
//
//				}
//			}
//
//			bw.write(d+",\t");
//			bw.write(count+"\n");
//
//		}
//
//		bw.close();
//
//	}
	
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
		
		Random rnd = new Random(0);
		
		int num_attr = 3;
		int num_mach = 64;
		int num_GUIDs = 100;
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
		List<GUID> GUIDs     = generateGUIDs(num_GUIDs, num_attr, rnd); 
		Queue<Update> uplist = generateUpdateLoad(num_attr, num_update_training_samples, GUIDs, rnd);
		Queue<Search> slist  = generateSearchLoad(num_attr, num_search_training_samples, rnd);
		
		for (int h = 1; h <= 2; h++) {
			
			String fileName = "experiment1.txt";
			
			if (h==1) {
				fileName = "heuristic1.txt";
				regions = heuristic1.partition(uplist, slist);
			} else {
				fileName = "heuristic2.txt";
				regions = heuristic2.partition(GUIDs, uplist, slist);
			}
			
			System.out.println(heuristic1.JFI(uplist, slist, regions)+"\n");
			
			Map<Integer, List<Integer>> realLoad = new TreeMap<Integer, List<Integer>>();
			
			ArrayList<Double> JFIs = new ArrayList<Double>(num_experiments);
			
			for (int i = 1; i <= num_experiments; i++) {
				
				rnd = new Random(i);
				
				// Generates new update & search loads
				List<GUID> newGUIDs     = generateGUIDs(num_GUIDs, num_attr, rnd); 
				Queue<Update> newUplist = generateUpdateLoad(num_attr, num_update_new_samples, newGUIDs, rnd);
				Queue<Search> newSlist  = generateSearchLoad(num_attr, num_search_new_samples, rnd);
				
				// Calculates JFI
				JFIs.add(heuristic1.JFI(newUplist, newSlist, regions));
	
				// Checks number of operations per region
				for (Region r : regions) {
					
					int index = regions.indexOf(r)+1;
					
					int myUpdateLoad = r.getUpdateLoad(newUplist).size();
					int mySearchLoad = r.getSearchLoad(newSlist).size();
									
					int totalLoad = myUpdateLoad+mySearchLoad;
					
					if (!realLoad.containsKey(index)) {
						ArrayList<Integer> load = new ArrayList<Integer>(num_experiments);
						load.add(totalLoad);
						realLoad.put(index, load);
					} else {
						realLoad.get(index).add(totalLoad);
					}
					
				}
				
			}
			
			Map<Integer, Double> meanLoad = new TreeMap<Integer, Double>();
	
			for (Map.Entry<Integer, List<Integer>> e : realLoad.entrySet()) {
				
				List<Integer> load = e.getValue();
				
				Integer sum = 0;
				for (Integer l : load) {
					sum += l;
				}
				
				double avg = sum.doubleValue() / load.size();
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