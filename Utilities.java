import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;

public class Utilities {
	
	/* Returns the Jain's Fairness Index (JFI) given a list of regions and operations. 
	 * This JFI is based on search & update loads per region. */
	public static double JFI(Queue<Operation> oplist, List<Region> rlist) {
		
		double upload = 0, sload = 0, upsquare = 0, ssquare = 0;
		
		checkLoadPerRegion(rlist, oplist);
		
		for (Region r : rlist) {			
			upload += r.getUpdateLoad();
			sload += r.getSearchLoad();
			upsquare += Math.pow(r.getUpdateLoad(), 2);
			ssquare += Math.pow(r.getSearchLoad(), 2);
		}
		
		double JU = 0.0, JS = 0.0;
		
		if (upload != 0)
			JU = Math.pow(upload, 2) / ( rlist.size() * upsquare );
		
		if (sload != 0)
			JS = Math.pow(sload , 2) / ( rlist.size() * ssquare  );
		
		int num_searches = 0;
		for (Operation op : oplist)
			if (op instanceof Search)
				num_searches++;
								
		double RHO = (double) num_searches / oplist.size();
						
		double JFI = ( RHO * JS ) + ( (1 - RHO) * JU );
		
		return JFI;
	}
	
	/* Returns the Jain's Fairness Index (JFI) given a list of regions and operations. 
	 * This JFI is based on search & update touches per region. */
	public static double JFI(Queue<Operation> oplist, Map<Integer, Map<String, Double>> GUIDs, List<Region> rlist) {
		
		long up_touches = 0, s_touches = 0, upsquare = 0, ssquare = 0;
		
		checkTouchesPerRegion(rlist, GUIDs, oplist);
		
		for (Region r : rlist) {			
			up_touches += r.getUpdateTouches();
			s_touches += r.getSearchTouches();
			upsquare += Math.pow(r.getUpdateTouches(), 2);
			ssquare += Math.pow(r.getSearchTouches(), 2);
		}
		
		double JU = 0.0, JS = 0.0;
		
		if (up_touches != 0)
			JU = Math.pow(up_touches, 2) / ( rlist.size() * upsquare );
		
		if (s_touches != 0)
			JS = Math.pow(s_touches , 2) / ( rlist.size() * ssquare  );
								
		int num_searches = 0;
		for (Operation op : oplist)
			if (op instanceof Search)
				num_searches++;
								
		double RHO = (double) num_searches / oplist.size();
						
		double JFI = ( RHO * JS ) + ( (1 - RHO) * JU );
		
		return JFI;
	}
	
	public static Map<Integer, Map<String, Double>> copyGUIDs(Map<Integer, Map<String, Double>> GUIDs) {
		
		Map<Integer, Map<String, Double>> copy = new TreeMap<Integer, Map<String, Double>>();
		
		for (Map.Entry<Integer, Map<String, Double>> guid : GUIDs.entrySet()) {
			
			int guidCopy = guid.getKey();
			Map<String, Double> attrCopy = new HashMap<String, Double>();
			
			for (Map.Entry<String, Double> attr : guid.getValue().entrySet()) {
				
				attrCopy.put(attr.getKey(), attr.getValue());
				
			}
			
			copy.put(guidCopy, attrCopy);
			
		}
		
		return copy;
		
	}
	
	public static List<Region> copyRegions(List<Region> regions) {
		List<Region> copy = new ArrayList<Region>(regions.size());
		for (Region r : regions)
			copy.add(new Region(r.getName(),r.getPairs()));
		return copy;
	}
	
	private static double nextExponential(double lambda, Random rnd) {
		double val;
		do {
			val = Math.log(1-rnd.nextDouble())/(-lambda);
		} while (val > 1);
		return val;
	}
	
	private static double nextGaussian(double deviation, double mean, Random rnd) {
		return rnd.nextGaussian()*deviation+mean;
	}
	
	private static double nextVal(String dist, Random rnd) {
		double val;
		switch(dist.toLowerCase()) {	
			case "normal":
			case "gaussian":
				val = nextGaussian(0.15, 0.5, rnd);
				break;
			case "exponential":
				val = nextExponential(1, rnd);
				break;
			case "uniform":
			default:
				val = rnd.nextDouble();
				break;
		}
		return val;
	}
	
	public static Queue<Update> generateUpdateLoad(int AttrNum, int UpNum, Map<Integer, Map<String, Double>> GUIDs, int numGuids, String dist, Random rnd) {
		Queue<Update> updates = new LinkedList<Update>();
		for (int i = 0; i < UpNum; i++) {
			int guid = rnd.nextInt(numGuids) + 1;
			Update up = new Update(guid);
			for (int j = 1; j <= AttrNum; j++) {
				double v = nextVal(dist, rnd);
				up.setAttr("A"+j, v);
			}
			updates.add(up);
		}
		return updates;
	}
	
	public static Queue<Search> generateSearchLoad(int AttrNum, int SNum, String dist, Random rnd) {
		Queue<Search> searches = new LinkedList<Search>();
		for (int i = 0; i < SNum; i++) {
			Search s = new Search();
			for (int j = 1; j <= AttrNum; j++) {
				double v1 = nextVal(dist, rnd);
				double v2 = nextVal(dist, rnd);
				s.setPair("A"+j, new Range(v1, v2));
			}
			searches.add(s);
		}
		return searches;
	}
	
	public static void checkLoadPerRegion(List<Region> regions, Queue<Operation> oplist) {
		
		clear_regions_load(regions);
		
		for (Operation op : oplist) {
			
			if (op instanceof Update) { checkUpdateLoadPerRegion(regions, (Update)op); }
			
			if (op instanceof Search) {	checkSearchLoadPerRegion(regions, (Search)op); }
			
		}
		
	}

	private static void checkUpdateLoadPerRegion(List<Region> regions, Update up) {

		for (Region r : regions) { // iterate over regions

			// Checks whether this update is in this region regarding its attribute

			boolean flag_attr = true;

			for (Map.Entry<String, Double> attr : up.getAttributes().entrySet()) { // iterate over this update attributes

				String up_attr = attr.getKey();   // update attribute
				double up_val  = attr.getValue(); // update value
				
				if (r.getPairs().containsKey(up_attr)) { // check the region's range for this attribute

					double r_start = r.getPairs().get(up_attr).getLow();  // region range start
					double r_end   = r.getPairs().get(up_attr).getHigh(); // region range end

					if (up_val < r_start || up_val > r_end) { // check whether update value is inside this region range
						flag_attr = false;
					}

				}

			}

			if (flag_attr) {

				r.insertUpdateLoad(up, 1);

			}

		}

	}
	
	private static void checkSearchLoadPerRegion(List<Region> regions, Search s) {
		
		double interval_size = 0.01;
		
		for (Region r : regions) { // iterate over regions

			boolean flag = true;
			
			double weight = 0;

			for (Map.Entry<String, Range> s_pair : s.getPairs().entrySet()) { // iterate over this search pairs

				String s_attr  = s_pair.getKey();             // search attribute
				double s_start = s_pair.getValue().getLow();  // search range start
				double s_end   = s_pair.getValue().getHigh(); // search range end
				
				if (r.getPairs().containsKey(s_attr)) { // check the region's range for this attribute 

					double r_start = r.getPairs().get(s_attr).getLow();  // region range start
					double r_end   = r.getPairs().get(s_attr).getHigh(); // region range end 

					if (s_start > s_end) { // trata o caso de buscas uniformes (circular) --> start > end: [start,1.0] ^ [0.0,end]

						if (s_start > r_end && s_end < r_start) {

							flag = false;

						}

						if (r_end > s_start) {
							double start = s_start, end = r_end;
							if (r_start > s_start) { start = r_start; }
							weight += (end-start)/interval_size;
						}
						if (r_start < s_end) {
							double start = r_start, end = s_end;
							if (r_end < s_end) { end = r_end; }
							weight += (end-start)/interval_size;
						}

					} else {

						if (s_start > r_end || s_end < r_start) { // check whether both region & search ranges overlap

							flag = false;

						}

						double start = s_start, end = s_end;
						if (r_start > s_start) { start = r_start; }
						if (r_end < s_end) { end = r_end; }
						weight += (end-start)/interval_size;

					}

				}

			}

			if (flag) {

				r.insertSearchLoad(s, weight);

			}

		}

	}
	
	public static Queue<Operation> sortOperations(Queue<Search> slist, Queue<Update> uplist, Random rnd) {
		
		Queue<Operation> operations = new LinkedList<Operation>();
		
		while (!slist.isEmpty() || !uplist.isEmpty()) {

			boolean flag = rnd.nextBoolean();

			Operation op = null;

			if (flag) {
				op = uplist.poll();
			} else {
				op = slist.poll();
			}
			
			if (op != null) {
				operations.add(op);
			}

		}
		
		return operations;
	}
	
	public static void checkTouchesPerRegion(List<Region> regions, Map<Integer, Map<String, Double>> guids, Queue<Operation> oplist) {
		
		clear_regions_touches(regions);
		
		for (Operation op : oplist) {
			
			if (op instanceof Update) {	checkUpdateTouchesPerRegion(regions, guids, (Update)op); }
			
			if (op instanceof Search) {	checkSearchTouchesPerRegion(regions, guids, (Search)op); }
			
		}
		
	}
	
	private static void checkUpdateTouchesPerRegion(List<Region> regions, Map<Integer, Map<String, Double>> guids, Update up) {
		
		int guid = up.getGuid();
		
		Region previousRegion = null;
		
		if (guids.containsKey(guid)) { // check whether there was a previous position
		
			// I) This first iteration over regions will look for touches due to previous GUID's positions
			for (Region region : regions) {
	
				boolean previouslyInRegion = true;
				int count = 0;
	
				// Checks whether this update's GUID is already in this region
				for (Map.Entry<String, Double> attr : guids.get(guid).entrySet()) { // iterate over guid attributes
	
					String guidAttrKey = attr.getKey();    
					double guidAttrVal = attr.getValue();
					
					if (region.getPairs().containsKey(guidAttrKey)) { // check the region's range for this attribute
	
						double regionRangeStart = region.getPairs().get(guidAttrKey).getLow(); 
						double regionRangeEnd   = region.getPairs().get(guidAttrKey).getHigh(); 
	
						if (guidAttrVal < regionRangeStart || guidAttrVal > regionRangeEnd) { // checks whether guid is in this region
							previouslyInRegion = false;
						}
					}
				}
	
				// If so ...
				if (previouslyInRegion) {
					count++; // counts a touch
					if (region.hasThisGuid(guid)) { region.removeGuid(guid); } // removes it from this region's guid list
					previousRegion = region;
				}
				
				// Sets this region's update touch count
				int previous_count = region.getUpdateTouches();
				region.setUpdateTouches(previous_count+count);
	
			}
		
		}
		
		// II) This second iteration over regions will look for touches due to new GUID's positions
		for (Region region : regions) {
			
			boolean comingToRegion = true;
			int count = 0;

			// Checks whether this update moves a GUID to this region
			for (Map.Entry<String, Double> attr : up.getAttributes().entrySet()) { // iterate over this update attributes

				String updateAttrKey = attr.getKey();
				double updateAttrVal = attr.getValue();
				
				if (region.getPairs().containsKey(updateAttrKey)) { // check the region's range for this attribute

					double regionRangeStart = region.getPairs().get(updateAttrKey).getLow(); 
					double regionRangeEnd = region.getPairs().get(updateAttrKey).getHigh();

					if (updateAttrVal < regionRangeStart || updateAttrVal > regionRangeEnd) { // checks whether guid is coming to this region (or if it is staying in this region)
						comingToRegion = false;
					}
				}
			}

			// If so ...
			if (comingToRegion) {

				// updates its attributes with info from this update operation
				Map<String, Double> guid_attr = new HashMap<String, Double>();
				guids.put(guid, guid_attr);
				for (Map.Entry<String, Double> up_attr : up.getAttributes().entrySet()) {
					guid_attr.put(up_attr.getKey(), up_attr.getValue());
				}

				region.insertGuid(guid); // adds it to this region's guid list
				if (!region.equals(previousRegion)) { count++; } // if it is coming from another region, counts one more touch

			}
			
			// Sets this region's update touch count
			int previous_count = region.getUpdateTouches();
			region.setUpdateTouches(previous_count+count);
			
		}
	}
	
	private static void checkSearchTouchesPerRegion(List<Region> regions, Map<Integer, Map<String, Double>> guids, Search s) {

		for (Region region : regions) { // iterate over regions

			boolean isInRegion = true;
			int count = 0;

			// I) Checks whether this search is in this region

			for (Map.Entry<String, Range> searchPair : s.getPairs().entrySet()) { // iterate over this search's attributes

				String searchAttrKey    = searchPair.getKey();
				double searchRangeStart = searchPair.getValue().getLow();
				double searchRangeEnd   = searchPair.getValue().getHigh();

				if (region.getPairs().containsKey(searchAttrKey)) { // check the region's range for this attribute
				
					double regionRangeStart = region.getPairs().get(searchAttrKey).getLow();
					double regionRangeEnd   = region.getPairs().get(searchAttrKey).getHigh();

					if (searchRangeStart > searchRangeEnd) {
						if (searchRangeStart > regionRangeEnd && searchRangeEnd < regionRangeStart) { isInRegion = false; }
					} else {
						if (searchRangeStart > regionRangeEnd || searchRangeEnd < regionRangeStart) { isInRegion = false; }
					}
				}
			}

			// II) If so, counts the number of guids already in this region that meet this search's requirements

			if (isInRegion) {

				for (int guid : region.getGUIDs()) { // iterate over this region's guids

					boolean flag = true;

					for (Map.Entry<String, Double> attr : guids.get(guid).entrySet()) { // iterate over this guid's attributes

						String guidAttrKey = attr.getKey();    
						double guidAttrVal = attr.getValue();

						if (s.getPairs().containsKey(guidAttrKey)) { // check the region's range for this attribute
						
							double searchRangeStart = s.getPairs().get(guidAttrKey).getLow();
							double searchRangeEnd   = s.getPairs().get(guidAttrKey).getHigh();

							if (searchRangeStart > searchRangeEnd) {
								if (guidAttrVal < searchRangeStart && guidAttrVal > searchRangeEnd) { flag = false; }
							} else {
								if (guidAttrVal < searchRangeStart || guidAttrVal > searchRangeEnd) { flag = false; }
							}
						}

					}

					if (flag) { count++; }

				}

			}
			
			// Sets this region's search touch counter
			int previous_count = region.getSearchTouches();
			region.setSearchTouches(previous_count+count);

		}
	}
			
//	private static void distributeGUIDsAmongRegions(List<Region> regions, List<GUID> guids) {
//		
//		for (GUID guid : guids) { // iterate over GUIDs
//			
//			for (Region region : regions) { // iterate over regions
//				
//				boolean isInRegion = true;
//				
//				for (Map.Entry<String, Double> attr : guid.getAttributes().entrySet()) { // iterate over this guid's attributes
//					
//					String guidAttrKey = attr.getKey();    
//					double guidAttrVal = attr.getValue();
//					
//					if (region.getPairs().containsKey(guidAttrKey)) { // check the region's range for this attribute
//					
//						double regionRangeStart = region.getPairs().get(guidAttrKey).getLow(); 
//						double regionRangeEnd = region.getPairs().get(guidAttrKey).getHigh();
//						
//						if (guidAttrVal < regionRangeStart || guidAttrVal > regionRangeEnd) { // checks whether guid is in this region
//
//							isInRegion = false;
//
//						}
//							
//					}
//					
//				}
//				
//				//If so ...
//				if (isInRegion) { 
//					region.insertGuid(guid); // adds it to this region's guids list
//				}
//				
//			}
//			
//		}
//		
//	}
	
	private static void clear_regions_load(List<Region> regions) {
		
		for (Region r : regions) {
			r.clearSearchLoad();
			r.clearUpdateLoad();
		}
		
	}
	
	private static void clear_regions_touches(List<Region> regions) {
		
		for (Region r : regions) {
			r.clearGUIDs();
			r.setSearchTouches(0);
			r.setUpdateTouches(0);
		}
		
	}

}
