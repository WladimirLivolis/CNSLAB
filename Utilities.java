import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.TreeMap;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class Utilities {
	
	/* Returns the Jain's Fairness Index (JFI) given a list of regions and operations. *
	 * This index can be either based upon load per region or touches per region.      */
	public static double JFI(String metric, Queue<Operation> oplist, List<Region> rlist) {
		
		double JFI = -1;
		
		if (metric.toLowerCase().equals("touches")) {
			JFI = JFI_touches(oplist, rlist);
		} else if (metric.toLowerCase().equals("load")) {
			JFI = JFI_load(oplist, rlist);			
		}
		
		return JFI;
		
	}
	
	private static double JFI_load(Queue<Operation> oplist, List<Region> rlist) {
		
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
	
	private static double JFI_touches(Queue<Operation> oplist, List<Region> rlist) {
		
		long up_touches = 0, s_touches = 0, upsquare = 0, ssquare = 0;
				
		checkTouchesPerRegion(rlist, oplist);
		
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
	
	@SuppressWarnings("unchecked")
	public static void generateOperations(int updateQty, int searchQty, int attrQty, int guidMaxQty, String distribution, String fileName, Random rnd) {
		
		// Creates a JSONArray for operations
		JSONArray operations = new JSONArray();
		
		Map<Integer, Map<String, Double>> guids = new TreeMap<Integer, Map<String, Double>>();
		
		int numSearches = 0, numUpdates = 0;
				
		while ( numSearches < searchQty || numUpdates < updateQty ) {
			
			// creates JSONObject that represents the new operation
	        JSONObject newOperation = new JSONObject();
				        
			boolean update = rnd.nextBoolean() && numUpdates < updateQty;
			
			boolean search = !update && numSearches < searchQty;
			
			if (update) {
				newOperation.put("Id", "U"+(++numUpdates));
				generateUpdate(attrQty, guidMaxQty, distribution, guids, newOperation, rnd);
				operations.add(newOperation);
			} 
			
			if (search) {
				newOperation.put("Id", "S"+(++numSearches));
				generateSearch(attrQty, distribution, newOperation, rnd);
				operations.add(newOperation);
			}
			
		}
		
		// Writes JSON to file
        PrintWriter pw;
		
        try {
			
        	pw = new PrintWriter(fileName);
			
			pw.write(operations.toJSONString());
			
			pw.close();
		
        } catch (FileNotFoundException e) {
			e.printStackTrace();
		}       
		
	}
	
	@SuppressWarnings("unchecked")
	private static void generateUpdate(int attrQty, int guidMaxQty, String distribution, Map<Integer, Map<String, Double>> guids, JSONObject newOperation, Random rnd) {
				
		int guid = rnd.nextInt(guidMaxQty) + 1;

		boolean flag = guids.containsKey(guid);

		newOperation.put("GUID", guid);

		// Creates a JSONArray for Attribute-Value Pairs
		JSONArray updateAttrValPairs = new JSONArray();
		
		Map<String, Double> guidAttrValPairs = new HashMap<String, Double>(attrQty);

		for (int i = 1; i <= attrQty; i++) {

			Map<String, Object> pair = new LinkedHashMap<String, Object>(2);
			pair.put("Attribute", "A"+i+"'");
			
			// check whether there was a previous value for this attribute
			if (flag) {
				double previous_value = guids.get(guid).get("A"+i);
				pair.put("Value", previous_value);
			} else {
				pair.put("Value", -1d);
			}
			
			updateAttrValPairs.add(pair);
			
			// new value for this attribute
			double v = nextVal(distribution, rnd);
			
			pair = new LinkedHashMap<String, Object>(2);
			
			pair.put("Attribute", "A"+i);
			pair.put("Value", v);
			
			updateAttrValPairs.add(pair);
			
			guidAttrValPairs.put("A"+i, v);

		}

		newOperation.put("AttributeValuePairs", updateAttrValPairs);
		
		guids.put(guid, guidAttrValPairs);
		
	}
	
	@SuppressWarnings("unchecked")
	private static void generateSearch(int attrQty, String distribution, JSONObject newOperation, Random rnd) {
		
		// Creates a JSONArray for Attribute-Range Pairs
		JSONArray searchAttrRangePairs = new JSONArray();
		
		for (int i = 1; i <= attrQty; i++) {
			
			double v1 = nextVal(distribution, rnd);
			double v2 = nextVal(distribution, rnd);
			
			Map<String, Double> rangeMap = new LinkedHashMap<String, Double>(2);
			
			rangeMap.put("Start", v1);
			rangeMap.put("End", v2);

			Map<String, Object> pair = new LinkedHashMap<String, Object>(2);

			pair.put("Attribute", "A"+i);
			pair.put("Range", rangeMap);
			
			searchAttrRangePairs.add(pair);

		}
		
		newOperation.put("AttributeRangePairs", searchAttrRangePairs);

	}
	
	public static Queue<Operation> readOperationsFile(String fileName) {
		
		Queue<Operation> operations = new LinkedList<Operation>();
		
        try {
			
        	// Parses the JSON file
        	Object obj = new JSONParser().parse(new FileReader(fileName));
        	
        	// Gets array of operations
        	JSONArray operationsJsonArray = (JSONArray) obj;
        	
        	for (Object op : operationsJsonArray) { // iterates over operations
        		
        		// Gets operation
        		JSONObject operation = (JSONObject) op;
        		
        		// Operation Id
        		String id = (String) operation.get("Id");

        		// Checks whether this operation is a search or an update
        		if (id.charAt(0) == 'U') { // Update
        			
        			// GUID
        			long guid = (long) operation.get("GUID");
        			
        			// Creates new update
        			Update up = new Update(id, (int)guid);
        			
        			// Attribute-Value Pairs
        			JSONArray attrValPairs = (JSONArray) operation.get("AttributeValuePairs");
        			
        			for (Object pair : attrValPairs) { // iterates over attribute-value pairs
        				
        				// Gets a attribute-value pair
        				JSONObject attrValPair = (JSONObject) pair;
        				
        				// Attribute
        				String attr = (String) attrValPair.get("Attribute");
        				
        				// Value
        				double val = (double) attrValPair.get("Value");
        				
        				up.setAttr(attr, val);
        				
        			}
        			
        			operations.add(up);
        			
        		}
        		
        		if (id.charAt(0) == 'S') { // Search
        			
        			// Creates new search
        			Search s = new Search(id);
        			
        			// Attribute-Range Pairs
        			JSONArray attrValRange = (JSONArray) operation.get("AttributeRangePairs");
        			
        			for (Object pair : attrValRange) { // iterates over attribute-range pairs
        			
        				// Gets a attribute-range pair
        				JSONObject attrRangePair = (JSONObject) pair;
        				
        				// Attribute
        				String attr = (String) attrRangePair.get("Attribute");
        				
        				// Range
        				JSONObject range = (JSONObject) attrRangePair.get("Range");
        				
        				// Range Start
        				double start = (double) range.get("Start");

        				// Range End
        				double end = (double) range.get("End");
        				
        				s.setPair(attr, new Range(start, end));
        				
        			}
        			
        			operations.add(s);
        			
        		}
        		
        	}
			
        } catch (Exception e) {
			e.printStackTrace();
		}

		return operations;
		
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

					if (up_val < r_start || up_val >= r_end) { // check whether update value is inside this region range
						flag_attr = false;
						break;
					}

				}

			}

			if (flag_attr) {

				r.insertUpdateLoad(up, 1);
				break;

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

					if (s_start > s_end) { // to make the search's range uniform --> start > end: [start,1.0] ^ [0.0,end]

						if (s_start >= r_end && s_end < r_start) {

							flag = false;
							break;

						}

						if (r_end > s_start) {
							double start = s_start, end = r_end;
							if (r_start > s_start) { start = r_start; }
							weight += (end-start)/interval_size; // half-open interval: [s_start, r_end) OR [r_start, r_end)
						}
						if (r_start <= s_end) {
							double start = r_start, end = s_end;
							if (r_end <= s_end) { 
								end = r_end;
								weight += (end-start)/interval_size; // half-open interval: [r_start, r_end)
							} else {
								weight += ( (end-start)/interval_size ) + 1; // closed interval: [r_start, s_end]
							}
						}

					} else {

						if (s_start >= r_end || s_end < r_start) { // check whether both region & search ranges overlap

							flag = false;
							break;

						}

						double start = s_start, end = s_end;
						if (r_start > s_start) { start = r_start; }
						if (r_end < s_end) { 
							end = r_end;
							weight += (end-start)/interval_size; // half-open interval: [s_start, r_end) OR [r_start, r_end)
						} else {
							weight += ( (end-start)/interval_size ) + 1; // closed interval: [s_start, s_end] OR [r_start, s_end]
						}

					}

				}

			}

			if (flag) {

				r.insertSearchLoad(s, weight);

			}

		}

	}
	
	public static void checkTouchesPerRegion(List<Region> regions, Queue<Operation> oplist) {
		
		clear_regions_touches(regions);
		
		Map<Integer, Map<String, Double>> guids = new TreeMap<Integer, Map<String, Double>>();
		
		for (Operation op : oplist) {
			
			if (op instanceof Update) {	checkUpdateTouchesPerRegion(regions, guids, (Update)op); }
			
			if (op instanceof Search) {	checkSearchTouchesPerRegion(regions, guids, (Search)op); }
			
		}
		
	}
	
	private static void checkUpdateTouchesPerRegion(List<Region> regions, Map<Integer, Map<String, Double>> guids, Update up) {
		
		int guid = up.getGuid();
		
		Region previousRegion = null;
		
		// I) This first iteration over regions will look for touches due to previous GUID's positions
		for (Region region : regions) {

			boolean previouslyInRegion = true;
			int count = 0;

			// Checks whether this update's GUID is already in this region
			for (Map.Entry<String, Double> attr : up.getAttributes().entrySet()) { // iterate over this update attributes

				if (!attr.getKey().contains("'")) { continue; } // here we just look into its guid attributes, i.e., the attributes ending with '

				String guidAttrKey = attr.getKey().substring(0, attr.getKey().length()-1);    
				double guidAttrVal = attr.getValue();

				if (region.getPairs().containsKey(guidAttrKey)) { // check the region's range for this attribute
					
					double regionRangeStart = region.getPairs().get(guidAttrKey).getLow(); 
					double regionRangeEnd   = region.getPairs().get(guidAttrKey).getHigh(); 

					if (guidAttrVal < regionRangeStart || guidAttrVal >= regionRangeEnd) { // checks whether guid is in this region
						previouslyInRegion = false;
						break;
					}
				}
			}

			// If so ...
			if (previouslyInRegion) {
				
				count++; // counts a touch
				if (region.hasThisGuid(guid)) { region.removeGuid(guid); } // removes it from this region's guid list
				previousRegion = region;
				
				// Sets this region's update touch count
				int previous_count = region.getUpdateTouches();
				region.setUpdateTouches(previous_count+count);
				
				break;
				
			}

		}
		
		// II) This second iteration over regions will look for touches due to new GUID's positions
		for (Region region : regions) {

			boolean comingToRegion = true;
			int count = 0;

			// Checks whether this update moves a GUID to this region
			for (Map.Entry<String, Double> attr : up.getAttributes().entrySet()) { // iterate over this update attributes

				if (attr.getKey().contains("'")) { continue; } // here we just look into this update attributes, i.e., the attributes not ending with '

				String updateAttrKey = attr.getKey();
				double updateAttrVal = attr.getValue();

				if (region.getPairs().containsKey(updateAttrKey)) { // check the region's range for this attribute

					double regionRangeStart = region.getPairs().get(updateAttrKey).getLow(); 
					double regionRangeEnd = region.getPairs().get(updateAttrKey).getHigh();

					if (updateAttrVal < regionRangeStart || updateAttrVal >= regionRangeEnd) { // checks whether guid is coming to this region (or if it is staying in this region)
						comingToRegion = false;
						break;
					}
				}
			}

			// If so ...
			if (comingToRegion) {

				// updates this GUID's attributes with info from this update operation
				Map<String, Double> guid_attr = new HashMap<String, Double>();
				for (Map.Entry<String, Double> up_attr : up.getAttributes().entrySet()) {
					if (up_attr.getKey().contains("'")) { continue; } // ignore attributes ending with ', as it's old info
					guid_attr.put(up_attr.getKey(), up_attr.getValue());
				}
				guids.put(guid, guid_attr);

				region.insertGuid(guid); // adds it to this region's guid list
				if (!region.equals(previousRegion)) { count++; } // if it is coming from another region, counts one more touch
				
				// Sets this region's update touch count
				int previous_count = region.getUpdateTouches();
				region.setUpdateTouches(previous_count+count);
				
				break;

			}
			
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
						if (searchRangeStart >= regionRangeEnd && searchRangeEnd < regionRangeStart) { isInRegion = false; break; }
					} else {
						if (searchRangeStart >= regionRangeEnd || searchRangeEnd < regionRangeStart) { isInRegion = false; break; }
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
								if (guidAttrVal < searchRangeStart && guidAttrVal > searchRangeEnd) { flag = false; break; }
							} else {
								if (guidAttrVal < searchRangeStart || guidAttrVal > searchRangeEnd) { flag = false; break; }
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
