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

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class Utilities {
	
	/* Returns the Jain's Fairness Index (JFI) given a list of regions and operations. *
	 * This index can be either based upon load per region or touches per region.      */
	public static double JFI(String metric, Queue<Operation> oplist, List<Region> rlist) {
		
		double JFI = -1;
		
		if (metric.toLowerCase().equals("touches")) {
			JFI = JFI_touches(oplist, rlist);
		} else if (metric.toLowerCase().equals("load")) {
			JFI = JFI_load(oplist, rlist);			
		} else if (metric.toLowerCase().equals("guids")) {
			JFI = JFI_guids(oplist, rlist);
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
	
	private static double JFI_guids(Queue<Operation> oplist, List<Region> rlist) {
		
		long guids = 0, guids_square = 0;
				
		checkTouchesPerRegion(rlist, oplist);
		
		for (Region r : rlist) {
			guids += r.getGUIDs().size();
			guids_square += Math.pow(r.getGUIDs().size(), 2);			
		}

		double JFI = Math.pow(guids, 2) / ( rlist.size() * guids_square );
		
		return JFI;
	}
	
	public static Map<String, Double> JFI_touches_and_guids(Queue<Operation> oplist, List<Region> rlist) {
		
		Map<String, Double> output = new HashMap<String, Double>(2);
		
		long up_touches = 0, s_touches = 0, upsquare = 0, ssquare = 0, guids = 0, guids_square = 0;
		
		checkTouchesPerRegion(rlist, oplist);
		
		for (Region r : rlist) {			
			up_touches += r.getUpdateTouches();
			s_touches += r.getSearchTouches();
			guids += r.getGUIDs().size();
			upsquare += Math.pow(r.getUpdateTouches(), 2);
			ssquare += Math.pow(r.getSearchTouches(), 2);
			guids_square += Math.pow(r.getGUIDs().size(), 2);			
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
						
		double JFI_touches = ( RHO * JS ) + ( (1 - RHO) * JU );
		
		double JFI_guids = Math.pow(guids, 2) / ( rlist.size() * guids_square );
		
		output.put("touches", JFI_touches);
		output.put("guids", JFI_guids);
		
		return output;
		
	}
	
	public static Map<String, Double> JFI(Queue<Operation> oplist, List<Machine> mlist) {
		
		Map<String, Double> output = new HashMap<String, Double>(2);
		
		long up_touches = 0, s_touches = 0, upsquare = 0, ssquare = 0, guids = 0, guids_square = 0;
		
		checkTouchesPerMachine(mlist, oplist);
		
		for (Machine m : mlist) {
			up_touches += m.getUpdateTouches();
			s_touches += m.getSearchTouches();
			guids += m.getGUIDs().size();
			upsquare += Math.pow(m.getUpdateTouches(), 2);
			ssquare += Math.pow(m.getSearchTouches(), 2);
			guids_square += Math.pow(m.getGUIDs().size(), 2);			
		}
		
		double JU = 0.0, JS = 0.0;
		
		if (up_touches != 0)
			JU = Math.pow(up_touches, 2) / ( mlist.size() * upsquare );
		
		if (s_touches != 0)
			JS = Math.pow(s_touches , 2) / ( mlist.size() * ssquare  );
								
		int num_searches = 0;
		for (Operation op : oplist)
			if (op instanceof Search)
				num_searches++;
								
		double RHO = (double) num_searches / oplist.size();
						
		double JFI_touches = ( RHO * JS ) + ( (1 - RHO) * JU );
		
		double JFI_guids = Math.pow(guids, 2) / ( mlist.size() * guids_square );
		
		output.put("touches", JFI_touches);
		output.put("guids", JFI_guids);
	
		return output;
	}
	
	public static Map<String, Double> JFI_subspaces(Queue<Operation> oplist, List<List<Region>> subspaces) {
		
		Map<String, Double> output = new HashMap<String, Double>(2);
		
		long up_touches = 0, s_touches = 0, upsquare = 0, ssquare = 0, guids = 0, guids_square = 0;
		
		checkTouchesPerSubspace(subspaces, oplist);
		
		int count = 0;
		for (List<Region> rlist : subspaces) {
			for (Region r : rlist) {			
				up_touches += r.getUpdateTouches();
				s_touches += r.getSearchTouches();
				guids += r.getGUIDs().size();
				upsquare += Math.pow(r.getUpdateTouches(), 2);
				ssquare += Math.pow(r.getSearchTouches(), 2);
				guids_square += Math.pow(r.getGUIDs().size(), 2);
				count++;
			}
		}
		
		double JU = 0.0, JS = 0.0;
		
		if (up_touches != 0)
			JU = Math.pow(up_touches, 2) / ( count * upsquare );
		
		if (s_touches != 0)
			JS = Math.pow(s_touches , 2) / ( count * ssquare  );
								
		int num_searches = 0;
		for (Operation op : oplist)
			if (op instanceof Search)
				num_searches++;
								
		double RHO = (double) num_searches / oplist.size();
						
		double JFI_touches = ( RHO * JS ) + ( (1 - RHO) * JU );
		
		double JFI_guids = Math.pow(guids, 2) / ( count * guids_square );
		
		output.put("touches", JFI_touches);
		output.put("guids", JFI_guids);
		
		return output;
		
	}
	
	public static List<Region> copyRegions(List<Region> regions) {
		List<Region> copy = new ArrayList<Region>(regions.size());
		for (Region r : regions) {
			copy.add(new Region(r.getName(), r.getPairs()));
		}
		return copy;
	}
	
	public static List<Region> copyRegionsWithGUIDs(List<Region> regions) {
		List<Region> copy = new ArrayList<Region>(regions.size());
		for (Region r : regions) {
			Region new_region = new Region(r.getName(), r.getPairs());
			for (Map.Entry<Integer, Map<String, Double>> guid : r.getGUIDs().entrySet()) {
				new_region.insertGuid(guid.getKey(), guid.getValue());
			}
			copy.add(new_region);
		}
		return copy;
	}
	
	public static void copyRegionsRanges(List<Region> regionsWithRangesToBeSet, List<Region> regionsWithRangesToBeCopied) {
		for (Region region : regionsWithRangesToBeSet) {
			int regionIndex = regionsWithRangesToBeSet.indexOf(region);
			for (Map.Entry<String, Range> pair : regionsWithRangesToBeCopied.get(regionIndex).getPairs().entrySet()) {
				region.setPair(pair.getKey(), pair.getValue().getLow(), pair.getValue().getHigh());
			}
		}
	}
	
	public static List<Region> buildNewRegions(int num_attr) {
		
		Map<String, Range> pairs = new HashMap<String, Range>(); // pairs attribute-range
		
		for (int i = 1; i <= num_attr; i++) {
			pairs.put("A"+i, new Range(0.0, 1.0));
		}
		
		Region region = new Region("R1", pairs);	
		
		List<Region> regions = new ArrayList<Region>();
		regions.add(region);
		
		return regions;
	}
	
	public static List<Region> buildNewRegions(int num_attr, int first_attr, int last_attr) {
		
		Map<String, Range> pairs = new HashMap<String, Range>(); // pairs attribute-range
		
		for (int i = first_attr; i <= last_attr; i++) {
			pairs.put("A"+i, new Range(0.0, 1.0));
		}
		
		Region region = new Region("R1", pairs);	
		
		List<Region> regions = new ArrayList<Region>();
		regions.add(region);
		
		return regions;
	}
	
	public static String printRegions(List<Region> regions) {
		StringBuilder str = new StringBuilder("{ ");
		for (Region region : regions) {
			str.append(region.toString());
			str.append(" ");
		}
		str.append("}");
		return str.toString();
	}
	
	private static double nextUniform(double low, double high, Random rnd) {
		double val;
		if (low > high) {
			do {
				val = rnd.nextDouble();
			} while(val > high && val < low);
		} else {
			val = low + (high - low) * rnd.nextDouble();
		}
		return val;
	}
	
	private static double nextExponential(double lambda, Random rnd) {
		double val;
		do {
			val = Math.log(1-rnd.nextDouble())/(-lambda);
		} while (val < 0 || val > 1);
		return val;
	}
	
	private static double nextGaussian(double deviation, double mean, Random rnd) {
		double val;
		do {
			val = rnd.nextGaussian()*deviation+mean;
		} while (val < 0 || val > 1);
		return val;
	}
	
	private static double nextVal(String dist, Map<String, Double> distParams, Random rnd) {
		double val;
		switch(dist.toLowerCase()) {	
			case "normal":
			case "gaussian":
				double deviation = distParams.get("deviation");
				double mean = distParams.get("mean");
				val = nextGaussian(deviation, mean, rnd);
				break;
			case "exponential":
				double lambda = distParams.get("lambda");
				val = nextExponential(lambda, rnd);
				break;
			case "uniform":
			default:
				double low = distParams.get("low");
				double high = distParams.get("high");
				val = nextUniform(low, high, rnd);
				break;
		}
		return val;
	}
	
	@SuppressWarnings("unchecked")
	public static void generateOperations(int updateQty, int searchQty, int attrQty, int guidMaxQty, Map<Integer, Map<String, Double>> guids, Map<String, Map<Integer, String>> distribution, Map<String, Map<Integer, Map<String, Double>>> distParams, String fileName, Random rnd) {
		
		// Creates a JSONArray for operations
		JSONArray operations = new JSONArray();
		
		int numSearches = 0, numUpdates = 0;
				
		while ( numSearches < searchQty || numUpdates < updateQty ) {
			
			// creates JSONObject that represents the new operation
	        JSONObject newOperation = new JSONObject();
				        
			boolean update = rnd.nextBoolean() && numUpdates < updateQty;
			
			boolean search = !update && numSearches < searchQty;
			
			if (update) {
				newOperation.put("Id", "U"+(++numUpdates));
				generateUpdate(attrQty, guidMaxQty, distribution.get("update"), distParams.get("update"), guids, newOperation, rnd);
				operations.add(newOperation);
			} 
			
			if (search) {
				newOperation.put("Id", "S"+(++numSearches));
				generateSearch(attrQty, distribution.get("search"), distParams.get("search"), newOperation, rnd);
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
	private static void generateUpdate(int attrQty, int guidMaxQty, Map<Integer, String> distribution, Map<Integer, Map<String, Double>> distParams, Map<Integer, Map<String, Double>> guids, JSONObject newOperation, Random rnd) {
				
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
			double v = nextVal(distribution.get(i), distParams.get(i), rnd);
			
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
	private static void generateSearch(int attrQty, Map<Integer, String> distribution, Map<Integer, Map<String, Double>> distParams, JSONObject newOperation, Random rnd) {
		
		// Creates a JSONArray for Attribute-Range Pairs
		JSONArray searchAttrRangePairs = new JSONArray();
		
		// minimum and maximum number of attributes a search can specify
		int min_num_attr = 3, max_num_attr = attrQty;
		
		// number of attributes this search will specify
		int num_attr = rnd.nextInt((max_num_attr - min_num_attr) + 1) + min_num_attr;
		
		int set_attr = 0;
		
		ArrayList<Integer> set_attrs = new ArrayList<Integer>();
		
		while (set_attr < num_attr) {
			
			int attr;
			do { attr = rnd.nextInt(attrQty) + 1; } while (set_attrs.contains(attr)); // make sure it picks always a new attribute
			
			double v1 = nextVal(distribution.get(attr), distParams.get(attr), rnd);
			double v2 = nextVal(distribution.get(attr), distParams.get(attr), rnd);
			
			Map<String, Double> rangeMap = new LinkedHashMap<String, Double>(2);

			rangeMap.put("Start", v1);
			rangeMap.put("End", v2);
			
			Map<String, Object> pair = new LinkedHashMap<String, Object>(2);

			pair.put("Attribute", "A"+attr);
			pair.put("Range", rangeMap);
			
			searchAttrRangePairs.add(pair);
			
			set_attrs.add(attr);
			set_attr++;
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
	
	/* For each type operation and for each attribute it picks a distribution and sets its parameters. */
	public static void generateRandomDistribution(int num_attr, ArrayList<String> possibleDistributions, Map<String, Map<Integer, String>> distribution, Map<String, Map<Integer, Map<String, Double>>> distParams, Random rnd) {
		
		_generateRandomDistribution("update", num_attr, possibleDistributions, distribution, distParams, rnd);
		_generateRandomDistribution("search", num_attr, possibleDistributions, distribution, distParams, rnd);
		
	}
	
	private static void _generateRandomDistribution(String operation, int num_attr, ArrayList<String> possibleDistributions, Map<String, Map<Integer, String>> distribution, Map<String, Map<Integer, Map<String, Double>>> distParams, Random rnd) {
		
		Map<Integer, String> operationDist = new TreeMap<Integer, String>();
		Map<Integer, Map<String, Double>> operationDistParams = new TreeMap<Integer, Map<String, Double>>();
		for (int i = 1; i <= num_attr; i++) {
			String dist = possibleDistributions.get(rnd.nextInt(possibleDistributions.size())).toLowerCase();
			operationDist.put(i, dist);
			Map<String, Double> attrDistParams = new HashMap<String, Double>();
			if (dist.equals("uniform")) {
				attrDistParams.put("low", rnd.nextDouble());
				attrDistParams.put("high", rnd.nextDouble());
			} else if (dist.equals("normal") || dist.equals("gaussian")) {
				double deviation;
				do { deviation = rnd.nextDouble(); } while (deviation == 0);
				attrDistParams.put("deviation", deviation);
				attrDistParams.put("mean", rnd.nextDouble());
			} else if (dist.equals("exponential")) {
				double lambda;
				do { lambda = 2 * rnd.nextDouble(); } while (lambda == 0);
				attrDistParams.put("lambda", lambda);
			}
			operationDistParams.put(i, attrDistParams);
		}
		distribution.put(operation, operationDist);
		distParams.put(operation, operationDistParams);
		
	}
	
	public static Map<String, Map<Integer, String>> pickRandomDistribution(int num_attr, ArrayList<String> possibleDistributions, Random rnd) {
		
		Map<String, Map<Integer, String>> distribution = new HashMap<String, Map<Integer, String>>();
		
		String[] operations = {"update", "search"};
		
		for (String operation : operations) {
			
			Map<Integer, String> operationDist = new TreeMap<Integer, String>();
			
			for (int i = 1; i <= num_attr; i++) {
				
				String dist = possibleDistributions.get(rnd.nextInt(possibleDistributions.size())).toLowerCase();
				operationDist.put(i, dist);
				
			}
			distribution.put(operation, operationDist);
			
		}
		
		return distribution;
		
	}
	
	/* Given the distribution for each type of operation and for each attribute it randomly sets its parameters */
	public static void generateRandomDistribution(int num_attr, Map<String, Map<Integer, String>> distribution, Map<String, Map<Integer, Map<String, Double>>> distParams, Random rnd) {
		
		_generateRandomDistribution("update", num_attr, distribution, distParams, rnd);
		_generateRandomDistribution("search", num_attr, distribution, distParams, rnd);
		
	}
	
	private static void _generateRandomDistribution(String operation, int num_attr, Map<String, Map<Integer, String>> distribution, Map<String, Map<Integer, Map<String, Double>>> distParams, Random rnd) {
		
		Map<Integer, Map<String, Double>> operationDistParams = new TreeMap<Integer, Map<String, Double>>();
		for (int i = 1; i <= num_attr; i++) {
			String dist = distribution.get(operation).get(i);
			Map<String, Double> attrDistParams = new HashMap<String, Double>();
			if (dist.equals("uniform")) {
				attrDistParams.put("low", rnd.nextDouble());
				attrDistParams.put("high", rnd.nextDouble());
			} else if (dist.equals("normal") || dist.equals("gaussian")) {
				double deviation;
				do { deviation = rnd.nextDouble(); } while (deviation == 0);
				attrDistParams.put("deviation", deviation);
				attrDistParams.put("mean", rnd.nextDouble());
			} else if (dist.equals("exponential")) {
				double lambda;
				do { lambda = 2 * rnd.nextDouble(); } while (lambda == 0);
				attrDistParams.put("lambda", lambda);
			}
			operationDistParams.put(i, attrDistParams);
		}
		distParams.put(operation, operationDistParams);
		
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
		
		for (Operation op : oplist) {
			
			if (op instanceof Update) {	checkUpdateTouchesPerRegion(regions, (Update)op); }
			
			if (op instanceof Search) {	checkSearchTouchesPerRegion(regions, (Search)op); }
			
		}
		
	}
	
	private static void checkUpdateTouchesPerRegion(List<Region> regions, Update up) {
		
		int guid = up.getGuid();
		
		Region previousRegion = null;
		
		// I) This first iteration over regions will look for touches due to previous GUID's positions
		for (Region region : regions) {

			boolean previouslyInRegion = region.hasThisGuid(guid);

			// If guid is in this region
			if (previouslyInRegion) {
				
				region.removeGuid(guid); // removes it from this region's guid list 
				
				previousRegion = region;
				
				// Counts one touch
				int previous_count = region.getUpdateTouches();
				region.setUpdateTouches(previous_count+1);
				
				break;
				
			}

		}
		
		// II) This second iteration over regions will look for touches due to new GUID's positions
		for (Region region : regions) {

			boolean comingToRegion = true;

			// Checks whether this update moves a GUID to this region
			for (Map.Entry<String, Double> attr : up.getAttributes().entrySet()) { // iterate over this update attributes

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
				region.insertGuid(guid, guid_attr); // adds it to this region's guid list

				// if it is coming from another region, counts one more touch
				if (!region.equals(previousRegion)) {
					int previous_count = region.getUpdateTouches();
					region.setUpdateTouches(previous_count+1);
				}
				
				break;

			}
			
		}
	}
	
	private static void checkSearchTouchesPerRegion(List<Region> regions, Search s) {

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

				for (int guid : region.getGUIDs().keySet()) { // iterate over this region's guids

					boolean flag = true;

					for (Map.Entry<String, Double> attr : region.getGUIDs().get(guid).entrySet()) { // iterate over this guid's attributes

						String guidAttrKey = attr.getKey();    
						double guidAttrVal = attr.getValue();

						if (s.getPairs().containsKey(guidAttrKey)) { // check the search range for this attribute
						
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
			r.setSearchTouches(0);
			r.setUpdateTouches(0);
		}
		
	}
	
	public static void checkTouchesPerSubspace(List<List<Region>> subspaces, Queue<Operation> oplist) {
		
		clear_subspaces_touches(subspaces);
		
		for (Operation op : oplist) {
			
			if (op instanceof Update) {	checkUpdateTouchesPerSubspace(subspaces, (Update)op); }
			
			if (op instanceof Search) {	checkSearchTouchesPerSubspace(subspaces, (Search)op); }
			
		}
		
	}
	
	private static void checkUpdateTouchesPerSubspace(List<List<Region>> subspaces, Update up) {
		
		int guid = up.getGuid();
		
		for (List<Region> regions : subspaces) { // We iterate over all hyperdex subspaces

			Region previousRegion = null;

			// I) This first iteration over regions will look for touches due to previous GUID's positions
			for (Region region : regions) {

				boolean previouslyInRegion = region.hasThisGuid(guid);

				// If guid is in this region
				if (previouslyInRegion) {

					region.removeGuid(guid); // removes it from this region's guid list 

					previousRegion = region;

					// Counts one touch
					int previous_count = region.getUpdateTouches();
					region.setUpdateTouches(previous_count+1);

					break;

				}

			}

			// II) This second iteration over regions will look for touches due to new GUID's positions
			for (Region region : regions) {

				boolean comingToRegion = true;

				// Checks whether this update moves a GUID to this region
				for (Map.Entry<String, Double> attr : up.getAttributes().entrySet()) { // iterate over this update attributes

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
					region.insertGuid(guid, guid_attr); // adds it to this region's guid list

					// if it is coming from another region, counts one more touch
					if (!region.equals(previousRegion)) {
						int previous_count = region.getUpdateTouches();
						region.setUpdateTouches(previous_count+1);
					}

					break;

				}

			}
			
		} // subspaces
		
	}
	
	private static void checkSearchTouchesPerSubspace(List<List<Region>> subspaces, Search s) {
		
		// We pick a random subspace
		List<Region> regions = subspaces.get((new Random().nextInt(subspaces.size())));
		
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

				for (int guid : region.getGUIDs().keySet()) { // iterate over this region's guids

					boolean flag = true;

					for (Map.Entry<String, Double> attr : region.getGUIDs().get(guid).entrySet()) { // iterate over this guid's attributes

						String guidAttrKey = attr.getKey();    
						double guidAttrVal = attr.getValue();

						if (s.getPairs().containsKey(guidAttrKey)) { // check the search range for this attribute
						
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
	
	private static void clear_subspaces_touches(List<List<Region>> subspaces) {
		
		for (List<Region> regions : subspaces) {
			for (Region r : regions) {
				r.setSearchTouches(0);
				r.setUpdateTouches(0);
			}
		}
		
	}
	
	public static void checkTouchesPerMachine(List<Machine> machines, Queue<Operation> oplist) {
		
		clear_machines_touches(machines);
		
		for (Operation op : oplist) {
			
			if (op instanceof Update) {	checkUpdateTouchesPerMachine(machines, (Update)op); }
			
			if (op instanceof Search) {	checkSearchTouchesPerMachine(machines, (Search)op); }
			
		}
		
	}
	
	private static void checkUpdateTouchesPerMachine(List<Machine> machines, Update up) {
		
		int guid = up.getGuid();
		
		// uses consistent hashing to find the machine associated with this guid
		HashFunction hf = Hashing.sha256();
		HashCode hc = hf.newHasher().putInt(guid).hash();
		int machine_index = Hashing.consistentHash(hc, machines.size());
		Machine machine = machines.get(machine_index);
		
		// updates this GUID's attributes with info from this update operation
		Map<String, Double> guid_attr = new HashMap<String, Double>();
		for (Map.Entry<String, Double> up_attr : up.getAttributes().entrySet()) {
			if (up_attr.getKey().contains("'")) { continue; } // ignore attributes ending with ', as it's old info
			guid_attr.put(up_attr.getKey(), up_attr.getValue());
		}
		machine.insertGuid(guid, guid_attr); // adds it to this machines's guid list
		
		// counts one touch
		int previous_count = machine.getUpdateTouches();
		machine.setUpdateTouches(previous_count+1);
		
	}
	
	private static void checkSearchTouchesPerMachine(List<Machine> machines, Search s) {
		
		for (Machine m : machines) { // iterate over all machines
			
			int count = 0;
			
			for (Map.Entry<Integer, Map<String, Double>> guid : m.getGUIDs().entrySet()) { // iterate over this machine's guids
				
				boolean flag = true;

				for (Map.Entry<String, Double> attr : guid.getValue().entrySet()) { // iterate over this guid's attributes

					String guidAttrKey = attr.getKey();    
					double guidAttrVal = attr.getValue();

					if (s.getPairs().containsKey(guidAttrKey)) { // check the search range for this attribute
					
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
			
			// Sets this machine's search touch counter
			int previous_count = m.getSearchTouches();
			m.setSearchTouches(previous_count+count);
			
		}
		
	}
	
	private static void clear_machines_touches(List<Machine> machines) {
		
		for (Machine m : machines) {
			m.setSearchTouches(0);
			m.setUpdateTouches(0);
		}
		
	}
	
	
	/* Computes and returns the number of exchange messages between the controller and each machine when adopting replicate-at-all strategy */
	public static Map<Integer, Integer> messagesCounterReplicateAtAll(int num_machines, Queue<Operation> oplist) {

		Map<Integer, Integer> messagesCounterPerMachine = new TreeMap<Integer, Integer>();

		// initializes messages counter per machine
		for (int i = 1; i <= num_machines; i++) {
			messagesCounterPerMachine.put(i, 0);
		}

		for (Operation op : oplist) {

			if (op instanceof Update) { // controller sends a message to every machine

				for (Map.Entry<Integer, Integer> e : messagesCounterPerMachine.entrySet()) {

					messagesCounterPerMachine.put(e.getKey(), e.getValue()+1);

				}

			} else if (op instanceof Search) { // controller sends a message to one machine

				int max = num_machines, min = 1;
				int machine = (new Random()).nextInt((max - min) + 1) + min;

				messagesCounterPerMachine.put(machine, messagesCounterPerMachine.get(machine)+1);

			}
			
		}
		
		return messagesCounterPerMachine;

	}
	
	/* Computes and returns the number of exchange messages between the controller and each machine when adopting query-all strategy */
	public static Map<Integer, Integer> messagesCounterQueryAll(int num_machines, String axis, Queue<Operation> oplist) {

		Map<Integer, Integer> messagesCounterPerMachine = new TreeMap<Integer, Integer>();

		// initializes messages counter per machine
		for (int i = 1; i <= num_machines; i++) {
			messagesCounterPerMachine.put(i, 0);
		}

		for (Operation op : oplist) {

			if (op instanceof Update) { // controller sends a message to one specific machine

				Update up = (Update)op;
				int guid = up.getGuid();
				
				// uses consistent hashing to find the machine associated with this guid
				HashFunction hf = Hashing.sha256();
				HashCode hc = hf.newHasher().putInt(guid).hash();
				int machine_index = Hashing.consistentHash(hc, num_machines);
				
				messagesCounterPerMachine.put(machine_index+1, messagesCounterPerMachine.get(machine_index+1)+1);

			} else if (op instanceof Search) { // controller sends messages to all machines
				
				for (Map.Entry<Integer, Integer> e : messagesCounterPerMachine.entrySet()) {

					messagesCounterPerMachine.put(e.getKey(), e.getValue()+1);

				}

			}

		}

		return messagesCounterPerMachine;

	}
	
	/* Computes and returns the number of exchange messages between the controller and each machine when adopting hyperspace strategy */
	public static Map<Integer, Integer> messagesCounterHyperspace(int num_machines, Queue<Operation> oplist, List<Region> regions) {

		Map<Integer, Integer> messagesCounterPerMachine = new TreeMap<Integer, Integer>();

		// initializes messages counter per machine
		for (int i = 1; i <= num_machines; i++) {
			messagesCounterPerMachine.put(i, 0);
		}

		for (Operation op : oplist) {

			if (op instanceof Update) { 

				Update up = (Update)op;

				for (Region r : regions) { // iterate over regions

					// Checks whether this update is in this region regarding its attribute or its guid
					boolean flag_attr = true, flag_guid = true;
										
					for (Map.Entry<String, Double> attr : up.getAttributes().entrySet()) { // iterate over this update attributes
						
						boolean isGUID = attr.getKey().contains("'");
						
						String attrKey;
						if (isGUID) {
							attrKey = attr.getKey().substring(0, attr.getKey().length()-1);
						} else {
							attrKey = attr.getKey();
						}
						double attrVal = attr.getValue();

						if (r.getPairs().containsKey(attrKey)) { // check the region's range for this attribute

							double region_low_range = r.getPairs().get(attrKey).getLow();
							double region_high_range = r.getPairs().get(attrKey).getHigh();

							if (attrVal < region_low_range || attrVal >= region_high_range) { // check whether attribute value is inside this region range
								if (isGUID) {
									flag_guid = false;
								} else {
									flag_attr = false;
								}
							}

						}

					}

					if (flag_attr || flag_guid) {

						int region_index = regions.indexOf(r)+1;
						for (int machine = (int)((region_index-1)*Math.sqrt(num_machines))+1; machine <= (region_index*Math.sqrt(num_machines)); machine++) {
							messagesCounterPerMachine.put(machine, messagesCounterPerMachine.get(machine)+1);
						}

					}

				}

			} else if (op instanceof Search) {

				Search s = (Search)op;

				for (Region r : regions) { // iterate over regions

					boolean flag = true;

					for (Map.Entry<String, Range> search_pair : s.getPairs().entrySet()) { // iterate over this search pairs

						String search_attr = search_pair.getKey();
						double search_low_range = search_pair.getValue().getLow();
						double search_high_range = search_pair.getValue().getHigh();

						if (r.getPairs().containsKey(search_attr)) { // check the region's range for this attribute 

							double region_low_range = r.getPairs().get(search_attr).getLow();
							double region_high_range = r.getPairs().get(search_attr).getHigh();

							if (search_low_range > search_high_range) {

								if (search_low_range >= region_high_range && search_high_range < region_low_range) {
									flag = false;
									break;
								}

							} else {

								if (search_low_range >= region_high_range || search_high_range < region_low_range) { // check whether both region & search ranges overlap
									flag = false;
									break;
								}

							}

						}

					}

					if (flag) {

						int region_index = regions.indexOf(r)+1;
						
						int max = (int)(region_index*Math.sqrt(num_machines)), min = (int)((region_index-1)*Math.sqrt(num_machines))+1;
						int machine = (new Random()).nextInt((max - min) + 1) + min;
						
						messagesCounterPerMachine.put(machine, messagesCounterPerMachine.get(machine)+1);

					}

				}

			}

		}

		return messagesCounterPerMachine;
		
	}
	
	public static Map<Integer, Integer> messagesCounterHyperDex(int num_machines, Queue<Operation> oplist, List<List<Region>> subspaces) {

		Map<Integer, Integer> messagesCounterPerMachine = new TreeMap<Integer, Integer>();

		// initializes messages counter per machine
		for (int i = 1; i <= num_machines; i++) {
			messagesCounterPerMachine.put(i, 0);
		}

		for (Operation op : oplist) {

			if (op instanceof Update) { 

				Update up = (Update)op;
				
				for (List<Region> regions : subspaces) { // iterate over subspaces

					for (Region r : regions) { // iterate over regions

						// Checks whether this update is in this region regarding its attribute or its guid
						boolean flag_attr = true, flag_guid = true;

						for (Map.Entry<String, Double> attr : up.getAttributes().entrySet()) { // iterate over this update attributes

							boolean isGUID = attr.getKey().contains("'");

							String attrKey;
							if (isGUID) {
								attrKey = attr.getKey().substring(0, attr.getKey().length()-1);
							} else {
								attrKey = attr.getKey();
							}
							double attrVal = attr.getValue();

							if (r.getPairs().containsKey(attrKey)) { // check the region's range for this attribute

								double region_low_range = r.getPairs().get(attrKey).getLow();
								double region_high_range = r.getPairs().get(attrKey).getHigh();

								if (attrVal < region_low_range || attrVal >= region_high_range) { // check whether attribute value is inside this region range
									if (isGUID) {
										flag_guid = false;
									} else {
										flag_attr = false;
									}
								}

							}

						}

						if (flag_attr || flag_guid) {

							int subspace_index = subspaces.indexOf(regions)+1;
							int region_index = regions.indexOf(r)+1;
							
							int machine_index = (subspace_index-1)*regions.size() + region_index;
							
							messagesCounterPerMachine.put(machine_index, messagesCounterPerMachine.get(machine_index)+1);

						}

					} // regions

				} // subspaces

			} else if (op instanceof Search) {

				Search s = (Search)op;
				
				// pick a random subspace
				List<Region> regions = subspaces.get((new Random().nextInt(subspaces.size())));

				for (Region r : regions) { // iterate over regions

					boolean flag = true;

					for (Map.Entry<String, Range> search_pair : s.getPairs().entrySet()) { // iterate over this search pairs

						String search_attr = search_pair.getKey();
						double search_low_range = search_pair.getValue().getLow();
						double search_high_range = search_pair.getValue().getHigh();

						if (r.getPairs().containsKey(search_attr)) { // check the region's range for this attribute 

							double region_low_range = r.getPairs().get(search_attr).getLow();
							double region_high_range = r.getPairs().get(search_attr).getHigh();

							if (search_low_range > search_high_range) {

								if (search_low_range >= region_high_range && search_high_range < region_low_range) {
									flag = false;
									break;
								}

							} else {

								if (search_low_range >= region_high_range || search_high_range < region_low_range) { // check whether both region & search ranges overlap
									flag = false;
									break;
								}

							}

						}

					}

					if (flag) {

						int subspace_index = subspaces.indexOf(regions)+1;
						int region_index = regions.indexOf(r)+1;
						
						int machine_index = (subspace_index-1)*regions.size() + region_index;
						
						messagesCounterPerMachine.put(machine_index, messagesCounterPerMachine.get(machine_index)+1);

					}

				}

			}

		}

		return messagesCounterPerMachine;
		
	}
	
	/* Look for GUIDs that must leave or enter a region because of a repartition. It can also compute the number of exchange messages because of a repartition.
	 * If countMessagesFlag = 0, it does not compute the number of messages.
	 * If countMessagesFlag = 1, the list of guids leaving or coming to a region is counted as one message to that region.
	 * If countMessagesFlag = 2, each guid leaving or coming to a region is counted as one message to that region. */
	public static void checkGUIDsAfterRepartition(Map<Integer, Integer> messagesCounterPerMachine, List<Region> regions, int countMessagesFlag) {
		
		Map<Integer, Map<String, Double>> leaving_guids = new TreeMap<Integer, Map<String, Double>>();
		Map<Region, List<Integer>> outgoing_regions = new HashMap<Region, List<Integer>>();
		Map<Region, List<Integer>> incoming_regions = new HashMap<Region, List<Integer>>();
		
		for (Region r : regions) {
			outgoing_regions.put(r, new ArrayList<Integer>());
			incoming_regions.put(r, new ArrayList<Integer>());
		}
		
		// I) Checks guids that must leave this region because of a repartition 
		for (Region r : regions) { // iterates over regions
			
			Map<Integer, Map<String, Double>> region_guids = new TreeMap<Integer, Map<String, Double>>(r.getGUIDs());
			
			for (int guid : region_guids.keySet()) { // iterates over this region's guids
			
				boolean guid_belongs_to_this_region = true;
				
				for (Map.Entry<String, Double> attr : region_guids.get(guid).entrySet()) { // iterates over this guid's attributes
					
					String guidAttrKey = attr.getKey();    
					double guidAttrVal = attr.getValue();
					
					if (r.getPairs().containsKey(guidAttrKey)) { // checks region's range for this attribute
						
						double regionLowRange  = r.getPairs().get(guidAttrKey).getLow();
						double regionHighRange = r.getPairs().get(guidAttrKey).getHigh();
						
						if (guidAttrVal < regionLowRange || guidAttrVal >= regionHighRange) { // checks whether attribute value is inside this region range
							guid_belongs_to_this_region = false;
							break;
						} 
						
					}
					
				}
				
				if (!guid_belongs_to_this_region) { // if guid doesn't belong to this region anymore
								
					// removes guid from this region
					leaving_guids.put(guid, region_guids.get(guid));
					r.removeGuid(guid);
					
					outgoing_regions.get(r).add(guid);
					
				}
				
			}
			
		}
		
		// II) Checks the right region for each guid leaving its region
		for (int guid : leaving_guids.keySet()) { // iterates over guids leaving regions
			
			for (Region r : regions) { // iterates over regions
				
				boolean guid_belongs_to_this_region = true;
				
				for (Map.Entry<String, Double> attr : leaving_guids.get(guid).entrySet()) { // iterates over this guid's attributes
					
					String guidAttrKey = attr.getKey();    
					double guidAttrVal = attr.getValue();
					
					if (r.getPairs().containsKey(guidAttrKey)) { // checks region's range for this attribute
						
						double regionLowRange  = r.getPairs().get(guidAttrKey).getLow();
						double regionHighRange = r.getPairs().get(guidAttrKey).getHigh();
						
						if (guidAttrVal < regionLowRange || guidAttrVal >= regionHighRange) { // checks whether attribute value is inside this region range
							guid_belongs_to_this_region = false;
							break;
						} 
						
					}
					
				}
				
				if (guid_belongs_to_this_region) { // if guid now belongs to this region
								
					// inserts guid into this region
					r.insertGuid(guid, leaving_guids.get(guid));
					
					incoming_regions.get(r).add(guid);
					
					break;
					
				}
				
			}
			
		}
		
		// III) Counts the number of messages per region 
		for (Region r : regions) {
			
			int num_of_messages; // number of messages per region
			
			switch (countMessagesFlag) {
			case 1:
				num_of_messages = 0;
				if (outgoing_regions.get(r).size() > 0)
					num_of_messages += 1;
				if (incoming_regions.get(r).size() > 0)
					num_of_messages += 1;
				break;
			case 2:
				num_of_messages = outgoing_regions.get(r).size() + incoming_regions.get(r).size();
				break;
			default:
				num_of_messages = 0;
				break;
			}
			
			if (num_of_messages > 0) {
				int num_machines = messagesCounterPerMachine.size();
				int region_index = regions.indexOf(r)+1;
				for (int machine = (int)((region_index-1)*Math.sqrt(num_machines))+1; machine <= (region_index*Math.sqrt(num_machines)); machine++) {
					// increments the number of messages for each machine associated with this region
					messagesCounterPerMachine.put(machine, messagesCounterPerMachine.get(machine)+num_of_messages);
				}
			}
			
		}
		
	}

}
