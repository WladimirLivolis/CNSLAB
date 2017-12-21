import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;

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
	public static double JFI(Queue<Operation> oplist, List<GUID> GUIDs, List<Region> rlist) {
		
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
	
	public static List<GUID> generateGUIDs(int qty, int AttrNum, Random rnd) {
		List<GUID> GUIDs = new ArrayList<GUID>(qty);
		for (int i = 0; i < qty; i++) {
			GUID guid = new GUID("GUID"+i);
			for (int j = 1; j <= AttrNum; j++) {
				double v = rnd.nextDouble();
				guid.set_attribute("A"+j, v);
			}
			GUIDs.add(guid);
		}
		return GUIDs;
	}
	
	public static List<GUID> copyGUIDs(List<GUID> GUIDs) {
		List<GUID> newGUIDs = new ArrayList<GUID>(GUIDs.size());
		for (GUID guid : GUIDs) {
			GUID copy = new GUID(guid.getName());
			for (Attribute a : guid.getAttributes()) {
				copy.set_attribute(a.getKey(), a.getValue());
			}
			newGUIDs.add(copy);
		}
		return newGUIDs;
	}
	
	public static Queue<Operation> copyOplist(Queue<Operation> oplist, List<GUID> copyOfGUIDs) {
		Queue<Operation> newOplist = new LinkedList<Operation>();
		for (Operation op : oplist) {
			
			if (op instanceof Update) {
				
				Update up = (Update)op;
				Update copy = null;
				
				for (GUID guid : copyOfGUIDs) {
					if (guid.getName().equals(up.getGuid().getName())) {
						copy = new Update(guid);
					}
				}
				
				newOplist.add(copy);				
				
			}
			
			if (op instanceof Search) {
				
				Search s = (Search)op;
				Search copy = new Search();
				
				for (PairAttributeRange pair : s.getPairs()) {
					
					copy.addPair(pair.getAttrkey(), new Range(pair.getRange().getLow(), pair.getRange().getHigh()));
					
				}
				
				newOplist.add(copy);
				
			}
			
		}
		
		return newOplist;
		
	}
	
	public static List<Region> copyRegions(List<Region> regions) {
		List<Region> copy = new ArrayList<Region>(regions.size());
		for (Region r : regions)
			copy.add(new Region(r.getName(),r.getPairs()));
		return copy;
	}
	
	public static Queue<Update> generateUpdateLoad(int AttrNum, int UpNum, List<GUID> GUIDs, Random rnd) {
		Queue<Update> updates = new LinkedList<Update>();
		for (int i = 0; i < UpNum; i++) {
			GUID guid = GUIDs.get(rnd.nextInt(GUIDs.size())); // pick one of the GUIDs already created
			Update up = new Update(guid);
			for (int j = 1; j <= AttrNum; j++) {
				double v = rnd.nextDouble();
				up.addAttr("A"+j, v);
			}
			updates.add(up);
		}
		return updates;
	}
	
	public static Queue<Search> generateSearchLoad(int AttrNum, int SNum, Random rnd) {
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
	
	public static void checkLoadPerRegion(List<Region> regions, Queue<Operation> oplist) {
		
		clear_regions_load(regions);
		
		for (Operation op : oplist) {
			
			if (op instanceof Update) { checkUpdateLoadPerRegion(regions, (Update)op); }
			
			if (op instanceof Search) {	checkSearchLoadPerRegion(regions, (Search)op); }
			
		}
		
	}

	private static void checkUpdateLoadPerRegion(List<Region> regions, Update up) {

		for (Region r : regions) { // iterate over regions

			// I) Checks whether this update is in this region regarding its attribute

			boolean flag_attr = true;

			for (Attribute attr : up.getAttributes()) { // iterate over this update attributes

				String up_attr = attr.getKey();   // update attribute
				double up_val  = attr.getValue(); // update value

				for (PairAttributeRange pair : r.getPairs()) { // iterate over this region pairs

					String r_attr  = pair.getAttrkey();         // region attribute
					double r_start = pair.getRange().getLow();  // region range start
					double r_end   = pair.getRange().getHigh(); // region range end

					if (up_attr.equals(r_attr)) { // if we are dealing with same attribute

						if (up_val < r_start || up_val > r_end) { // check whether update value is inside this region range
							flag_attr = false;
						}

					}

				}

			}

			// II) Now checks whether this update is in this region regarding its guid

//			boolean flag_guid = true;
//
//			GUID guid = up.getGuid();
//
//			for (Attribute attr : guid.getAttributes()) { // iterate over this guid attributes
//
//				String guid_attr = attr.getKey();   // guid attribute
//				double guid_val  = attr.getValue(); // guid value
//
//				for (PairAttributeRange pair : r.getPairs()) { // iterate over this region pairs
//
//					String r_attr  = pair.getAttrkey();         // region attribute
//					double r_start = pair.getRange().getLow();  // region range start
//					double r_end   = pair.getRange().getHigh(); // region range end
//
//					if (guid_attr.equals(r_attr)) { // if we are dealing with same attribute
//
//						if (guid_val < r_start || guid_val > r_end) { // check whether guid value is inside this region range
//							flag_guid = false;
//						}
//
//					}
//
//				}
//
//			}

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

			for (PairAttributeRange s_pair : s.getPairs()) { // iterate over this search pairs

				String s_attr  = s_pair.getAttrkey();         // search attribute
				double s_start = s_pair.getRange().getLow();  // search range start
				double s_end   = s_pair.getRange().getHigh(); // search range end

				for (PairAttributeRange r_pair : r.getPairs()) { // iterate over this region pairs

					String r_attr  = r_pair.getAttrkey();         // region attribute
					double r_start = r_pair.getRange().getLow();  // region range start
					double r_end   = r_pair.getRange().getHigh(); // region range end 

					if (s_attr.equals(r_attr)) { // check whether we are dealing with same atribute

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
	
	public static void checkTouchesPerRegion(List<Region> regions, List<GUID> guids, Queue<Operation> oplist) {
		
		clear_regions_touches(regions);
		
		distributeGUIDsAmongRegions(regions, guids);
		
		for (Operation op : oplist) {
			
			if (op instanceof Update) {	checkUpdateTouchesPerRegion(regions, (Update)op); }
			
			if (op instanceof Search) {	checkSearchTouchesPerRegion(regions, (Search)op); }
			
		}
		
	}
	
	private static void checkUpdateTouchesPerRegion(List<Region> regions, Update up) {
		
		GUID guid = up.getGuid();
		
		Region previousRegion = null;
		
		// I) This first iteration over regions will look for touches due to previous GUID's positions
		for (Region region : regions) {

			boolean previouslyInRegion = true;
			int count = 0;

			// Checks whether this update's GUID is already in this region
			for (Attribute attr : guid.getAttributes()) { // iterate over guid attributes

				String guidAttrKey = attr.getKey();    
				double guidAttrVal = attr.getValue(); 

				for (PairAttributeRange pair : region.getPairs()) { // iterate over this region attributes

					String regionAttrKey    = pair.getAttrkey();         
					double regionRangeStart = pair.getRange().getLow(); 
					double regionRangeEnd   = pair.getRange().getHigh(); 

					if (guidAttrKey.equals(regionAttrKey)) {
						if (guidAttrVal < regionRangeStart || guidAttrVal > regionRangeEnd) { // checks whether guid is in this region
							previouslyInRegion = false;
						}
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
		
		// II) This second iteration over regions will look for touches due to new GUID's positions
		for (Region region : regions) {
			
			boolean comingToRegion = true;
			int count = 0;

			// Checks whether this update moves a GUID to this region
			for (Attribute attr : up.getAttributes()) { // iterate over this update attributes

				String updateAttrKey = attr.getKey();
				double updateAttrVal = attr.getValue();

				for (PairAttributeRange pair : region.getPairs()) { // iterate over this region attributes

					String regionAttrKey = pair.getAttrkey();         
					double regionRangeStart = pair.getRange().getLow(); 
					double regionRangeEnd = pair.getRange().getHigh();

					if (updateAttrKey.equals(regionAttrKey)) {
						if (updateAttrVal < regionRangeStart || updateAttrVal > regionRangeEnd) { // checks whether guid is coming to this region (or if it is staying in this region)
							comingToRegion = false;
						}
					}
				}
			}

			// If so ...
			if (comingToRegion) {

				for (Attribute attr : up.getAttributes()) { guid.set_attribute(attr.getKey(), attr.getValue()); } // updates its attributes with info from this update operation			

				region.insertGuid(guid); // adds it to this region's guid list
				if (!previousRegion.equals(region)) { count++; } // if it is coming from another region, counts one more touch

			}
			
			// Sets this region's update touch count
			int previous_count = region.getUpdateTouches();
			region.setUpdateTouches(previous_count+count);
			
		}
	}
	
	private static void checkSearchTouchesPerRegion(List<Region> regions, Search s) {

		for (Region region : regions) { // iterate over regions

			boolean isInRegion = true;
			int count = 0;

			// I) Checks whether this search is in this region

			for (PairAttributeRange searchPair : s.getPairs()) { // iterate over this search's attributes

				String searchAttrKey    = searchPair.getAttrkey();
				double searchRangeStart = searchPair.getRange().getLow();
				double searchRangeEnd   = searchPair.getRange().getHigh();

				for (PairAttributeRange regionPair : region.getPairs()) { // iterate over this region's attributes

					String regionAttrKey    = regionPair.getAttrkey();
					double regionRangeStart = regionPair.getRange().getLow();
					double regionRangeEnd   = regionPair.getRange().getHigh();

					if (searchAttrKey.equals(regionAttrKey)) {
						if (searchRangeStart > searchRangeEnd) {
							if (searchRangeStart > regionRangeEnd && searchRangeEnd < regionRangeStart) { isInRegion = false; }
						} else {
							if (searchRangeStart > regionRangeEnd || searchRangeEnd < regionRangeStart) { isInRegion = false; }
						}
					}
				}
			}

			// II) If so, counts the number of guids already in this region that meet this search's requirements

			if (isInRegion) {

				for (GUID guid : region.getGUIDs()) { // iterate over this region's guids

					boolean flag = true;

					for (Attribute attr : guid.getAttributes()) { // iterate over this guid's attributes

						String guidAttrKey = attr.getKey();    
						double guidAttrVal = attr.getValue();

						for (PairAttributeRange pair : s.getPairs()) { // iterate over this search's attributes

							String searchAttrKey    = pair.getAttrkey();
							double searchRangeStart = pair.getRange().getLow();
							double searchRangeEnd   = pair.getRange().getHigh();

							if (guidAttrKey.equals(searchAttrKey)) {
								if (searchRangeStart > searchRangeEnd) {
									if (guidAttrVal < searchRangeStart && guidAttrVal > searchRangeEnd) { flag = false; }
								} else {
									if (guidAttrVal < searchRangeStart || guidAttrVal > searchRangeEnd) { flag = false; }
								}
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
			
	private static void distributeGUIDsAmongRegions(List<Region> regions, List<GUID> guids) {
		
		for (GUID guid : guids) { // iterate over GUIDs
			
			for (Region region : regions) { // iterate over regions
				
				boolean isInRegion = true;
				
				for (Attribute attr : guid.getAttributes()) { // iterate over this guid's attributes
					
					String guidAttrKey = attr.getKey();    
					double guidAttrVal = attr.getValue();
					
					for (PairAttributeRange pair : region.getPairs()) { // iterate over this region's attributes
						
						String regionAttrKey = pair.getAttrkey();         
						double regionRangeStart = pair.getRange().getLow(); 
						double regionRangeEnd = pair.getRange().getHigh();
						
						if (guidAttrKey.equals(regionAttrKey)) {
							
							if (guidAttrVal < regionRangeStart || guidAttrVal > regionRangeEnd) { // checks whether guid is in this region
								
								isInRegion = false;
								
							}
							
						}
						
					}
					
				}
				
				//If so ...
				if (isInRegion) { 
					region.insertGuid(guid); // adds it to this region's guids list
				}
				
			}
			
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

}
