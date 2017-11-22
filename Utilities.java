import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

public class Utilities {
	
	public static List<GUID> generateGUIDs(int qty, int AttrNum, Random rnd) {
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
	
	public static Queue<Update> generateUpdateLoad(int AttrNum, int UpNum, List<GUID> GUIDs, Random rnd) {
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

	public static void checkUpdateLoadPerRegion(List<Region> regions, Queue<Update> uplist) {
		
		clear_update_load(regions);
		
		for (Update up : uplist) { // iterate over updates

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
				
				boolean flag_guid = true;

				GUID guid = up.getGuid();

				for (Attribute attr : guid.getAttributes()) { // iterate over this guid attributes

					String guid_attr = attr.getKey();   // guid attribute
					double guid_val  = attr.getValue(); // guid value

					for (PairAttributeRange pair : r.getPairs()) { // iterate over this region pairs

						String r_attr  = pair.getAttrkey();         // region attribute
						double r_start = pair.getRange().getLow();  // region range start
						double r_end   = pair.getRange().getHigh(); // region range end

						if (guid_attr.equals(r_attr)) { // if we are dealing with same attribute

							if (guid_val < r_start || guid_val > r_end) { // check whether guid value is inside this region range
								flag_guid = false;
							}
							
						}

					}

				}
				
				if (flag_attr || flag_guid) {
					
					r.getUpdateLoad().add(up);

				}
				
			}

		}

	}
	
	public static void checkSearchLoadPerRegion(List<Region> regions, Queue<Search> slist) {
		
		clear_search_load(regions);

		for (Search s : slist) { // iterate over searches

			for (Region r : regions) { // iterate over regions

				boolean flag = true;

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
								
							} else {

								if (s_start > r_end || s_end < r_start) { // check whether both region & search ranges overlap

									flag = false;

								}

							}

						}

					}
					
				}
				
				if (flag) {

					r.getSearchLoad().add(s);

				}

			}

		}

	}
	
	public static void checkTouchesPerRegion(List<Region> regions, List<GUID> guids, Queue<Search> slist, Queue<Update> uplist) {
		
		distributeGUIDsAmongRegions(regions, guids);
		
		Map<Region, Integer> updateTouchesPerRegion = checkUpdateTouchesPerRegion(regions, uplist);
		Map<Region, Integer> searchTouchesPerRegion = checkSearchTouchesPerRegion(regions, slist);
		
		for (Region r : regions) {
			
			int count = 0;
			count += updateTouchesPerRegion.get(r);
			count += searchTouchesPerRegion.get(r);
			r.setTouches(count);
			
		}
		
	}
	
	private static Map<Region, Integer> checkUpdateTouchesPerRegion(List<Region> regions, Queue<Update> uplist) {
		
		Map<Region, Integer> updateTouchesPerRegion = new HashMap<Region, Integer>();
		
		for (Update up : uplist) { // iterate over updates
			
			GUID guid = up.getGuid();

			for (Region region : regions) { // iterate over regions

				boolean previouslyInRegion = true;
				int count = 0;

				// I) Checks whether this update's GUID is already in this region
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
					if (region.getGUIDs().contains(guid)) { region.getGUIDs().remove(guid); } // removes it from this region's guid list
				}
				
				boolean comingToRegion = true;

				// II) Checks whether this update moves a GUID to this region
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
				
					region.getGUIDs().add(guid); // adds it to this region's guid list
					if (!previouslyInRegion) { count++; } // if it is coming from another region, counts one more touch
					
				}

				updateTouchesPerRegion.put(region, count);

			}
		}
		
		return updateTouchesPerRegion;
		
	}
	
	private static Map<Region, Integer> checkSearchTouchesPerRegion(List<Region> regions, Queue<Search> slist) {
		
		Map<Region, Integer> searchTouchesPerRegion = new HashMap<Region, Integer>();

		for (Search s : slist) { // iterate over searches

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
				
				searchTouchesPerRegion.put(region, count);
				
			}
		}
		
		return searchTouchesPerRegion;
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
					region.getGUIDs().add(guid); // adds it to this region's guids list
				}
				
			}
			
		}
		
	}
	
	private static void clear_search_load(List<Region> regions) {
		
		for (Region r : regions)
			r.getSearchLoad().clear();
		
	}
	
	private static void clear_update_load(List<Region> regions) {
		
		for (Region r : regions)
			r.getUpdateLoad().clear();
		
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
