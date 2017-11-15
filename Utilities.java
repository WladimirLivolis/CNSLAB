import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
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
		
		clear_regions_load(regions);

		for (Update up : uplist) { // iterate over updates

			// I) Check whether this update is in this region regarding its attribute
			
			for (Attribute attr : up.getAttributes()) { // iterate over this update attributes

				String up_attr = attr.getKey();   // update attribute
				double up_val  = attr.getValue(); // update value

				for (Region r : regions) { // iterate over regions
					
					boolean flag = false;

					for (PairAttributeRange pair : r.getPairs()) { // iterate over this region pairs

						String r_attr  = pair.getAttrkey();         // region attribute
						double r_start = pair.getRange().getLow();  // region range start
						double r_end   = pair.getRange().getHigh(); // region range end

						if (up_attr.equals(r_attr)) { // if we are dealing with same attribute

							if (up_val >= r_start && up_val <= r_end) { // check whether update value is inside this region range
								flag = true;
							} else {
								flag = false;
							}

						}

					}

					if (flag) {

						r.getUpdateLoad().add(up);

					}

				}

			}
			
			// II) Now checks whether this update is in this region regarding its guid
			
			GUID guid = up.getGuid();
			
			for (Attribute attr : guid.getAttributes()) { // iterate over this guid attributes
				
				String guid_attr = attr.getKey();   // guid attribute
				double guid_val  = attr.getValue(); // guid value
				
				for (Region r : regions) { // iterate over regions
					
					boolean flag = false;
					
					for (PairAttributeRange pair : r.getPairs()) { // iterate over this region pairs
						
						String r_attr  = pair.getAttrkey();         // region attribute
						double r_start = pair.getRange().getLow();  // region range start
						double r_end   = pair.getRange().getHigh(); // region range end

						if (guid_attr.equals(r_attr)) { // if we are dealing with same attribute
							
							if (guid_val >= r_start && guid_val <= r_end) { // check whether guid value is inside this region range
								flag = true;
							} else {
								flag = false;
							}
						}
						
					}
					
					if (flag && !r.getSearchLoad().contains(up)) {
						
						r.getUpdateLoad().add(up);

					}					
				}
				
			}

		}

	}

	public static void checkSearchLoadPerRegion(List<Region> regions, Queue<Search> slist) {
		
		clear_regions_load(regions);

		for (Search s : slist) { // iterate over searches

			for (PairAttributeRange s_pair : s.getPairs()) { // iterate over this search pairs

				String s_attr  = s_pair.getAttrkey();         // search attribute
				double s_start = s_pair.getRange().getLow();  // search range start
				double s_end   = s_pair.getRange().getHigh(); // search range end

				for (Region r : regions) { // iterate over regions

					boolean flag = false;

					for (PairAttributeRange r_pair : r.getPairs()) { // iterate over this region pairs

						String r_attr  = r_pair.getAttrkey();         // region attribute
						double r_start = r_pair.getRange().getLow();  // region range start
						double r_end   = r_pair.getRange().getHigh(); // region range end 

						if (s_attr.equals(r_attr)) { // check whether we are dealing with same atribute

							if (s_start > s_end) { // trata o caso de buscas uniformes (circular) --> start > end: [start,1.0] ^ [0.0,end]

								if (s_start <= r_end || s_end >= r_start) {

									flag = true;

								} else {

									flag = false;

								}

							} else {

								if (s_start <= r_end && s_end >= r_start) { // check whether both region & search ranges overlap

									flag = true;

								} else {

									flag = false;

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

	}
	
	private static void clear_regions_load(List<Region> regions) {
		
		for (Region r : regions) {
			
			r.getSearchLoad().clear();
			r.getUpdateLoad().clear();
			
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
