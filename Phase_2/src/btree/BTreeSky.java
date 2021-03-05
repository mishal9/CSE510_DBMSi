package btree;

import java.io.IOException;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.SortException;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

public class BTreeSky extends Iterator implements GlobalConst {

	private String relationName;
	private BTreeFile[] btreeindexes;
	private AttrType[] attrType;
	private int attr_nos;
	private short[] t1_str_sizes;
	private int amt_of_mem;
	private Iterator iterator1;
	private int[] pref_list;
	private int pref_length_list;
	private int n_pages;


	private Tuple firstSkyLineElement;
	//private BlockNestedLoopSkyline blockNestedLoopSkyline;



	/**
	 * @param in1
	 * @param len_in1
	 * @param t1_str_sizes
	 * @param amt_of_mem
	 * @param am1
	 * @param relationName used to open indexscans
	 * @param pref_list
	 * @param pref_length_list length of the preference list
	 * @param index_file_list
	 * @param n_pages
	 * @throws Exception 
	 */
	public BTreeSky(AttrType[] in1, int len_in1, short[] t1_str_sizes, int amt_of_mem, Iterator am1,
			String relationName, int[] pref_list, int pref_length_list, IndexFile[] index_file_list,
			int n_pages) throws Exception {


		this.relationName = relationName;
		this.btreeindexes = (BTreeFile[]) index_file_list;

		this.attrType = in1;
		this.attr_nos = len_in1;
		this.t1_str_sizes = t1_str_sizes;
		this.amt_of_mem = amt_of_mem;
		this.iterator1 = am1;
		this.pref_list = pref_list;
		this.pref_length_list = pref_length_list;
		this.n_pages = n_pages;

		this.firstSkyLineElement = null;

		//TODO blockNestedLoopSkyline = null;


	}


	private void runBtreeSkyAlgo() throws Exception {
		int numberOfBtreeIndexes= btreeindexes.length;

		//create full index scans for all btrees
		BTFileScan[] fullBtreeIndexScans = new BTFileScan[numberOfBtreeIndexes];
		DiskBackedArray[] setArr = new DiskBackedArray[numberOfBtreeIndexes];

		for (int i = 0; i < numberOfBtreeIndexes; i++) {
			fullBtreeIndexScans[i] = btreeindexes[i].new_scan(null, null);
			setArr[i] = new DiskBackedArray();
		}
		RID firstSkyLineElementRID = null;
		boolean stopBtreeSkyLoop =false;
		for (int skyLoopCtr = 0; skyLoopCtr <= 5 && stopBtreeSkyLoop  == false; skyLoopCtr++) {

			// loop over full index scans for each btree
			for (int i = 0; i < numberOfBtreeIndexes; i++) {
				KeyDataEntry scannedVal = fullBtreeIndexScans[i].get_next();
				if (scannedVal == null) {
					System.out.println("got null");
					break;
				}

				// check in all other btree if this key has been found before
				RID rid = ((LeafData) scannedVal.data).getData();
				System.out.println("tree: " + i + " scannedVal: " + scannedVal.key + " RID: " + rid);
				System.out.println(i + " : " + setArr[i]);

				// check in other indexes
				int foundCount=0;
				for (int otherid = 0; otherid < numberOfBtreeIndexes; otherid++) {
					if (otherid != i) {
						System.out.println("checking " + setArr[otherid] + " for " + rid);
						if (setArr[otherid].getIndex(rid) >= 0) {
							System.out.println("rid found in other index" + rid);
							setArr[otherid].printToConsole();
							foundCount++;
						}
					}
				}
				if(foundCount == (numberOfBtreeIndexes -1)) {
					stopBtreeSkyLoop = true; //stop the btree skyline loop
					firstSkyLineElementRID = rid;
					System.out.println("firstSkyLineElement: "+firstSkyLineElementRID);

					break;
				}

				setArr[i].add(rid);
				System.out.println("oooo");
			}
			System.out.println("---");
		} //end of btreeskyline main loop, now do block nested

		//open main relation data file
		Heapfile originalDataHeapFile = new Heapfile(relationName);
		firstSkyLineElement = getEmptyTuple();
		firstSkyLineElement.tupleCopy(originalDataHeapFile.getRecord(firstSkyLineElementRID));
		
		//create heapfile with all elements of the all the arrays of the indexes

		//create a heapfile which will store the pruned data
		Heapfile prunedDataFile = new Heapfile("somerelation.in");

		//an array to check and avoid inserting duplicates in the pruned data
		DiskBackedArray insertCheckerList = new DiskBackedArray();

		for (int i = 0; i < numberOfBtreeIndexes; i++) {
			DiskBackedArray curArray = setArr[i];
			System.out.print("----- pruning for array: "+i +" ----------- " );
			curArray.printToConsole();
			RID temp= new RID();
			Scan scan =curArray.getHeapfile().openScan();
			Tuple tup;
			boolean done = false;
			while(!done) {
				tup= scan.getNext(temp );
				if(tup==null) {
					done=true;
					break;
				}
				RID scannedRID = DiskBackedArray.getRIDFromByteArr(tup.returnTupleByteArray());

				if(scannedRID.equals(firstSkyLineElementRID)) {
					System.out.println("not inserting any more to pruned file as skyline element hit");
					break;
				}

				if(insertCheckerList.getIndex(scannedRID) < 0) { //this record not present in pruned file
					Tuple mainFileTuple = originalDataHeapFile.getRecord(scannedRID);

					prunedDataFile.insertRecord(mainFileTuple.getTupleByteArray() );
					insertCheckerList.add(scannedRID);
					System.out.println("inserted record to pruned data file");
				} else {
					System.out.println("not inserting record as already present in pruned file");
				}

			}
			System.out.println("");



			scan.closescan();

		}

		System.out.println("prunedDataFile count --> "+prunedDataFile.getRecCnt());

		//run block nested loop skyline on the pruned data now

		//TODO
		// blockNestedLoopSkyline = new BlockNestedLoopSkyline()
	}


	public static void main(String[] args) {
		System.out.println("start");
		float aa = (float) 0.356314137;
		System.out.println(aa+" " );


	}

	//util method to create an empty tuple of reqd specs
	private Tuple getEmptyTuple() throws InvalidTypeException, InvalidTupleSizeException, IOException {
		Tuple t = new Tuple();
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		int size = t.size();
		t = new Tuple(size);
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		return t;
	}



	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
	InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
	LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {

		if(firstSkyLineElement == null) { //run btreesky
			runBtreeSkyAlgo();
			return firstSkyLineElement;

		} else { //run block nested loop sky
			//blockNestedLoopSkyline.get_next(); TODO
		}
		return null;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO blockNestedLoopSkyline.close()

	}

}
