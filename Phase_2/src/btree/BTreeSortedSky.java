package btree;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

import java.io.IOException;
import java.util.Arrays;

import bufmgr.PageNotReadException;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import btree.*;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import iterator.FileScan;
import iterator.TupleUtils;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.PredEvalException;
import iterator.SortException;
import iterator.SortPref;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.UnknownKeyTypeException;

/**
 * BTreeSortedSky(
 * AttrType[] in1, 
 * int len_in1, 
 * short[] t1_str_sizes, 
 * int Iterator am1, 
   java.lang.String relationName, 
   int[] pref_list, 
   int[] pref_list_length, 
   IndexFile index_file,
	int n_pages)
 * @author kunjpatel
 *
 */

public class BTreeSortedSky implements GlobalConst {
	
	private AttrType[] attrType;
	private int attr_len;
	private short[] t1_str_sizes;
	private Iterator am1;
	String relationName;
	private int[] pref_list;
	private int pref_list_length;
	private IndexFile index_file;
	private int n_pages;
	private int amt_of_mem;

	boolean status = OK;
	private static Tuple[] _window;
	
	/**
	 * AttrType[] in1 
	 * int attr_len
	 * short[] t1_str_sizes
	 * int Iterator am1
	 * java.lang.String relationName 
	 * int[] pref_list 
	 * int[] pref_list_length
	 * IndexFile index_file
	 * int n_pages
	 */
 
	public BTreeSortedSky(AttrType[] attrType, int attr_len, short[] t1_str_sizes, int amt_of_mem, Iterator am1,
			String relationName, int[] pref_list, int pref_list_length, IndexFile index_file,
			int n_pages) throws Exception {


		this.relationName = relationName;
		this.index_file = index_file;
		this.attrType = attrType;
		this.attr_len = attr_len;
		this.t1_str_sizes = t1_str_sizes;
		this.am1 = am1;
		this.pref_list = pref_list;
		this.pref_list_length = pref_list_length;
		this.n_pages = n_pages; 
		
		this.amt_of_mem = amt_of_mem;
		
		System.out.println("************** INIT CALLED ****************");
		System.out.println("relationName: " + relationName );
		System.out.println("attrType: " + Arrays.toString(attrType) );
		System.out.println("attr_len: " + attr_len );
		System.out.println("t1_str_sizes: " + t1_str_sizes );
		System.out.println("pref_list: " + Arrays.toString(pref_list) );
		System.out.println("pref_list_length: " + pref_list_length );
		System.out.println("n_pages: " + n_pages );
		System.out.println("**************   INIT END  ****************");
	}
	
	
	
	public void computeSkylines(String file, SortPref sort, Heapfile tmp) throws InvalidSlotNumberException, InvalidTupleSizeException, Exception {
//
//        /*
//        SORT FIRST SKY:
//
//        1. Put the first <n_pages> sorted tuples in main memory
//        2. Compare the rest against the ones in main memory
//            1. If tuple in heap file is dominated by at least one in main memory - simply discard it from heap
//            2. If tuple in heap file dominates any one in main memory - replace the tuple with the one in main memory
//            3. If tuple is not dominated by any tuple in main memory - put into temp heap file
//        3. At end - return both temp heap + main memory objects
//
//
//        Scenarios:
//
//        If tuple heap_tuple (dominated by):
//	        dominated by tuple memory_tuple -> discard heap_tuple
//	        not dominated by any of tuple memory_tuple -> put into temp heap
//        If tuple heap_tuple  (dominates):
//	        dominates memory_tuple -> discard memory_tuple
//         */
		
		Heapfile hf = new Heapfile("heap_" + "AAA");
		BTFileScan scan = ((BTreeFile) index_file).new_scan(null, null);
		KeyDataEntry entry;
		RID rid;
		
		Tuple t = getEmptyTuple();
	    
		_window = new Tuple[(MINIBASE_PAGESIZE / t.size()) * n_pages];
		
		System.out.println("SIZE: " + (MINIBASE_PAGESIZE / t.size()) * n_pages);
		//Getting the first tuple
	    entry = scan.get_next();
	    
	    int count = 0;
	    
//	    while (t != null && count < _window.length) {
//            Tuple temp = new Tuple(t);
//            _window[count++] = temp;
//            try {
//                t = sort.get_next();
//            }
//            catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//        }
	    
	    System.out.println("attrType " + attrType.length);
	    
	    while (entry != null && count < _window.length) {
	    	Tuple temp = getEmptyTuple();
        	rid = ((LeafData) entry.data).getData();
        	temp.tupleCopy(hf.getRecord(rid));
        	temp.print(attrType); 
        	_window[count++] = temp;
            entry = scan.get_next();
        }
	    
	    System.out.println("In memory objects");
        for(int i=0; i<_window.length; i++) {
            if(_window[i] != null)
                _window[i].print(attrType);
        }
        

        int dominates = 0;
        
        while (entry != null) {
            boolean isDominatedBy = false;
//            System.out.println("=======");
            Tuple htuple = getEmptyTuple();
            
            rid = ((LeafData) entry.data).getData();
            htuple.tupleCopy(hf.getRecord(rid));
//            try {
//                /*
//                Compare the rest against the ones in main memory
//                1. If tuple in heap file is dominated by at least one in main memory - simply discard it from heap
//                2. If tuple in heap file dominates any one in main memory - replace the tuple with the one in main memory
//                3. If tuple is not dominated by any tuple in main memory - put into temp heap file
//                */
            	
                for(int i=0; i<_window.length; i++){
                    if (TupleUtils.Dominates(htuple, attrType, _window[i], attrType, (short) attr_len, t1_str_sizes, pref_list, pref_list_length)) {
                        // 2. If tuple in heap file dominates any one in main memory - replace the tuple with the one in main memory
//                        System.out.println("Heap tuple");
//                        htuple.print(attrType);
//                        System.out.println("Dominates ");
//                        _window[i].print(attrType);
//                        System.out.println(" ");
                        _window[i] = htuple;
                        dominates++;
                    } else {
                        // 1. If tuple in heap file is dominated by at least one in main memory - simply discard it from heap
                        isDominatedBy = true;
//                        System.out.println("Heap tuple");
//                        htuple.print(attrType);
//                        System.out.println("Dominated by ");
//                        _window[i].print(attrType);
//                        System.out.println(" ");
                        break;
                    }
                }

                if(!isDominatedBy){
                    // 3. If tuple is not dominated by any tuple in main memory - put into temp heap file
                    // Tuple remained un-dominated -> potential skyline object
                    // check if space left in window
                    // else put in temp heaps
                    htuple.print(attrType);
                    System.out.println("Is not Dominated by any window objects");
                } 
              entry = scan.get_next();
        }
        
        System.out.println("Dominates "+dominates);
        System.out.println("Window objects ");
        
        for(int i=0; i<_window.length; i++){
            if(_window[i] != null)
                _window[i].print(attrType);
        }

        return;
    }
	
	
	private Tuple getEmptyTuple() throws InvalidTypeException, InvalidTupleSizeException, IOException {
		Tuple t = new Tuple();
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		int size = t.size();
		t = new Tuple(size);
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		return t;
	}

}