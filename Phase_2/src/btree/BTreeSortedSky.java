package btree;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

import java.io.IOException;
import java.util.Arrays;

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
import iterator.FileScan;
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
	
	
	
//	public void computeSkylines(String file, SortPref sort, Heapfile tmp) throws IOException {
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
//
//        // create a tuple of appropriate size
//        Tuple t = new Tuple();
//
//        try {
//            // check if there's atleast one tuple
//            t = sort.get_next();
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }
//
//        int count = 0;
//
//        while (t != null && count < n_pages) {
//            Tuple temp = new Tuple(t);
////            temp.print(attrType);
//            _window[count] = temp;
//            // _window.add(temp);
//            count++;
//
//            try {
//                t = sort.get_next();
//            }
//            catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//        }
//
//
//        // AT THIS point: in-memory window is formed
//
//        System.out.println("In memory objects");
//        for(int i=0; i<_window.length; i++) {
//            if(_window[i] != null)
//                _window[i].print(_attrType);
//        }
//
//        FileScan fscan = null;
//
//        try {
//            fscan = new FileScan(file, _attrType, _attrSize, (short) _len_in, _len_in, _projlist, null);
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }
//
//        try {
//            t = fscan.get_next();
//        }
//        catch (Exception e) {
//            status = FAIL;
//            e.printStackTrace();
//        }
//
//        int dominates = 0;
//
//        while (t != null) {
//            boolean isDominatedBy = false;
//            System.out.println("=======");
//            Tuple htuple = new Tuple(t);
//
//            try {
//                /*
//                Compare the rest against the ones in main memory
//                1. If tuple in heap file is dominated by at least one in main memory - simply discard it from heap
//                2. If tuple in heap file dominates any one in main memory - replace the tuple with the one in main memory
//                3. If tuple is not dominated by any tuple in main memory - put into temp heap file
//                */
//
//                for(int i=0; i<_window.length; i++){
//                    if (TupleUtils.Dominates(htuple, _attrType, _window[i], _attrType, _len_in, _str_sizes, _pref_list, _pref_list_length)) {
//                        // 2. If tuple in heap file dominates any one in main memory - replace the tuple with the one in main memory
//                        System.out.println("Heap tuple");
//                        htuple.print(_attrType);
//                        System.out.println("Dominates ");
//                        _window[i].print(_attrType);
//                        System.out.println(" ");
//                        _window[i] = htuple;
//                        dominates++;
//                    } else {
//                        // 1. If tuple in heap file is dominated by at least one in main memory - simply discard it from heap
//                        isDominatedBy = true;
//                        System.out.println("Heap tuple");
//                        htuple.print(_attrType);
//                        System.out.println("Dominated by ");
//                        _window[i].print(_attrType);
//                        System.out.println(" ");
//                        break;
//                    }
//                }
//
//                if(!isDominatedBy){
//                    // 3. If tuple is not dominated by any tuple in main memory - put into temp heap file
//                    // Tuple remained un-dominated -> potential skyline object
//                    // check if space left in window
//                    // else put in temp heap
//                    htuple.print(_attrType);
//                    System.out.println("Is not Dominated by any window objects");
//                }
//
//            }
//            catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//
//            try {
//                t = fscan.get_next();
//            }
//            catch (Exception e) {
//                status = FAIL;
//                e.printStackTrace();
//            }
//        }
//
//        System.out.println("Dominates "+dominates);
//
//        System.out.println("Window objects ");
//
//        for(int i=0; i<_window.length; i++){
//            if(_window[i] != null)
//                _window[i].print(_attrType);
//        }
//
//        close();    // free up resources
//
//        return;
//    }
	
	
	private Tuple getEmptyTuple() throws InvalidTypeException, InvalidTupleSizeException, IOException {
		Tuple t = new Tuple();
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		int size = t.size();
		t = new Tuple(size);
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		return t;
	}

}