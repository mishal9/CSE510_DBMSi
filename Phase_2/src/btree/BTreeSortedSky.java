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
import heap.FieldNumberOutOfBoundException;
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
	
	private Heapfile temp;
	
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
		
//		System.out.println("************** INIT CALLED ****************");
//		System.out.println("relationName: " + relationName );
//		System.out.println("attrType: " + Arrays.toString(attrType) );
//		System.out.println("attr_len: " + attr_len );
//		System.out.println("t1_str_sizes: " + t1_str_sizes );
//		System.out.println("pref_list: " + Arrays.toString(pref_list) );
//		System.out.println("pref_list_length: " + pref_list_length );
//		System.out.println("n_pages: " + n_pages );
//		System.out.println("**************   INIT END  ****************");
	}
	
	
	
	public void startLoop() throws InvalidSlotNumberException, InvalidTupleSizeException, Exception {

		Heapfile hf = new Heapfile("heap_" + "AAA");
		temp = new Heapfile("sortFirstSkyTemp.in");
		
		BTFileScan scan = ((BTreeFile) index_file).new_scan(null, null);
		KeyDataEntry entry;
		RID rid;
		
		Tuple t = getEmptyTuple();
	    
		_window = new Tuple[(MINIBASE_PAGESIZE / t.size()) * n_pages];
		
		System.out.println("SIZE: " + (MINIBASE_PAGESIZE / t.size()) * n_pages);
	    entry = scan.get_next();
	    
	    int count = 0;
	    
	    while (entry != null && count < _window.length) {
	    	Tuple temp = getEmptyTuple();
        	rid = ((LeafData) entry.data).getData();
        	temp.tupleCopy(hf.getRecord(rid));
        	temp.print(attrType); 
        	
        	boolean isDominatedByWindow = checkDominationForWindowTuples(temp,count);
        	
        	if(!isDominatedByWindow) {
        		_window[count++] = temp;
        	}
        	
            entry = scan.get_next();
        }
	    
//	    System.out.println("In memory objects");
//        for(int i=0; i<_window.length; i++) {
//            if(_window[i] != null) {}
//                _window[i].print(attrType);
//        }
                   
        while (entry != null) {
            boolean isDominatedBy = false;
            
            Tuple htuple = getEmptyTuple();
            
            rid = ((LeafData) entry.data).getData();
            htuple.tupleCopy(hf.getRecord(rid));

            	
                for(int i=0; i<_window.length; i++){
                    if (TupleUtils.DominatesForCombinedTree(_window[i] , attrType, htuple, attrType, (short) attr_len, t1_str_sizes, pref_list, pref_list_length)) {
                    	isDominatedBy = true;
                        System.out.println("Heap tuple");
                        htuple.print(attrType);
                        System.out.println("Dominated by ");
                        _window[i].print(attrType);
                        break;
                    } 
                }

                if(!isDominatedBy){
                    try {
                        rid = temp.insertRecord(htuple.returnTupleByteArray());
                        
                    }
                    catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                } 
              entry = scan.get_next();
        }
        
        System.out.println("Window objects ");
        for(int i=0; i<_window.length; i++){
            if(_window[i] != null) {
                _window[i].print(attrType);
            }
        }
        
        System.out.println("HEAP ELEMENTS: " + temp.getRecCnt());
        System.out.println("WINDOW SIZE: " + _window.length);

        return;
    }
	
	
	private BTFileScan computeSkyline(BTFileScan scan, Tuple _window) {
		 
		
		
		
		return scan;
	}
	
	
	private boolean checkDominationForWindowTuples(Tuple temp, int count) throws TupleUtilsException, UnknowAttrType, FieldNumberOutOfBoundException, IOException {
		if(count == 0) return false;
		
		for(int i = 0; i < count; i++) {
			if (TupleUtils.DominatesForCombinedTree(_window[i] , attrType, temp, attrType, (short) attr_len, t1_str_sizes, pref_list, pref_list_length)) {
				return true;
			}
		}
		
		return false;
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