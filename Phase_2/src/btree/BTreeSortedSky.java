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

public class BTreeSortedSky {
	
	private String relationName;
	private AttrType[] attrType;
	private int len_in1;
	private short[] t1_str_sizes;
	private Iterator iterator1;
	private int[] pref_list;
	private int pref_list_length;
	private int n_pages;
	
	private BTreeFile[] btreeindexes;
	private int attr_nos;
	private int amt_of_mem;


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
	public BTreeSortedSky(AttrType[] in1, int len_in1, short[] t1_str_sizes, int amt_of_mem, Iterator am1,
			String relationName, int[] pref_list, int pref_list_length, IndexFile[] index_file_list,
			int n_pages) throws Exception {


		this.relationName = relationName;
		this.btreeindexes = (BTreeFile[]) index_file_list;

		this.attrType = in1;
		this.attr_nos = len_in1;
		this.t1_str_sizes = t1_str_sizes;
		this.amt_of_mem = amt_of_mem;
		this.iterator1 = am1;
		this.pref_list = pref_list;
		this.pref_list_length = pref_list_length;
		this.n_pages = n_pages; 

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