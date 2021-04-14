package iterator;

import heap.*;
import index.IndexException;
import index.IndexScan;
import index.UnknownIndexTypeException;
import iterator.Iterator;
import global.*;
import bufmgr.*;
import btree.*;

import java.lang.*;
import java.io.*;
import java.util.*;

public class TopK_HashJoin extends Iterator implements GlobalConst {
	
	AttrType[] in1; 
	int len_in1;
	short[] t1_str_sizes;
	FldSpec joinAttr1;
	FldSpec mergeAttr1;
	AttrType[] in2;
	int len_in2;
	short[] t2_str_sizes;
	FldSpec joinAttr2;
	FldSpec mergeAttr2;
	java.lang.String relationName1;
	java.lang.String relationName2;
	int k;
	int n_pages;

	public TopK_HashJoin(
			AttrType[] in1, int len_in1, short[] t1_str_sizes,
			FldSpec joinAttr1,
			FldSpec mergeAttr1,
			AttrType[] in2, int len_in2, short[] t2_str_sizes,
			FldSpec joinAttr2,
			FldSpec mergeAttr2,
			java.lang.String relationName1,
			java.lang.String relationName2,
			int k,
			int n_pages
			) throws JoinsException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		
		this.in1 = in1; 
		this.len_in1 = len_in1;
		this.t1_str_sizes = t1_str_sizes;
		this.joinAttr1 = joinAttr1;
		this.mergeAttr1 = mergeAttr1;
		this.in2 = in2;
		this.len_in2 = len_in2;
		this.t2_str_sizes = t2_str_sizes;
		this.joinAttr2 = joinAttr2;
		this.mergeAttr2 = mergeAttr2;
		this.relationName1 = relationName1;
		this.relationName2 = relationName2;
		this.k = k;
		this.n_pages = n_pages;
		
	    IndexType b_index = new IndexType (IndexType.B_Index);
	    
	    FldSpec [] Sprojection = {
	    	       new FldSpec(new RelSpec(RelSpec.outer), 1),
	    	       new FldSpec(new RelSpec(RelSpec.outer), 2),
	    };
	    
	    System.out.println("relationName1: "+ relationName1);
	    iterator.Iterator am = new IndexScan ( b_index, relationName1,
	    		"AAA1", in1, null, 2, 2,
				   Sprojection, null, 2, false);

	    System.out.println(am.get_next());
	    System.out.println(am.get_next());
	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
		
	}

}
