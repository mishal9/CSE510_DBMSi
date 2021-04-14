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

	    Tuple t = new Tuple();
	    
	    //heap_AAA1
	    
	    FldSpec []  proj1 = {
	    	       new FldSpec(new RelSpec(RelSpec.outer), 1),
	    	       new FldSpec(new RelSpec(RelSpec.outer), 2),
	    	       new FldSpec(new RelSpec(RelSpec.innerRel), 1),
	    	       new FldSpec(new RelSpec(RelSpec.innerRel), 2)
	    };
	    
	     CondExpr [] outFilter = new CondExpr[2];
		    outFilter[0] = new CondExpr();
		    outFilter[1] = new CondExpr();
	     
	    
		    outFilter[0].next  = null;
		    outFilter[0].op    = new AttrOperator(AttrOperator.aopEQ);
		    outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		    outFilter[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer), 1);
		    outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		    outFilter[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
		    outFilter[1] = null;
	    
	    NestedLoopsJoins nlj = null;
	    try {
	      nlj = new NestedLoopsJoins (in1, 2, null,
	    		  in2, 2, null,
					  10,
					  am, "heap_AAA2",
					  outFilter, null, proj1, 4);
	    }
	    catch (Exception e) {
	      System.err.println ("*** Error preparing for nested_loop_join");
	    }
	    
	    AttrType[] temp = new AttrType[4];
	    temp[0] = new AttrType (AttrType.attrReal);
	    temp[1] = new AttrType (AttrType.attrReal);
	    temp[2] = new AttrType (AttrType.attrReal);
	    temp[3] = new AttrType (AttrType.attrReal);
	    
	    nlj.get_next().print(temp);
	    nlj.get_next().print(temp);
	    nlj.get_next().print(temp);
	    
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
