package iterator;

import heap.*;
import index.*;
import iterator.Iterator;
//import tests.Reserves;
import global.*;
import bufmgr.*;
import btree.*;

import java.lang.*;
import java.io.*;
import java.util.*;

class Test {
    public int first;
    public int second;
    
    public Test(int val1, int val2) {
    	this.first = val1;
    	this.second = val2;
    }
}

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
			    
	    Table table1 = SystemDefs.JavabaseDB.get_relation(this.relationName1);
//		Table table2 = SystemDefs.JavabaseDB.get_relation(this.relationName2);

	    FldSpec [] Sprojection = {
	    	       new FldSpec(new RelSpec(RelSpec.outer), 1),
	    	       new FldSpec(new RelSpec(RelSpec.outer), 2),
	    };
	    
	    FldSpec[] projlist = new FldSpec[this.len_in1];
		RelSpec rel = new RelSpec(RelSpec.outer);
		
		for (int i=0; i<this.len_in1; i++ ) {
			projlist[i] = new FldSpec(rel, i+1);
		}
		

	    IndexScan am = new IndexScan(new IndexType(IndexType.Cl_B_Index_DESC), 
				  this.relationName1, 
				  table1.get_clustered_index_filename(this.mergeAttr1.offset, "btree"), 
				  table1.getTable_attr_type(), 
				  table1.getTable_attr_size(), 
				  table1.getTable_num_attr(), 
				  table1.getTable_num_attr(), 
				  projlist, 
				  null,
				  table1.getTable_num_attr(), 
				  false);
	    
//	    Tuple t = am.get_next();
//	    
//	    while(t!= null) {
//	    	t.print(table1.getTable_attr_type());
//	    	t = am.get_next();
//	    }
	    
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
	    
	    System.out.println("" + this.relationName2);
	    
	    AttrType[] Rtypes = new AttrType[2];
        Rtypes[0] = new AttrType(AttrType.attrInteger);
        Rtypes[1] = new AttrType(AttrType.attrInteger);

        short[] Rsizes = new short[2];
        Rsizes[0] = 15;
        Rsizes[1] = 15;
	    
	    Heapfile f = new Heapfile(this.relationName2+".txt");
	    
	    Tuple t = new Tuple();
	    t.setHdr((short) 2, Rtypes, Rsizes);
	    int size = t.size();
	    
	    Test[] test = {
	    		new Test(4,4),
	    		new Test(1,6),
	    		new Test(6,9),
	    		new Test(3,1),
	    };
	    
	    t = new Tuple(size);
	    t.setHdr((short) 2, Rtypes, Rsizes);
	    
	    for(int i = 0; i< test.length; i++) {
	    	t.setIntFld(1, test[i].first);
	    	t.setIntFld(2, test[i].second);
            RID rid = f.insertRecord(t.returnTupleByteArray());
	    }
	    
	    HashJoin nlj = null;
	    
	    nlj = new HashJoin(
    		  table1.getTable_attr_type(), table1.getTable_attr_type().length, table1.getTable_attr_size(),
    		  table1.getTable_attr_type(), table1.getTable_attr_type().length, table1.getTable_attr_size(),
    		  100,
    		  am, this.relationName2+".txt",
    		  outFilter, null, proj1, 4);
	    
	    
	    AttrType[] temp = new AttrType[4];
	    temp[0] = new AttrType (AttrType.attrInteger);
	    temp[1] = new AttrType (AttrType.attrInteger);
	    temp[2] = new AttrType (AttrType.attrInteger);
	    temp[3] = new AttrType (AttrType.attrInteger);
	    
	    System.out.println("============================");
	    nlj.get_next().print(temp);
	    nlj.get_next().print(temp);
	    nlj.get_next().print(temp);
//	    nlj.get_next().print(temp);
	    System.out.println("============================");
	    
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