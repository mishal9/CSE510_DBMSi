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

class TupleComparator implements Comparator<Tuple>{

	@Override
	public int compare(Tuple o1, Tuple o2) {
		try {
			if (o1.getFloFld( o1.noOfFlds() ) < o2.getFloFld( o2.noOfFlds() ) )
			  return 1;
			else if (o1.getFloFld( o1.noOfFlds() ) > o2.getFloFld( o2.noOfFlds() ))
			  return -1;
			else return 0;
		} catch (FieldNumberOutOfBoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
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
	
	PriorityQueue<Tuple> pq = null;
	HashJoin hj = null;
	
	AttrType[] newAttrType = null;

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
	    	    
	    AttrType[] Rtypes = new AttrType[2];
        Rtypes[0] = new AttrType(AttrType.attrInteger);
        Rtypes[1] = new AttrType(AttrType.attrInteger);

        short[] Rsizes = new short[2];
        Rsizes[0] = 15;
        Rsizes[1] = 15;
	    
	    Heapfile f = new Heapfile(this.relationName2+".txt");
	    
	    Tuple t = new Tuple();
	    t.setHdr((short) len_in2, Rtypes, Rsizes);
	    int size = t.size();
	    
	    Test[] test = {
	    		new Test(4,4),
	    		new Test(1,6),
	    		new Test(6,9),
	    		new Test(3,1),
	    };
	    
	    t = new Tuple(size);
	    t.setHdr((short) len_in2, Rtypes, Rsizes);
	    
	    for(int i = 0; i< test.length; i++) {
	    	t.setIntFld(1, test[i].first);
	    	t.setIntFld(2, test[i].second);
            RID rid = f.insertRecord(t.returnTupleByteArray());
	    }
	    
	    hj = new HashJoin(
    		  table1.getTable_attr_type(), table1.getTable_attr_type().length, table1.getTable_attr_size(),
    		  table1.getTable_attr_type(), table1.getTable_attr_type().length, table1.getTable_attr_size(),
    		  100,
    		  am, this.relationName2+".txt",
    		  outFilter, null, proj1, 4);
	    
	    int newLength = table1.getTable_attr_type().length + len_in2 + 1;
    	newAttrType = new AttrType[newLength];
        short[] newAttrSize = new short[newLength];
        
        int pointer = 0;
        
        for(int i = 0; i < table1.getTable_attr_type().length; i++) {
        	newAttrType[pointer] = table1.getTable_attr_type()[i];
        	newAttrSize[pointer] = table1.getTable_attr_size()[i];
        	pointer++;
        }
        for(int i = 0; i < len_in2; i++) {
        	newAttrType[pointer] = in2[i];
        	newAttrSize[pointer] = t2_str_sizes[i];
        	pointer++;
        }
        
        newAttrType[pointer] = new AttrType(AttrType.attrReal); 
    	newAttrSize[pointer] = 32;
    	
	    t = hj.get_next();
	    
	    pq = new 
                PriorityQueue<Tuple>(new TupleComparator());
	    
	    while(t != null) { 

	    	Tuple newTuple = new Tuple();
	    	newTuple.setHdr((short) newLength, newAttrType, newAttrSize);
	    	int newSize = newTuple.size();
	    	newTuple = new Tuple(newSize);
	    	newTuple.setHdr((short) newLength, newAttrType, newAttrSize);
	    	
	    	int curr = 1;
	    	for(int i = 1; i < newLength; i++) {
	    		newTuple.setIntFld(curr, t.getIntFld(i));
	    		curr++;
	        }
	    	newTuple.setFloFld(curr, (float) ( t.getIntFld(mergeAttr1.offset) + 
	    			 t.getIntFld( table1.getTable_attr_type().length + mergeAttr2.offset)) / (float) 2.0
	    	);
	    	
	    	pq.add(newTuple);
	    	
	    	t = hj.get_next();
	    }
	    
	}

	@Override
	public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException,
			InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException,
			LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {		
		
		if(k > 0) {
			Tuple t = pq.poll();
			k--;
			return t;
		}
		else {
			return null;
		}
	}

	@Override
	public void close() throws IOException, JoinsException, SortException, IndexException {
		// TODO Auto-generated method stub
	}

}