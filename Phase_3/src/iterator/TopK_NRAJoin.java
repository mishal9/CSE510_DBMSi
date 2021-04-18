package iterator;

import heap.*;
import index.IndexException;
import index.IndexScan;
import iterator.Iterator;
import global.*;
import bufmgr.*;

import java.lang.*;
import java.io.*;
import java.util.*;

import btree.*;

public class TopK_NRAJoin extends Iterator implements GlobalConst {
	
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
	
    HashMap<String, NRABounds> map = new HashMap<>();

	
	public TopK_NRAJoin(
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
	) {
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
		
	}
	
	
	public void calculateTopKJoins() throws Exception {
//		Heapfile hf1 = new Heapfile(this.relationName1);
//		Heapfile hf2 = new Heapfile(this.relationName2);
		
		Table table1 = SystemDefs.JavabaseDB.get_relation(this.relationName1);
		Table table2 = SystemDefs.JavabaseDB.get_relation(this.relationName2);

		if(table1.clustered_index_exist(this.mergeAttr1.offset, this.relationName1)) {
			System.out.println("Relation 1 doesnot have clustered index");
			return;
		}
		if(table2.clustered_index_exist(this.mergeAttr1.offset, this.relationName2)) {
			System.out.println("Relation 2 doesnot have clustered index");
			return;
		}
		
		FldSpec[] projlist = new FldSpec[this.len_in1];
		RelSpec rel = new RelSpec(RelSpec.outer);
		
		for (int i=0; i<this.len_in1; i++ ) {
			projlist[i] = new FldSpec(rel, i+1);
		}
				
		IndexScan iscan1 = new IndexScan(new IndexType(IndexType.Cl_B_Index_DESC), 
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
		
		IndexScan iscan2 = new IndexScan(new IndexType(IndexType.Cl_B_Index_DESC), 
				  this.relationName1, 
				  table2.get_clustered_index_filename(this.mergeAttr2.offset, "btree"), 
				  table2.getTable_attr_type(), 
				  table2.getTable_attr_size(), 
				  table2.getTable_num_attr(), 
				  table2.getTable_num_attr(), 
				  projlist, 
				  null,
				  table2.getTable_num_attr(), 
				  false);
		
		Tuple temp1 = iscan1.get_next();
		Tuple temp2 = iscan2.get_next();
 
        int objectsSeen = 0;
    	float currDepthScore = 0.0f;
    	float minLowerBound = 0.0f;
    	
    	int depth = 0;
    	
    	while(true) {
    		depth += 1;
    		
    		if(temp1 == null || temp2 == null) break;
    		
    		int join1 = temp1.getIntFld(joinAttr1.offset);
    		int join2 = temp2.getIntFld(joinAttr2.offset);
    		
    		int merge1 = temp1.getIntFld(mergeAttr1.offset);
    		int merge2 = temp2.getIntFld(mergeAttr2.offset);
    		
    		currDepthScore = merge1 + merge2;
    		
    		System.out.println("***********************************");
    		System.out.println("objectsSeen: " + objectsSeen);
    		System.out.println("currDepthScore: " + currDepthScore);
    		System.out.println("minLowerBound: " + minLowerBound);
        	System.out.println("***********************************");
        	
    		if(objectsSeen >= k && currDepthScore < minLowerBound) break;
    		
    		String key1 = "" + join1;
    		if(map.containsKey(key1)) {
    			NRABounds temp = map.get(key1);
    			if(temp.createBy == "REL2") {
    				temp.updateBounds(merge1);
    				minLowerBound = getMinLowerBound();
    			}
    			else {
    			}
    		}
    		else {
    			NRABounds nbound = new NRABounds(merge1, "REL1");
    			map.put(key1, nbound);
				minLowerBound = getMinLowerBound();
        		objectsSeen += 1;
    		}
    		
    		String key2 = "" + join2;
    		if(map.containsKey(key2)) {
    			NRABounds temp = map.get(key2);
    			if(temp.createBy == "REL1") {
    				temp.updateBounds(merge2);
    				minLowerBound = getMinLowerBound();

    			}
    			else {
    			}
    		}
    		else {
    			NRABounds nbound = new NRABounds(merge2, "REL2");
    			map.put(key2, nbound);
        		objectsSeen += 1;
				minLowerBound = getMinLowerBound();
    		}
    		
    		temp1 = iscan1.get_next();
    		temp2 = iscan2.get_next();
    	}
    	
    	System.out.println("===================================");
    	
    	PriorityQueue<NRABounds> pq = new 
                PriorityQueue<NRABounds>(k, new NRABoundsComparator());
    	
    	for (Map.Entry<String,NRABounds> entry : map.entrySet()) {
    		pq.add(entry.getValue());
        }
    	
    	while(k > 0) {
    		System.out.println(pq.poll().toString());
    		k--;
    	}
//    	
//    	System.out.println(pq.poll().toString());

	}
	
	private float getMinLowerBound() {
		float min = Float.MAX_VALUE;
		
		for (Map.Entry<String,NRABounds> entry : map.entrySet()) {
			if(entry.getValue().getLowerBoundVal() < min) {
				min = entry.getValue().getLowerBoundVal();
			}
		}
		
		return min;
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


	@Override
	public List<Tuple> get_next_aggr() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}