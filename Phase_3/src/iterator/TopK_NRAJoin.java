package iterator;

import heap.*;
import index.IndexException;
import index.IndexScan;
import iterator.Iterator;
import global.*;
import bufmgr.*;
import clustered_btree.ClusteredBTreeFile;

import java.lang.*;
import java.io.*;
import java.util.*;

import btree.*;

class TupleComparatorNRA implements Comparator<Tuple>{

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
	
	boolean firstEntry = true;
	
    HashMap<String, NRABounds> map = new HashMap<>();
    PriorityQueue<Tuple> pq = null;
    
    IndexScan scan = null;
    
    public AttrType joinAttrType[];
    public short[] joinAttrSize;
    
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
		
		pq = new PriorityQueue<Tuple>(new TupleComparatorNRA());
	}
	
	
	public void calculateTopKJoins() throws Exception {
		
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
		
		AttrType[] table1_attr = table1.getTable_attr_type();
		int table1_len = table1.getTable_attr_type().length;
	    short[] table1_attr_size = table1.getTable_attr_size();
		
		AttrType[] table2_attr = table2.getTable_attr_type();
		int table2_len = table2.getTable_attr_type().length;
	    short[] table2_attr_size = table2.getTable_attr_size();
	    
	    FldSpec[] projlist1 = new FldSpec[table1_len];
		RelSpec rel1 = new RelSpec(RelSpec.outer);
		
	    for (int i=0; i<table1_len; i++ ) {
			projlist1[i] = new FldSpec(rel1, i+1);
		}
	    
	    FldSpec[] projlist2 = new FldSpec[table2_len];
		RelSpec rel2 = new RelSpec(RelSpec.outer);
		
	    for (int i=0; i<table2_len; i++ ) {
			projlist2[i] = new FldSpec(rel2, i+1);
		}
		
		IndexScan iscan1 = new IndexScan(new IndexType(IndexType.Cl_B_Index_DESC), 
					  this.relationName1, 
					  table1.get_clustered_index_filename(this.mergeAttr1.offset, "btree"), 
					  table1_attr, 
					  table1_attr_size, 
					  table1.getTable_num_attr(), 
					  table1.getTable_num_attr(), 
					  projlist1, 
					  null,
					  table1.getTable_num_attr(), 
					  false);
		
		IndexScan iscan2 = new IndexScan(new IndexType(IndexType.Cl_B_Index_DESC), 
		  this.relationName2, 
		  table2.get_clustered_index_filename(this.mergeAttr2.offset, "btree"), 
		  table2_attr, 
		  table2_attr_size, 
		  table2.getTable_num_attr(), 
		  table2.getTable_num_attr(), 
		  projlist2, 
		  null,
		  table2.getTable_num_attr(), 
		  false);
		
		Tuple temp1 = iscan1.get_next();
		Tuple temp2 = iscan2.get_next();
 
        int objectsSeen = 0;
    	float currDepthScore = 0.0f;
    	float minLowerBound = 0.0f;
    	
    	int depth = 0;
    	
    	int newLength = table1_len + table2_len;
		joinAttrType = new AttrType[newLength];
		joinAttrSize = new short[newLength];

		int count = 0;
		for(int i = 0; i < table1_len; i++) {
			joinAttrType[count] = table1_attr[i];
			joinAttrSize[count] = table1_attr_size[i];
			count++;
		}
		for(int i = 0; i < table2_len; i++) {
			if(i+1 == joinAttr2.offset) continue;
			joinAttrType[count] = table2_attr[i];
			joinAttrSize[count] = table2_attr_size[i];
			count++;
		}
		
		joinAttrType[count] = new AttrType(AttrType.attrReal);
		joinAttrSize[count] = STRSIZE;

    	
    	while(true) {
    		depth += 1;
    		
    		if(temp1 == null || temp2 == null) break;
    		
    		int join1 = temp1.getIntFld(joinAttr1.offset);
    		int join2 = temp2.getIntFld(joinAttr2.offset);
    		
    		int merge1 = temp1.getIntFld(mergeAttr1.offset);
    		int merge2 = temp2.getIntFld(mergeAttr2.offset);
    		
    		currDepthScore = merge1 + merge2;
    		
//    		System.out.println("***********************************");
//    		System.out.println("objectsSeen: " + objectsSeen);
//    		System.out.println("currDepthScore: " + currDepthScore);
//    		System.out.println("minLowerBound: " + minLowerBound);
//        	System.out.println("***********************************");
        	
    		if(objectsSeen >= k && currDepthScore < minLowerBound) break;
    		
    		
    		String joinKey1 = "" + join1;
    		if(map.containsKey(joinKey1)) {
    			NRABounds temp = map.get(joinKey1);
    			if(temp.createBy == "REL2") {
    				temp.t2 = temp1;
//    				System.out.println("These tuples will be merged");
//    				temp.t1.print(table1_attr);
//    				temp.t2.print(table2_attr);
//    				System.out.println("************ END *************");
    				
    				Tuple mergedTuple = new Tuple();
    				mergedTuple.setHdr((short) newLength, joinAttrType, joinAttrSize);
    		    	int newSize = mergedTuple.size();
    		    	mergedTuple = new Tuple(newSize);
    		    	mergedTuple.setHdr((short) newLength, joinAttrType, joinAttrSize);
    		    	
    		    	count = 1;
    				for(int i = 0; i < table1_len; i++) {
    					if(table1_attr[i].attrType == AttrType.attrInteger) {
    						mergedTuple.setIntFld(count, temp.t1.getIntFld(i+1));
    					}
    					else if(table1_attr[i].attrType == AttrType.attrReal) {
    						mergedTuple.setFloFld(count, temp.t1.getFloFld(i+1));
    					}
    					else if(table1_attr[i].attrType == AttrType.attrString) {
    						mergedTuple.setStrFld(count, temp.t1.getStrFld(i+1));
						}
    					count++;
    				}
    				for(int i = 0; i < table2_len; i++) {
    					if(i+1 == joinAttr2.offset) continue;
    					if(table1_attr[i].attrType == AttrType.attrInteger) {
    						mergedTuple.setIntFld(count, temp.t2.getIntFld(i+1));
    					}
    					else if(table1_attr[i].attrType == AttrType.attrReal) {
    						mergedTuple.setFloFld(count, temp.t2.getFloFld(i+1));
    					}
    					else if(table1_attr[i].attrType == AttrType.attrString) {
    						mergedTuple.setStrFld(count, temp.t2.getStrFld(i+1));
						}
    					count++;
    				}
    				mergedTuple.setFloFld(count, (float) ( temp.t1.getIntFld(mergeAttr1.offset) + 
    						temp.t2.getIntFld( mergeAttr2.offset)) / (float)2.0 );
    				pq.add(mergedTuple);
    			}

    		}
    		else {
    			NRABounds nb1 = new NRABounds(merge1, "REL1");
    			nb1.t1 = temp1;
    			objectsSeen+=1;
    			map.put(joinKey1, nb1);
    		}
    		
    		
    		String joinKey2 = "" + join2;
    		if(map.containsKey(joinKey2)) {
    			NRABounds temp = map.get(joinKey2);
    			if(temp.createBy == "REL1") {
    				temp.t2 = temp2;
    				
    				Tuple mergedTuple = new Tuple();
    				mergedTuple.setHdr((short) newLength, joinAttrType, joinAttrSize);
    		    	int newSize = mergedTuple.size();
    		    	mergedTuple = new Tuple(newSize);
    		    	mergedTuple.setHdr((short) newLength, joinAttrType, joinAttrSize);
    		    	
    		    	count = 1;
    				for(int i = 0; i < table1_len; i++) {
    					if(table1_attr[i].attrType == AttrType.attrInteger) {
    						mergedTuple.setIntFld(count, temp.t1.getIntFld(i+1));
    					}
    					else if(table1_attr[i].attrType == AttrType.attrReal) {
    						mergedTuple.setFloFld(count, temp.t1.getFloFld(i+1));
    					}
    					else if(table1_attr[i].attrType == AttrType.attrString) {
    						mergedTuple.setStrFld(count, temp.t1.getStrFld(i+1));
						}
    					count++;
    				}
    				for(int i = 0; i < table2_len; i++) {
    					if(i+1 == joinAttr2.offset) continue;
    					if(table1_attr[i].attrType == AttrType.attrInteger) {
    						mergedTuple.setIntFld(count, temp.t2.getIntFld(i+1));
    					}
    					else if(table1_attr[i].attrType == AttrType.attrReal) {
    						mergedTuple.setFloFld(count, temp.t2.getFloFld(i+1));
    					}
    					else if(table1_attr[i].attrType == AttrType.attrString) {
    						mergedTuple.setStrFld(count, temp.t2.getStrFld(i+1));
						}
    					count++;
    				}
    				mergedTuple.setFloFld(count, (float) ( temp.t1.getIntFld(mergeAttr1.offset) + 
    						temp.t2.getIntFld( mergeAttr2.offset)) / (float)2.0 );
    				pq.add(mergedTuple);
    			}
    		}
    		else {
    			NRABounds nb2 = new NRABounds(merge2, "REL2");
    			nb2.t1 = temp2;
    			objectsSeen+=1;
    			map.put(joinKey2, nb2);
    		}
    		
    		temp1 = iscan1.get_next();
    		temp2 = iscan2.get_next();
    	}
    	
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
		while(k > 0) {
			pq.poll().print(joinAttrType);
			k--;
    	}
		
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

/*String key1 = "" + join1;
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
}*/