package iterator;

import heap.*;
import index.IndexException;
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
		Heapfile hf1 = new Heapfile(this.relationName1);
		Heapfile hf2 = new Heapfile(this.relationName2);
		
        BTFileScan scan1 = null;
        BTFileScan scan2 = null;
        
        KeyDataEntry entry1;
        KeyDataEntry entry2;

        RID rid1 = new RID();
        RID rid2 = new RID();
        
        int [] pref_list = {0,1};
        
        GenerateIndexFiles obj = new GenerateIndexFiles();
        IndexFile indexFile1 = obj.createCombinedBTreeIndex("/Users/kunjpatel/Desktop/CSE510_DBMSi/Phase_3/data/nra1.txt",pref_list, 2);
        IndexFile indexFile2 = obj.createCombinedBTreeIndex("/Users/kunjpatel/Desktop/CSE510_DBMSi/Phase_3/data/nra2.txt",pref_list, 2);
        
        short [] Ssizes = null;
        
        scan1 = ((BTreeFile) indexFile1).new_scan(null, null);
        scan2 = ((BTreeFile) indexFile2).new_scan(null, null);
        
        
        //get tuple size
        Tuple t = new Tuple();
        t.setHdr((short)2, in1, Ssizes);            
        int size = t.size();
        
        
        // Generate 2 tuples based on size
        Tuple t1 = new Tuple(size);
        t1.setHdr((short)2, in1, Ssizes);

        Tuple t2 = new Tuple(size);
        t2.setHdr((short)2, in2, Ssizes);

        boolean rel1Ended = false;
        boolean rel2Ended = false;
        
    	int objectsSeen = 0;
    	float currDepthScore = 0.0f;
    	float minLowerBound = 0.0f;
    	
    	int depth = 0;
    	
    	while(true) {
    		depth += 1;
    		
    		entry1 = scan1.get_next();
    		entry2 = scan2.get_next();
          
    		if(entry1 == null || entry2 == null) break;
    		
    		rid1 = ((LeafData) entry1.data).getData();
    		rid2 = ((LeafData) entry2.data).getData();
    		
    		t1.tupleCopy(hf1.getRecord(rid1));
    		t2.tupleCopy(hf2.getRecord(rid2));
    		
    		float join1 = t1.getFloFld(joinAttr1.offset);
    		float join2 = t2.getFloFld(joinAttr2.offset);
    		
    		float merge1 = t1.getFloFld(mergeAttr1.offset);
    		float merge2 = t2.getFloFld(mergeAttr2.offset);
    		
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
    	}
    	
    	System.out.println("===================================");
    	
    	PriorityQueue<NRABounds> pq = new 
                PriorityQueue<NRABounds>(k, new TupleComparator());
    	
    	for (Map.Entry<String,NRABounds> entry : map.entrySet()) {
    		pq.add(entry.getValue());
        }
    	
    	System.out.println(pq.poll().toString());
    	System.out.println(pq.poll().toString());

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

}