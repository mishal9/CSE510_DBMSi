package hashindex;

import java.io.IOException;

import btree.FloatKey;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import heap.Heapfile;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.Tuple;

public class HashIndexGeneratorUtil implements GlobalConst {

	public static void createUnclusteredHashIndex(String hashIndexName, String heapFileName, AttrType[] attrType, short[] t1_str_sizes,
			int keyNoForIndex) throws Exception {
		Scan scan = new Heapfile(heapFileName).openScan();
		
		HIndex hindex = new HIndex(hashIndexName, AttrType.attrReal, 100,80);
		RID rid = new RID();
		Tuple t = getEmptyTuple(attrType,t1_str_sizes);
		float key = 0;
		
		Tuple temp = scan.getNext(rid);
		
		while (temp != null) {
			t.tupleCopy(temp);

			key = t.getFloFld(keyNoForIndex);
			//t.print(attrType);

			hindex.insert(new HashKey(key), rid);

			temp = scan.getNext(rid);

		}
		hindex.close();
		scan.closescan();
		System.out.println("HashIndex: "+hashIndexName+" created !!");
		
	}
	
	private static Tuple getEmptyTuple(AttrType[] attrType, short[] t1_str_sizes) throws InvalidTypeException, InvalidTupleSizeException, IOException {
		Tuple t = new Tuple();
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		int size = t.size();
		t = new Tuple(size);
		t.setHdr((short) attrType.length, attrType, t1_str_sizes);
		return t;
	}

}
