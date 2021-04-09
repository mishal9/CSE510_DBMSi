package hashindex;

import global.GlobalConst;
import global.PageId;
import global.RID;
import heap.HashClustDataFile;
import heap.Scan;
import heap.Tuple;

public class ClusHIndex implements GlobalConst{
	
	HashBucket bucket;
	HashClustDataFile dataFile;
	public ClusHIndex() throws Exception {
		bucket=new HashBucket("asldhalskdaslk0");
		dataFile = new HashClustDataFile("cldf-asldhalskdaslk");
	}
	
	public void insert(HashKey key,Tuple tup) throws Exception {
		int pageNumber = checkIfKeyAlreadyInBucket(key,bucket);
		if(pageNumber == -1) { //new key
			//dataFile.in
		} else { //duplicate key
			RID rid = new RID(new PageId(pageNumber), 12);
			HashEntry entry = new HashEntry(key, rid );
			dataFile.insertRecordOnExistingPage(tup.getTupleByteArray(), new PageId(pageNumber));
			bucket.insertEntry(entry);
		}
		
		
	}
	
	private int checkIfKeyAlreadyInBucket(HashKey key,HashBucket buc) throws Exception {
		int pageNum = -1;
		Scan scan = buc.heapfile.openScan();
		RID rid = new RID();
		Tuple tup;
		boolean done = false;
		while (!done) {
			tup = scan.getNext(rid);

			if (tup == null) {
				done = true;
				break;
			}
			HashEntry scannedHashEntry = new HashEntry(tup.returnTupleByteArray(), 0);
			if(scannedHashEntry.key.equals(key)) {
				pageNum = scannedHashEntry.rid.pageNo.pid;
				HashUtils.log("Key is already in bucket with page pointer: "+pageNum);
				done = true;
				break;
			}
		}
		scan.closescan();
		return pageNum;
	}

}
