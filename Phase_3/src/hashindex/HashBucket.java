package hashindex;

import java.io.IOException;

import global.GlobalConst;
import global.PageId;
import global.RID;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.Scan;
import heap.Tuple;

public class HashBucket implements GlobalConst {

	String heapfileName;
	Heapfile heapfile;

	public HashBucket(String bucketName) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
		heapfileName = bucketName;
		heapfile = new Heapfile(heapfileName);
	}

	public void insertEntry(HashEntry entry) throws Exception {
		int entrySize = entry.size();
		byte[] byteArr = new byte[entrySize];
		entry.writeToByteArray(byteArr, 0);
		RID hashLocation = heapfile.insertRecord(byteArr);
	}

	public boolean deleteEntry(HashKey key)
			throws InvalidSlotNumberException, HFException, HFBufMgrException, HFDiskMgrException, Exception {
		return deleteEntry(new HashEntry(key, new RID()));
	}

	public boolean deleteEntry(HashEntry entryToDelete)
			throws InvalidSlotNumberException, HFException, HFBufMgrException, HFDiskMgrException, Exception {
		RID foundLocation = null;
		Scan scan = heapfile.openScan();
		RID rid = new RID();
		Tuple tup;
		boolean done = false;
		int i = 0;
		while (!done) {
			tup = scan.getNext(rid);

			if (tup == null) {
				done = true;
				break;
			}
			
			HashEntry scannedHashEntry = new HashEntry(tup.returnTupleByteArray(), 0);
			// System.out.println(i+" scannedRid: "+scannedRID);
			if (scannedHashEntry.key.equals(entryToDelete.key)) {
				done = true;
				foundLocation = new RID(new PageId(rid.pageNo.pid), rid.slotNo);
				break;
			}
			i++;
		}
		scan.closescan();
		if (foundLocation == null) {
			System.out.println("Not found in hashbucket");
			return false;
		}
		HashUtils.log("foundLocation: " + foundLocation);
		heapfile.deleteRecord(foundLocation);
		// TODO check if heapfile can be compacted, ie all current records can fit in 1
		// page
		//int numberOfRecordsInBucket = heapfile.getRecCnt();

		return true;

	}

	public void printToConsole() throws Exception {
		Scan scan = heapfile.openScan();
		RID rid = new RID();
		Tuple tup;
		int count = 0;
		boolean done = false;
		System.out.println("HashBucket [ ");
		while (!done) {
			tup = scan.getNext(rid);
			if (tup == null) {
				done = true;
				break;
			}
			HashEntry scannedHashEntry = new HashEntry(tup.returnTupleByteArray(), 0);
			System.out.println("  " + scannedHashEntry + " @ " + rid);
			count++;
		}
		System.out.println("] count: " + count);
		scan.closescan();
	}

	@Override
	public String toString() {
		return "HashBucket [heapfileName=" + heapfileName + "]";
	}

}
