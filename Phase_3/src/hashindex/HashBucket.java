package hashindex;

import java.io.IOException;

import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.DataPageInfo;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.HFPage;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;

public class HashBucket implements GlobalConst {

	String heapfileName;
	Heapfile heapfile;

	public HashBucket(String bucketName) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
		heapfileName = bucketName;
		heapfile = new Heapfile(heapfileName);
	}

	public void insertEntry(HashEntry entry) throws Exception {
		//getRecCnt();
		int entrySize = entry.size();
		byte[] byteArr = new byte[entrySize];
		entry.writeToByteArray(byteArr, 0);
		RID hashLocation = heapfile.insertRecord(byteArr);
	}

	public boolean deleteEntry(HashKey key)
			throws InvalidSlotNumberException, HFException, HFBufMgrException, HFDiskMgrException, Exception {
		return deleteEntry(new HashEntry(key, new RID()));
	}

	public int getRecCnt() throws Exception	{
		int numberOfPages = 0;
		int freeSpace = 0;
		int answer = 0;
		PageId currentDirPageId = SystemDefs.JavabaseDB.get_file_entry(heapfileName);

		PageId nextDirPageId = new PageId(0);

		HFPage currentDirPage = new HFPage();
		Page pageinbuffer = new Page();

		while (currentDirPageId.pid != INVALID_PAGE) {
			SystemDefs.JavabaseBM.pinPage(currentDirPageId, currentDirPage, false);
			
			RID rid = new RID();
			Tuple atuple;
			for (rid = currentDirPage.firstRecord(); rid != null; // rid==NULL means no more record
					rid = currentDirPage.nextRecord(rid)) {
				atuple = currentDirPage.getRecord(rid);
				DataPageInfo dpinfo = new DataPageInfo(atuple);
				
				answer += dpinfo.recct;
				freeSpace+=dpinfo.availspace;
				numberOfPages++;
			}
			
			//currentDirPage.dumpPage();
			// ASSERTIONS: no more record
			// - we have read all datapage records on
			// the current directory page.

			nextDirPageId = currentDirPage.getNextPage();
			SystemDefs.JavabaseBM.unpinPage(currentDirPageId, false /* undirty */);
			currentDirPageId.pid = nextDirPageId.pid;
		}

		System.out.println("recCount:"+answer+" numberOfPages:"+numberOfPages+" freespace:" +freeSpace);
		return answer;
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
			System.out.println("rid: " + rid);
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
		System.out.println("foundLocation: " + foundLocation);
		heapfile.deleteRecord(foundLocation);
		// TODO check if heapfile can be compacted, ie all current records can fit in 1
		// page
		int numberOfRecordsInBucket = heapfile.getRecCnt();

		return true;

	}

	public void printToConsole() throws InvalidTupleSizeException, IOException {
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
		System.out.println("] count: "+count);
		scan.closescan();
	}

}
