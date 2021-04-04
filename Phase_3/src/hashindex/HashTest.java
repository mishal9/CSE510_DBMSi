package hashindex;

import java.io.IOException;

import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Scan;

public class HashTest implements GlobalConst {

	public static void main(String[] args) {
		
		System.out.println("Start");
		
		HashTest thiss = new HashTest();
		try {
			thiss.testHIndex();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("End");
	}
	
	
	private void testHIndex() throws Exception {
		HIndex h = new HIndex("whatever", 1, 12);
		for (int i = 10; i < 30; i++) {
			HashKey key = new HashKey(i);
			RID rid = new RID(new PageId(i),i);
			h.insert(key, rid);
		}
		
		HashBucket bucket = new HashBucket(h.headerPage.get_NthBucketName(0));
		bucket.printToConsole();
		
		h.delete(new HashKey(13));
		bucket.printToConsole();
		h.close();
	}
	
	
	private void testHHeaderPage() throws Exception {
		HIndex h = new HIndex("whatever", 1, 12);
		printPinnedPages();
		String bucketStart = h.headerPage.get_HashIndexName();
		System.out.println("bucket start: "+bucketStart);
		System.out.println("clsing hindex...");
		h.close();
		h = null;
		h = new HIndex("whatever", 1, 12);
		h.close();
		
	}

	public HashTest() {
		long time=System.currentTimeMillis();
		time=10;
		String dbpath = "HASHTEST" + time + ".minibase-db";
		//SystemDefs.MINIBASE_RESTART_FLAG=true;
		SystemDefs sysdef = new SystemDefs(dbpath, 5000, 100, "Clock");

	}


	private void testHashBucket() throws Exception {

		printPinnedPages();
		HashBucket bucket = new HashBucket("SomeNam");
		
		for (int i = 10; i < 30; i++) {
			HashKey key = new HashKey(i);
			RID rid = new RID(new PageId(i),i);
			HashEntry ent = new HashEntry(key, rid);
			bucket.insertEntry(ent);

		}
		bucket.printToConsole();
		HashEntry entryToDelete  = new HashEntry(new HashKey(13), new RID(new PageId(13),13));
		bucket.deleteEntry(entryToDelete);
		bucket.printToConsole();
		printPinnedPages();
		//46

	}
	
	private void testEntryCreation() throws IOException {
		HashKey key = new HashKey(12);
		RID rid = new RID(new PageId(2),3);
		HashEntry entry = new HashEntry(key, rid);
		System.out.println(""+entry);
		byte[] arr = new byte[entry.size()];
		entry.writeToByteArray(arr, 0);
		
		HashEntry deser = new HashEntry(arr, 0);
		System.out.println(""+deser);
		
	}
	
	private void testKeyCreation() throws IOException {
		HashKey key = new HashKey("blaaaaaaa");
		int keyLength = key.size();
		System.out.println(key+" keyLength: "+keyLength);
		byte[] arr = new byte[keyLength+1];
		key.writeToByteArray(arr, 1);
		key = new HashKey(arr, 1);
		System.out.println(key);
	}
	
	public static void printPinnedPages() {
		System.out.println("pin: "+(SystemDefs.JavabaseBM.getNumBuffers()- SystemDefs.JavabaseBM.getNumUnpinnedBuffers()));

	}

}
