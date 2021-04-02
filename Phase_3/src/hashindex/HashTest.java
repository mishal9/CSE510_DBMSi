package hashindex;

import java.io.IOException;

import global.PageId;
import global.RID;

public class HashTest {

	public static void main(String[] args) {
		
		System.out.println("Start");
		
		HashTest thiss = new HashTest();
		try {
			thiss.testHashBucket();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("End");
	}
	

	private void testHashBucket() throws IOException {
		HashBucket bucket = new HashBucket(null);


		for (int i = 0; i < 10; i++) {
			HashKey key = new HashKey(i);
			RID rid = new RID(new PageId(i),i);
			HashEntry ent = new HashEntry(key, rid);
			bucket.insertEntry(ent);

		}
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

}
