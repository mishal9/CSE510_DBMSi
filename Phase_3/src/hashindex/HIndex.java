package hashindex;

import btree.AddFileEntryException;
import btree.GetFileEntryException;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;

public class HIndex implements GlobalConst {

	HIndexHeaderPage headerPage;
	PageId headerPageId;
	
	
	int targetUtilization = 80;

	public HIndex(String fileName, int keyType, int keySize) throws Exception {
		headerPageId = get_file_entry(fileName);
		if (headerPageId == null) // file not exist
		{
			HashUtils.log("Creating new HIndex header page");
			//creating new header page with filename and number of buckets 1
			headerPage = new HIndexHeaderPage(fileName,2);
			headerPageId = headerPage.getPageId();
			add_file_entry(fileName, headerPageId);

			headerPage.set_keyType((byte) keyType);
			headerPage.set_H0Deapth(1);
			headerPage.set_SplitPointerLocation(0);
			headerPage.set_EntriesCount(0);
			

		} else {
			HashUtils.log("Opening existing HIndex");
			headerPage = new HIndexHeaderPage(headerPageId);
		}
		
		//h1Deapth = h0Deapth + 1;
		
		
	}


	public void insert(HashKey key, RID rid) throws Exception {
		HashEntry entry = new HashEntry(key, rid);
		int hash = entry.key.getHash(headerPage.get_H0Deapth());
		int splitPointer = headerPage.get_SplitPointerLocation();
		if(hash<splitPointer) {
			hash = entry.key.getHash(headerPage.get_H0Deapth()+1);
			HashUtils.log("new hash: "+hash);
		}
		
		int bucketNumber = hash;
		String bucketName = headerPage.get_NthBucketName(bucketNumber);
		HashUtils.log("Inserting to bucket: " + bucketNumber);
		HashBucket bucket = new HashBucket(bucketName);
		bucket.insertEntry(entry);
		headerPage.set_EntriesCount(headerPage.get_EntriesCount() + 1);
		// now add buckets(pages) if reqd
		float currentEntryCount = headerPage.get_EntriesCount();
		int bucketCount = headerPage.get_NumberOfBuckets();
		float maxPossibleEntries = (bucketCount * MINIBASE_PAGESIZE) / entry.size();
		float util = currentEntryCount / maxPossibleEntries;
		HashUtils.log("util: " + util);
		
		if (util >= targetUtilization) {
			HashUtils.log("Adding a bucket page to HIndex");
			
			
			//rehash element in bucket splitPointer
			//TODO
			
			splitPointer++;
			headerPage.set_SplitPointerLocation(splitPointer);
			
		}
	}

	public boolean delete(HashKey key)  throws Exception{
		HashBucket bucket = new HashBucket(headerPage.get_NthBucketName(0));
		boolean status = bucket.deleteEntry(key);
		return status;
	}



	public void close() throws Exception {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	// static methods ///////
	private static PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private static void add_file_entry(String fileName, PageId pageno) throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}


}
