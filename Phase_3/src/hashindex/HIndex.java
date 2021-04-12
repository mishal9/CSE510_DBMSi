package hashindex;

import btree.AddFileEntryException;
import btree.GetFileEntryException;
import btree.KeyNotMatchException;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;

public class HIndex implements GlobalConst {

	HIndexHeaderPage headerPage;
	PageId headerPageId;
	
	
	final float targetUtilization;

	public HIndex(String fileName, int keyType, int keySize,int targetUtilization) throws Exception {
		headerPageId = get_file_entry(fileName);
		if (headerPageId == null) // file not exist
		{
			HashUtils.log("Creating new HIndex header page");
			//creating new header page with filename and number of buckets 2
			headerPage = new HIndexHeaderPage(fileName,2);
			headerPageId = headerPage.getPageId();
			add_file_entry(fileName, headerPageId);

			headerPage.set_keyType( keyType);
			headerPage.set_H0Deapth(1);
			headerPage.set_SplitPointerLocation(0);
			headerPage.set_EntriesCount(0);
			headerPage.set_TargetUtilization(targetUtilization);
			

		} else {
			HashUtils.log("Opening existing HIndex");
			headerPage = new HIndexHeaderPage(headerPageId);
		}
		this.targetUtilization = (float) ((float)headerPage.get_TargetUtilization()/100.0);
				
	}
	
	public HIndex(String fileName) throws Exception {
		headerPageId = get_file_entry(fileName);
		if(headerPageId == null) {
			throw new IllegalArgumentException("No index found with name "+fileName);
		}
		headerPage = new HIndexHeaderPage(headerPageId);
		this.targetUtilization = (float) ((float)headerPage.get_TargetUtilization()/100.0);
	}


	public void insert(HashKey key, RID rid) throws Exception {
		if (key.type != headerPage.get_keyType()) {
			throw new KeyNotMatchException("Key types dont match!");
		}
		HashEntry entry = new HashEntry(key, rid);
		int hash = entry.key.getHash(headerPage.get_H0Deapth());
		int splitPointer = headerPage.get_SplitPointerLocation();
		if (hash < splitPointer) {
			hash = entry.key.getHash(headerPage.get_H0Deapth() + 1);
			HashUtils.log("new hash: " + hash);
		}

		int bucketNumber = hash;
		String bucketName = headerPage.get_NthBucketName(bucketNumber);
		HashUtils.log("Inserting " + entry.key + " to bucket: " + bucketNumber);
		HashBucket bucket = new HashBucket(bucketName);
		bucket.insertEntry(entry);
		headerPage.set_EntriesCount(headerPage.get_EntriesCount() + 1);
		// now add buckets(pages) if reqd
		float currentEntryCount = headerPage.get_EntriesCount();
		int bucketCount = headerPage.get_NumberOfBuckets();
		float maxPossibleEntries = (bucketCount * MINIBASE_PAGESIZE) / entry.size();
		float currentUtilization = currentEntryCount / maxPossibleEntries;
		HashUtils.log("currentUtilization: " + currentUtilization);
		HashUtils.log("targetUtilization: " + targetUtilization);
		if (currentUtilization >= targetUtilization) {
			HashUtils.log("Adding a bucket page to HIndex");

			headerPage.set_NumberOfBuckets(headerPage.get_NumberOfBuckets() + 1);
			// rehash element in bucket splitPointer

			rehashBucket(headerPage.get_NthBucketName(splitPointer), headerPage.get_H0Deapth() + 1);
			splitPointer++;
			if (splitPointer == (1 << headerPage.get_H0Deapth())) {
				splitPointer = 0;
				headerPage.set_H0Deapth(headerPage.get_H0Deapth() + 1);
				HashUtils.log("resetting split pointer to 0 ");
			}
			headerPage.set_SplitPointerLocation(splitPointer);
			HashUtils.log("after split splitPointer: " + splitPointer);

		}
	}

	public boolean delete(HashKey key,RID rid)  throws Exception{
		int hash = key.getHash(headerPage.get_H0Deapth());
		int splitPointer = headerPage.get_SplitPointerLocation();
		if (hash < splitPointer) {
			hash = key.getHash(headerPage.get_H0Deapth() + 1);
			//HashUtils.log("new hash: " + hash);
		}

		int bucketNumber = hash;
		String bucketName = headerPage.get_NthBucketName(bucketNumber);
		HashBucket bucket = new HashBucket(bucketName);
		HashEntry entry = new HashEntry(key, rid);

		HashUtils.log("Deleting entry: "+entry+" from bucket: "+bucketNumber);
		boolean status = bucket.deleteEntry(entry);
		headerPage.set_EntriesCount(headerPage.get_EntriesCount() - 1);
		
		//now shrink index if reqd
		float maxPossibleEntries = (headerPage.get_NumberOfBuckets() * MINIBASE_PAGESIZE) / entry.size();
		float currentUtilization = headerPage.get_EntriesCount() / maxPossibleEntries;
		HashUtils.log("currentUtilization: " + currentUtilization);
		float deletionTargetUtilization = (float) (targetUtilization - 0.1 < 0 ? 0 : targetUtilization - 0.1) ;
		HashUtils.log("deletionTargetUtilization: " + deletionTargetUtilization);
		if (headerPage.get_NumberOfBuckets() > 2 && currentUtilization <= deletionTargetUtilization) {
			HashUtils.log("Shrinking the index");
			System.out.println("sp: "+splitPointer);
			rehashBucket(headerPage.get_NthBucketName(splitPointer+1), headerPage.get_H0Deapth());
			splitPointer --;
			if (splitPointer == -1) {
				splitPointer = (1 << headerPage.get_H0Deapth()) - 1;
				headerPage.set_H0Deapth(headerPage.get_H0Deapth() - 1);
				HashUtils.log("resetting split pointer to  "+splitPointer);
			}
			headerPage.set_NumberOfBuckets(headerPage.get_NumberOfBuckets()-1);
			headerPage.set_SplitPointerLocation(splitPointer);
			HashUtils.log("after shrink splitPointer: " + splitPointer);
		}
		
		
		return status;
	}

	private void rehashBucket(String bucketToBeRehashedName,int newDeapth) throws Exception {
		Heapfile tempheapfile = new Heapfile("temp");
		HashBucket bucketToBeRehashed = new HashBucket(bucketToBeRehashedName);
		Scan scan = bucketToBeRehashed.heapfile.openScan();
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
			tempheapfile.insertRecord(tup.returnTupleByteArray());
			i++;
		}
		HashUtils.log("entries added to temp heapfile: "+i);
		scan.closescan();
		bucketToBeRehashed.heapfile.deleteFile();
		Scan tempHeapScan = tempheapfile.openScan();
		bucketToBeRehashed = new HashBucket(bucketToBeRehashedName);
		 rid = new RID();
		done = false;
		i = 0;
		while (!done) {
			tup = tempHeapScan.getNext(rid);

			if (tup == null) {
				done = true;
				break;
			}
			HashEntry scannedHashEntry = new HashEntry(tup.returnTupleByteArray(), 0);
			int hash1 = scannedHashEntry.key.getHash(newDeapth);
			String newBucketName = headerPage.get_NthBucketName(hash1);
			HashBucket newBucket = new HashBucket(newBucketName );
			newBucket.insertEntry(scannedHashEntry);
			HashUtils.log("Rehashing "+scannedHashEntry.key+" to bucket "+newBucketName);
			
			i++;
		}
		HashUtils.log("entries rehashed: "+i);
		tempHeapScan.closescan();
		tempheapfile.deleteFile();
	}
	
	
	public HIndexScan new_scan(HashKey key) throws Exception {
		HIndexScan scan = new HIndexScan(this, key);
		return scan;
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
	
	public void print_bucket_names() {
		try {
			for( int i=0; i<this.headerPage.get_NumberOfBuckets(); i++ ) {
				System.out.println("BUCKET: "+i);
				HashBucket bucket = new HashBucket(this.headerPage.get_NthBucketName(i));
				bucket.printToConsole();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

}
