package hashindex;

import java.util.ArrayList;
import java.util.List;

import btree.KeyNotMatchException;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.ClusHIndexDataFile;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;

public class ClusHIndex implements GlobalConst{

	HIndexHeaderPage headerPage;
	PageId headerPageId;
	ClusHIndexDataFile dataFile;

	final float targetUtilization;


	public ClusHIndex(String datafilename, String indexfileName, int keyType, int keySize,int targetUtilization) throws Exception {

		headerPageId = HashUtils.get_file_entry(indexfileName);
		if (headerPageId == null) // file not exist
		{
			HashUtils.log("Creating new HIndex header page");
			//creating new header page with filename and number of buckets 2
			headerPage = new HIndexHeaderPage(indexfileName,2);
			headerPageId = headerPage.getPageId();
			HashUtils.add_file_entry(indexfileName, headerPageId);

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
		this.dataFile = new ClusHIndexDataFile(datafilename);


	}

	public ClusHIndex(String datafilename, String indexfileName) throws Exception {
		headerPageId = HashUtils.get_file_entry(indexfileName);
		if(headerPageId == null) {
			throw new IllegalArgumentException("No index found with name "+indexfileName);
		}
		headerPage = new HIndexHeaderPage(headerPageId);
		this.targetUtilization = (float) ((float)headerPage.get_TargetUtilization()/100.0);
		this.dataFile = new ClusHIndexDataFile(datafilename);
	}

	public RID insert(HashKey key,Tuple tup) throws Exception {
		HashUtils.log("[ClusHIndex] trying to insert key : "+key);

		if (key.type != headerPage.get_keyType()) {
			throw new KeyNotMatchException("Key types dont match!");
		}
		int hash = key.getHash(headerPage.get_H0Deapth());
		int splitPointer = headerPage.get_SplitPointerLocation();
		if (hash < splitPointer) {
			hash = key.getHash(headerPage.get_H0Deapth() + 1);
			HashUtils.log("new hash: " + hash);
		}

		int bucketNumber = hash;
		String bucketName = headerPage.get_NthBucketName(bucketNumber);
		HashBucket bucket = new HashBucket(bucketName);


		//insert data in datafile and key in bucket(if reqd)
		RID insertedLocation = insertInDataFileAndBucket(key, tup, bucket);


		// now add buckets(pages) if reqd
		float currentEntryCount = headerPage.get_EntriesCount();
		int bucketCount = headerPage.get_NumberOfBuckets();
		float maxPossibleEntries = (bucketCount * MINIBASE_PAGESIZE) / (8+key.size());
		float currentUtilization = currentEntryCount / maxPossibleEntries;
		HashUtils.log("currentUtilization: " + currentUtilization);
		HashUtils.log("targetUtilization: " + targetUtilization);
		if (currentUtilization >= targetUtilization) {
			HashUtils.log("Adding a bucket page to HIndex");
			//System.out.println("currentUtilization: " + currentUtilization);
			headerPage.set_NumberOfBuckets(headerPage.get_NumberOfBuckets() + 1);
			// rehash element in bucket splitPointer

			rehashClusBucket(headerPage.get_NthBucketName(splitPointer), headerPage.get_H0Deapth() + 1);
			splitPointer++;
			if (splitPointer == (1 << headerPage.get_H0Deapth())) {
				splitPointer = 0;
				headerPage.set_H0Deapth(headerPage.get_H0Deapth() + 1);
				HashUtils.log("resetting split pointer to 0 ");
			}
			headerPage.set_SplitPointerLocation(splitPointer);
			HashUtils.log("after split splitPointer: " + splitPointer);

		}
		HashUtils.log("[ClusHIndex] inserted key "+key+" @ "+ insertedLocation );
		return insertedLocation;
	}

	public void close() throws Exception {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	private RID insertInDataFileAndBucket(HashKey key, Tuple tup, HashBucket bucket) throws Exception {
		byte[] record = tup.getTupleByteArray();
		List<Integer> pageNumList = getExistingPagePointersForKeyInBucket(key,bucket);
		RID insertedLocationOfTupleInDataFile = null;
		boolean recordInsertedInDataFile = false;
		//first try to insert in any of the pages which have same key
		for (Integer pageNumOfKeyInDataFile : pageNumList) {
			RID locationInDataFile = dataFile.insertRecordOnExistingPage(record, new PageId(pageNumOfKeyInDataFile));

			if(locationInDataFile != null) {
				insertedLocationOfTupleInDataFile = new RID(new PageId(locationInDataFile.pageNo.pid),locationInDataFile.slotNo);
				recordInsertedInDataFile = true;
				HashUtils.log("[ClusHIndex][SAME_KEY] Inserted data in " + key + " to existing bucket entry, RID in data file: " + insertedLocationOfTupleInDataFile);
				break;
			}
		}

		if(recordInsertedInDataFile == false) { //insert data to new page in datafile, key in bucket
			RID loc = dataFile.insertRecordToNewPage(record);
			insertedLocationOfTupleInDataFile = new RID(new PageId(loc.pageNo.pid),loc.slotNo);
			HashEntry ent = new HashEntry(key, insertedLocationOfTupleInDataFile); 
			bucket.insertEntry(ent);
			headerPage.set_EntriesCount(headerPage.get_EntriesCount() + 1);
			HashUtils.log("[ClusHIndex][NEW_KEY] Inserting " + key + " to bucket: " + bucket);

		} 
		return insertedLocationOfTupleInDataFile;
	}

	public static List<Integer> getExistingPagePointersForKeyInBucket(HashKey key,HashBucket buc) throws Exception {
		List<Integer> pageNumList = new ArrayList<>();
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
				int pageNum = scannedHashEntry.rid.pageNo.pid;
				//HashUtils.log("Key is already in bucket with page pointer: "+pageNum);
				pageNumList.add(pageNum);
			}
		}
		scan.closescan();
		return pageNumList;
	}
	
	private void rehashClusBucket(String bucketToBeRehashedName,int newDeapth) throws Exception {
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
		HashUtils.log("entries rehashed: " + i);
		tempHeapScan.closescan();
		tempheapfile.deleteFile();
	}
	
	public ClusHIndexScan new_scan(HashKey key) throws Exception {
		ClusHIndexScan scan = new ClusHIndexScan(this, key);
		return scan;
	}

	public HIndexHeaderPage getHeaderPage() {
		return headerPage;
	}
	public ClusHIndexDataFile getDataFile() {
		return dataFile;
	}
}
