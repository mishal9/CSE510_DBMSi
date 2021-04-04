package hashindex;

import java.io.IOException;

import btree.AddFileEntryException;
import btree.GetFileEntryException;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;

public class HIndex implements GlobalConst {

	HIndexHeaderPage headerPage;
	PageId headerPageId;

	public HIndex(String fileName, int keyType, int keySize) throws Exception {
		headerPageId = get_file_entry(fileName);
		if (headerPageId == null) // file not exist
		{
			HashUtils.log("Creating new HIndex header page");
			//creating new header page with filename and number of buckets 1
			headerPage = new HIndexHeaderPage(fileName,1);
			headerPageId = headerPage.getPageId();
			add_file_entry(fileName, headerPageId);

			headerPage.set_keyType((byte) keyType);

		} else {
			HashUtils.log("Opening existing HIndex");
			headerPage = new HIndexHeaderPage(headerPageId);
		}
	}


	public void insert(HashKey key, RID rid) throws Exception{
		HashEntry entry = new HashEntry(key, rid);
		int bucketNumber =0;
		HashUtils.log("Inserting to bucket: "+bucketNumber);
		HashBucket bucket = new HashBucket(headerPage.get_NthBucketName(bucketNumber ));
		bucket.insertEntry(entry);
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
