package heap;

import java.io.IOException;
import java.util.function.Function;

import diskmgr.Page;
import global.PageId;
import global.RID;
import hashindex.HashKey;
import hashindex.HashUtils;

public class ClusHIndexDataFile extends Heapfile {

	public ClusHIndexDataFile(String name) throws HFException, HFBufMgrException, HFDiskMgrException, IOException {
		super(name);
		//HashUtils.log("Created ClustDataFile" + name);
	}

	public RID insertRecordOnExistingPage(byte[] record, PageId pageToInsertId) throws Exception {
		
		int recLen = record.length;
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		PageId nextDirPageId = new PageId(0);

		HFPage currentDirPage = new HFPage();
		RID ridOfPageInDirPage = null;
		boolean found = false;
		boolean canBeStored = false;
		DataPageInfo dpinfo = new DataPageInfo();
		while(currentDirPageId.pid != INVALID_PAGE)
		{
			//HashUtils.log("checking dir page id :"+currentDirPageId.pid);
			pinPage(currentDirPageId, currentDirPage, false);

			RID rid = new RID();
			Tuple atuple;
			for (rid = currentDirPage.firstRecord();
					rid != null;	// rid==NULL means no more record
					rid = currentDirPage.nextRecord(rid))
			{
				atuple = currentDirPage.getRecord(rid);
				dpinfo = new DataPageInfo(atuple);

				if(dpinfo.pageId.pid == pageToInsertId.pid) {
					//HashUtils.log("page id found in directory "+dpinfo.pageId.pid);
					found = true;
					if(recLen <= dpinfo.availspace)
					{
						HashUtils.log("data page has enough space");
						ridOfPageInDirPage = new RID(new PageId(rid.pageNo.pid), rid.slotNo);
						canBeStored = true;
					}else {
						HashUtils.log("not enough space in data page :"+pageToInsertId.pid);
						canBeStored = false;
					}

					break;
				}
				
			}

			unpinPage(currentDirPageId, false /*undirty*/);
			if(found == true ) {
				//HashUtils.log("breaking from outer loop");
				break;
			}
			nextDirPageId = currentDirPage.getNextPage();
			currentDirPageId.pid = nextDirPageId.pid;
		}
		
		if(canBeStored == false) { //not possible to insert in this data page
			return null;
		} 
		HFPage dataPageToInsert = new HFPage();
		pinPage(pageToInsertId, dataPageToInsert, false);
		
		if ((dpinfo.pageId).pid == INVALID_PAGE) // check error!
			throw new HFException(null, "invalid PageId");

		if (!(dataPageToInsert.available_space() >= recLen))
			throw new SpaceNotAvailableException(null, "no available space");

		
		RID rid;
		rid = dataPageToInsert.insertRecord(record);
		System.out.println("dpinfo.availspace: "+dpinfo.availspace);
		dpinfo.recct++;
		dpinfo.availspace = dataPageToInsert.available_space();
		System.out.println("after dpinfo.availspace: "+dpinfo.availspace);
		dpinfo.flushToTuple();
		unpinPage(dpinfo.pageId, true /* = DIRTY */);
		
		pinPage(currentDirPageId, currentDirPage, false);
		 DataPageInfo dpinfo_ondirpage = new DataPageInfo(currentDirPage.returnRecord(ridOfPageInDirPage));
	      
	      
	      dpinfo_ondirpage.availspace = dpinfo.availspace;
	      dpinfo_ondirpage.recct = dpinfo.recct;
	      dpinfo_ondirpage.pageId.pid = dpinfo.pageId.pid;
	      dpinfo_ondirpage.flushToTuple();
	      
		unpinPage(currentDirPageId, true /* = DIRTY */);


		return rid;

	}
	
	public RID insertRecordToNewPage(byte[] record) throws Exception {

		//create new page and insert data to it

		DataPageInfo newPageInfo = new DataPageInfo();
		HFPage newDataPage = _newDatapage(newPageInfo );
		pinPage(newPageInfo.pageId, newDataPage, false);
		RID insertedRecordLocation;
		insertedRecordLocation = newDataPage.insertRecord(record);
		newPageInfo.recct++;
		newPageInfo.availspace = newDataPage.available_space();


		// add the DataPageInfo to directory
		PageId currentDirPageId = new PageId(_firstDirPageId.pid);

		PageId nextDirPageId = new PageId(0);
		HFPage currentDirPage = new HFPage();
		boolean foundSpaceInDirectoryPage = false;
		while(currentDirPageId.pid != INVALID_PAGE)
		{
			//HashUtils.log("checking dir page id :"+currentDirPageId.pid);
			pinPage(currentDirPageId, currentDirPage, false);

			
			if(currentDirPage.available_space() >= newPageInfo.size) {
				foundSpaceInDirectoryPage = true;

				byte [] tmpData = newPageInfo.convertToTuple().getTupleByteArray();
				RID newPageLocationInExistingDirectoryPage = currentDirPage.insertRecord(tmpData);
				//HashUtils.log("newPageLocationInExistingDirectoryPage "+newPageLocationInExistingDirectoryPage);
				RID tmprid = currentDirPage.firstRecord();

			}

			if(foundSpaceInDirectoryPage == true) {
				unpinPage(currentDirPageId, true /*DIRTY*/);
				break;
			}

			
			nextDirPageId = currentDirPage.getNextPage();
			
			if(nextDirPageId.pid == INVALID_PAGE) { //insert another directory page
				HashUtils.log("Inserting another directory page");
				Page pageinbuffer = new Page();
				nextDirPageId = newPage(pageinbuffer , 1);
				// need check error!
				if(nextDirPageId == null)
					throw new HFException(null, "can't new pae");
				
				HFPage newDirPage = new HFPage();
				// initialize new directory page
				newDirPage .init(nextDirPageId, pageinbuffer);
				PageId temppid = new PageId(INVALID_PAGE);
				newDirPage.setNextPage(temppid);
				newDirPage.setPrevPage(currentDirPageId);
				
				// update current directory page and unpin it
				currentDirPage.setNextPage(nextDirPageId);
				unpinPage(currentDirPageId, true/*dirty*/);
				
				//insert the new page info in the new directory page
				byte [] tmpData = newPageInfo.convertToTuple().getTupleByteArray();
				RID newPageLocationInNewDirectoryPage = newDirPage.insertRecord(tmpData);
				//HashUtils.log("newPageLocationInNewDirectoryPage "+newPageLocationInNewDirectoryPage);
				
				unpinPage(nextDirPageId, true/*dirty*/);
				break;

			}
			unpinPage(currentDirPageId, false /*undirty*/);
			
			currentDirPageId.pid = nextDirPageId.pid;
		}



		//cleanup
		unpinPage(newPageInfo.pageId, true /* = DIRTY */);


		return insertedRecordLocation;

	}

	@Override
	public RID insertRecord(byte[] recPtr) throws InvalidSlotNumberException, InvalidTupleSizeException,
			SpaceNotAvailableException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
		throw new IllegalArgumentException("Funtion not supported for clustered file");
	}
	
	public void printToConsole(Function<byte[], String> mapper) throws Exception {
		Scan scan = openScan();
		RID rid = new RID();
		Tuple tup;
		boolean done = false;
		while (!done) {
			tup = scan.getNext(rid);

			if (tup == null) {
				done = true;
				break;
			}
			HashUtils.log("Tuple length "+mapper.apply(tup.getTupleByteArray())+" @ "+rid);
				
		}
		scan.closescan();
	}

}
