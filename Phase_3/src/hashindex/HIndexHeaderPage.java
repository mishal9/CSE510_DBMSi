package hashindex;

import java.io.IOException;

import btree.ConstructPageException;
import diskmgr.Page;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.Tuple;

public class HIndexHeaderPage extends HFPage {

	/**
	 * locations of stuff <br>
	 * hashIndexName --> first element in page <br>
	 * numberOfBuckets --> previous page id <br>
	 * 
	 * 
	 * 
	 * @param hashIndexName must be unique
	 * @throws Exception
	 */
	public HIndexHeaderPage(String hashIndexName, int numberOfBuckets) throws Exception {
		super();

		Page apage = new Page();
		PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
		if (pageId == null)
			throw new ConstructPageException(null, "new page failed");
		this.init(pageId, apage);

		RID hashIndexNameLocation = insertRecord(hashIndexName.getBytes());
		HashUtils.log("hashIndexNameLocation: " + hashIndexNameLocation);
		set_NumberOfBuckets(numberOfBuckets);

	}

	public HIndexHeaderPage(PageId pageno) throws ConstructPageException {
		super();
		try {
			SystemDefs.JavabaseBM.pinPage(pageno, this, false/* Rdisk */);
		} catch (Exception e) {
			throw new ConstructPageException(e, "pinpage failed");
		}
	}

	PageId getPageId() throws IOException {
		return getCurPage();
	}

	void setPageId(PageId pageno) throws IOException {
		setCurPage(pageno);
	}

	public void set_keyType(byte keyType) {

	}

	public byte get_keyType() {
		return 0;
	}

	public String get_HashIndexName() throws Exception {
		Tuple tup = getRecord(firstRecord());
		return new String(tup.getTupleByteArray());
	}

	public String get_NthBucketName(int n) throws Exception {
		return get_HashIndexName() + n;
	}

	public void set_NumberOfBuckets(int numberOfBuckets) throws IOException {
		setPrevPage(new PageId(numberOfBuckets));
	}

	public int get_NumberOfBuckets() throws IOException {
		return getPrevPage().pid;
	}

}
