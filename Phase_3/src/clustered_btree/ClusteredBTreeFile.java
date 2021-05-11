/*
 * @(#) bt.java   98/03/24
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu).
 *
 */

package clustered_btree;
import btree.*;
import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;

/** btfile.java
 * This is the main definition of class BTreeFile, which derives from 
 * abstract base class IndexFile.
 * It provides an insert/delete interface.
 */
public class ClusteredBTreeFile extends ClBTreeFile
implements GlobalConst {



	/**  BTreeFile class
	 * an index file with given filename should already exist; this opens it.
	 *@param filename the B+ tree file name. Input parameter.
	 *@exception GetFileEntryException  can not ger the file from DB 
	 *@exception PinPageException  failed when pin a page
	 *@exception ConstructPageException   BT page constructor failed
	 */
	public ClusteredBTreeFile(String filename)
			throws GetFileEntryException,  
			PinPageException, 
			ConstructPageException        
	{      
		super(filename);
	}  


	/**
	 *  if index file exists, open it; else create it.
	 *@param filename file name. Input parameter.
	 *@param keytype the type of key. Input parameter.
	 *@param keysize the maximum size of a key. Input parameter.
	 *@param delete_fashion full delete or naive delete. Input parameter.
	 *           It is either DeleteFashion.NAIVE_DELETE or 
	 *           DeleteFashion.FULL_DELETE.
	 *@exception GetFileEntryException  can not get file
	 *@exception ConstructPageException page constructor failed
	 *@exception IOException error from lower layer
	 *@exception AddFileEntryException can not add file into DB
	 */
	public ClusteredBTreeFile(String filename, int keytype,
			int keysize, int delete_fashion)  
					throws GetFileEntryException, 
					ConstructPageException,
					IOException, 
					AddFileEntryException
	{

		super(filename, keytype, keysize, delete_fashion);
	}

	public void  updateHeader(PageId newRoot, PageId maxPageId)
			throws   IOException, 
			PinPageException,
			UnpinPageException
	{

		BTreeHeaderPage header;
		PageId old_data;


		header= new BTreeHeaderPage( pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId( newRoot);

		/* bottom rightmost page of the btree */
		header.set_maxPageno(maxPageId);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */ );


		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty

	}

	/** create a scan with given keys
	 * Cases:
	 *      (1) lo_key = null, hi_key = null
	 *              scan the whole index
	 *      (2) lo_key = null, hi_key!= null
	 *              range scan from min to the hi_key
	 *      (3) lo_key!= null, hi_key = null
	 *              range scan from the lo_key to max
	 *      (4) lo_key!= null, hi_key!= null, lo_key = hi_key
	 *              exact match ( might not unique)
	 *      (5) lo_key!= null, hi_key!= null, lo_key < hi_key
	 *              range scan from lo_key to hi_key
	 *@param lo_key the key where we begin scanning. Input parameter.
	 *@param hi_key the key where we stop scanning. Input parameter.
	 *@exception IOException error from the lower layer
	 *@exception KeyNotMatchException key is not integer key nor string key
	 *@exception IteratorException iterator error
	 *@exception ConstructPageException error in BT page constructor
	 *@exception PinPageException error when pin a page
	 *@exception UnpinPageException error when unpin a page
	 */
	public ClBTFileScan new_scan_cl(KeyClass lo_key, KeyClass hi_key)
			throws IOException,  
			KeyNotMatchException, 
			IteratorException, 
			ConstructPageException, 
			PinPageException, 
			UnpinPageException

	{
		ClBTFileScan scan = new ClBTFileScan();
		if ( headerPage.get_rootId().pid==INVALID_PAGE) {
			scan.leafPage=null;
			return scan;
		}

		scan.treeFilename=dbname;
		scan.endkey=hi_key;
		scan.didfirst=false;
		scan.deletedcurrent=false;
		scan.curRid=new RID();     
		scan.keyType=headerPage.get_keyType();
		scan.maxKeysize=headerPage.get_maxKeySize();
		scan.bfile=this;

		//this sets up scan at the starting position, ready for iteration
		scan.leafPage=findRunStart( lo_key, scan.curRid);
		return scan;
	}
	
	/** create a scan with given keys
	 * Cases:
	 *      (1) lo_key = null, hi_key = null
	 *              scan the whole index
	 *      (2) lo_key = null, hi_key!= null
	 *              range scan from min to the hi_key
	 *      (3) lo_key!= null, hi_key = null
	 *              range scan from the lo_key to max
	 *      (4) lo_key!= null, hi_key!= null, lo_key = hi_key
	 *              exact match ( might not unique)
	 *      (5) lo_key!= null, hi_key!= null, lo_key < hi_key
	 *              range scan from lo_key to hi_key
	 *@param lo_key the key where we begin scanning. Input parameter.
	 *@param hi_key the key where we stop scanning. Input parameter.
	 *@exception IOException error from the lower layer
	 *@exception KeyNotMatchException key is not integer key nor string key
	 *@exception IteratorException iterator error
	 *@exception ConstructPageException error in BT page constructor
	 *@exception PinPageException error when pin a page
	 *@exception UnpinPageException error when unpin a page
	 */
	public ClBTFileScanASC new_scan_cl_ASC(KeyClass lo_key, KeyClass hi_key)
			throws IOException,  
			KeyNotMatchException, 
			IteratorException, 
			ConstructPageException, 
			PinPageException, 
			UnpinPageException

	{
		ClBTFileScanASC scan = new ClBTFileScanASC();
		if ( headerPage.get_rootId().pid==INVALID_PAGE) {
			scan.leafPage=null;
			return scan;
		}

		scan.treeFilename=dbname;
		scan.endkey=hi_key;
		scan.didfirst=false;
		scan.deletedcurrent=false;
		scan.curRid=new RID();     
		scan.keyType=headerPage.get_keyType();
		scan.maxKeysize=headerPage.get_maxKeySize();
		scan.bfile=this;

		//this sets up scan at the starting position, ready for iteration
		scan.leafPage=findRunStart( lo_key, scan.curRid);
		return scan;
	}
}
