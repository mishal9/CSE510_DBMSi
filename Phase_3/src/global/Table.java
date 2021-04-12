package global;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.Scanner;

import btree.AddFileEntryException;
import btree.BT;
import btree.BTreeFile;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteRecException;
import btree.GetFileEntryException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.InsertRecException;
import btree.IntegerKey;
import btree.IteratorException;
import btree.Key;
import btree.KeyClass;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.StringKey;
import btree.UnpinPageException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import clustered_btree.ClusteredBTreeFile;
import hashindex.HIndex;
import hashindex.HashKey;
import heap.ClusteredHeapfile;
import heap.FieldNumberOutOfBoundException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.InvalidTypeException;
import heap.Scan;
import heap.SpaceNotAvailableException;
import heap.Tuple;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.JoinsException;
import iterator.LowMemException;
import iterator.OurFileScan;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;
import iterator.TupleUtils;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;

/** 
 * Enumeration class for TupleOrder
 * 
 */

public class Table implements GlobalConst{

	/* data folder */
	private  static String data_folder = "data/";
	
	/* table separateer in printing the table */
	private static String table_sep = "\t\t";
	
	/* tempfile name used for creating/adding data */
	private static String temp_heap_file = "temp_table_khusmodi.in";
	
	/* extension for the heapfile */
	private static String heapfile_ext = ".in";
	
	/* extension for the data filename */
	private static String data_file_ext = ".txt";
	
	/* extension for btree clustered index */
	private static String btree_clustered_file_ext = ".btreeclustered";
	
	/* extension for hash clustered index */
	private static String hash_clustered_file_ext = ".hashclustered";
	
	/* extension for clustered index */
	private static String btree_unclustered_file_ext = ".btreeunclustered";
	
	/* extension for clustered index */
	private static String hash_unclustered_file_ext = ".hashunclustered";
	
	/* list of all the tables that exist in this DB */
	private static List<String> DBtablenames;
	
	/* number of tables in the DB */
	private static int num_tables;
	
	/* name of this table */
	private String tablename;
	
	/* name of the heap file containing the heapfile for the tabe data */
	private String table_heapfile;
	
	/* list of all the unclustered index attributes in the table --> 0,1,2,3... */
	private boolean[] btree_unclustered_attr;
	
	public boolean[] getBtree_unclustered_attr() {
		return btree_unclustered_attr;
	}

	public void setBtree_unclustered_attr(boolean[] btree_unclustered_attr) {
		this.btree_unclustered_attr = btree_unclustered_attr;
	}

	/* list of all the hash unclustered index attributes in the table --> 0,1,2,3... */
	private boolean[] hash_unclustered_attr;

	public boolean[] getHash_unclustered_attr() {
		return hash_unclustered_attr;
	}

	public void setHash_unclustered_attr(boolean[] hash_unclustered_attr) {
		this.hash_unclustered_attr = hash_unclustered_attr;
	}
	
	/* attribute number for the btree clustered index --> 0,1,2,3....*/
	private int clustered_btree_attr = -1;
	
	public int getClustered_btree_attr() {
		return clustered_btree_attr;
	}

	public void setClustered_btree_attr(int clustered_btree_attr) {
		this.clustered_btree_attr = clustered_btree_attr;
	}

	public int getClustered_hash_attr() {
		return clustered_hash_attr;
	}

	public void setClustered_hash_attr(int clustered_hash_attr) {
		this.clustered_hash_attr = clustered_hash_attr;
	}

	/* attribute number for the hash clustered index --> 0,1,2,3.... */
	private int clustered_hash_attr = -1;
	
	
	public String getTable_heapfile() {
		return table_heapfile;
	}

	public void setTable_heapfile(String table_heapfile) {
		this.table_heapfile = table_heapfile;
	}

	/* name of the data file given by the user when creating the table */
	private String table_data_file;
	
	public String getTable_data_file() {
		return table_data_file;
	}

	public void setTable_data_file(String table_data_file) {
		this.table_data_file = table_data_file;
	}

	/* number of attributes in the table */
	private int table_num_attr;
	
	public String getTablename() {
		return tablename;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public int getTable_num_attr() {
		return table_num_attr;
	}

	public void setTable_num_attr(int table_num_attr) {
		this.table_num_attr = table_num_attr;
	}

	public AttrType[] getTable_attr_type() {
		return table_attr_type;
	}

	public void setTable_attr_type(AttrType[] table_attr_type) {
		this.table_attr_type = table_attr_type;
	}

	public String[] getTable_attr_name() {
		return table_attr_name;
	}

	public void setTable_attr_name(String[] table_attr_name) {
		this.table_attr_name = table_attr_name;
	}

	public int getTable_tuple_size() {
		return table_tuple_size;
	}

	public void setTable_tuple_size(int table_tuple_size) {
		this.table_tuple_size = table_tuple_size;
	}

	/* attr type of each attribute in the table */
	private AttrType[] table_attr_type;
	
	/* name of each column in the table */
	private String[] table_attr_name;
	
	/* size of the string fields in the data */
	private short[] table_attr_size;
	
	public short[] getTable_attr_size() {
		return table_attr_size;
	}

	public void setTable_attr_size(short[] table_attr_size) {
		this.table_attr_size = table_attr_size;
	}

	/* size of the tuple in the data field */
	private int table_tuple_size;
  
  public Table( String filename ) {
	  this.table_data_file = filename;
	  this.tablename = filename.substring(0, filename.length()-data_file_ext.length());
	  this.table_heapfile = filename.substring(0, filename.length()-data_file_ext.length()) + heapfile_ext;
  }
  
  public Table( String tablename,
		  		int table_num_attr,
		  		AttrType[] table_attr_type,
		  		String[] table_attr_name,
		  		int table_tuple_size,
		  		boolean[] btree_unclustered_attr,
		  		boolean[] hash_unclustered_attr,
		  		int clustered_btree_attr,
		  		int clustered_hash_attr) {
	  this.tablename = tablename;
	  this.table_heapfile = tablename + heapfile_ext;
	  this.table_num_attr = table_num_attr;
	  this.table_attr_type = table_attr_type;
	  this.table_attr_name = table_attr_name;
	  this.table_tuple_size = table_tuple_size;
	  this.btree_unclustered_attr = btree_unclustered_attr;
	  this.hash_unclustered_attr = hash_unclustered_attr;
	  this.clustered_btree_attr = clustered_btree_attr;
	  this.clustered_hash_attr = clustered_hash_attr;
	  
	  /* create the tuple and calculate the size of the tuple */
	  table_attr_size = new short[table_num_attr];
	  for(int i=0; i<table_attr_size.length; i++){
		  table_attr_size[i] = STRSIZE;
      }
  }
  
  /* create a table from the data file and stores it in the heap file */
  public void create_table(String heap_file_name, String data_file_name) {
	  try {
		/* print out the table name under process */
		System.out.println("Creating table "+tablename);
		/* initialising the heapfile for the table */
		Heapfile hf = new Heapfile(heap_file_name);
		
		/* opening the data file for reading */
		File file = new File(data_folder + table_data_file);
	    Scanner sc = new Scanner(file);
	    
	    /* initialising the number of attributes in the table */
	    table_num_attr = sc.nextInt();
	    
	    /* initialise the btree unclustered attr array i.e. no unclustered index exist at the time of creating the table for the first time*/
	    btree_unclustered_attr = new boolean[table_num_attr];
	    Arrays.fill(btree_unclustered_attr, false);
	    
	    /* initialise the hash unclustered attr array i.e. no unclustered index exist at the time of creating the table for the first time*/
	    hash_unclustered_attr = new boolean[table_num_attr];
	    Arrays.fill(hash_unclustered_attr, false);
	    
	    /* initialising the attr type array of attributes */
	    table_attr_type = new AttrType[table_num_attr];
	    
	    /* initialising the names of the attributes array */
	    table_attr_name = new String[table_num_attr];
	    
	    /* moving to next line to skip the firs tline read above */
	    sc.nextLine();
	    
	    /* parse the attributes from the data file */
	    int counter = 0;
	    while ( sc.hasNextLine() && ( counter < table_num_attr ) ) {
	    	String next_line = sc.nextLine();
	    	String[] tokens_next_line = next_line.split("\\s+");
	    	table_attr_name[counter] = tokens_next_line[0];
	    	table_attr_type[counter] = new AttrType(tokens_next_line[1].equals("STR") ? AttrType.attrString : AttrType.attrInteger);
	    	counter++;
	    }
	    
	    /* create the tuple and calculate the size of the tuple */
	    table_attr_size = new short[table_num_attr];
	    for(int i=0; i<table_attr_size.length; i++){
	    	table_attr_size[i] = STRSIZE;
        }
	    Tuple t = new Tuple();
        try {
            t.setHdr( (short)table_num_attr, table_attr_type, table_attr_size);
            table_tuple_size = t.size();
            //System.out.println("Size of the tuple: "+table_tuple_size);
            t = new Tuple(table_tuple_size);
            t.setHdr( (short)table_num_attr, table_attr_type, table_attr_size);
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
        }
        
        /* parse the data and store it in the heapfile */
	    while ( sc.hasNextLine() ) {
	    	String temp_next_line = sc.nextLine().trim();
	    	String[] token_next_line = temp_next_line.split("\\s+");
	    	for ( int i=0; i<table_num_attr; i++ ) {
	    		try {
		    		switch ( table_attr_type[i].attrType ) {
		    			case AttrType.attrString:
		    				t.setStrFld(i+1, token_next_line[i]);
		    				break;
		    			case AttrType.attrInteger:
		    				t.setIntFld(i+1, Integer.parseInt(token_next_line[i]));
		    				break;
		    			default:
		    				break;	    			
		    		}
	    		} catch (Exception e) {
                    e.printStackTrace();
                }
	    	}
	    	RID rid = new RID();
	    	try {
				rid = hf.insertRecord(t.returnTupleByteArray());
			} catch (InvalidSlotNumberException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTupleSizeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SpaceNotAvailableException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    try {
	    	SystemDefs.JavabaseDB.add_to_relation_queue(this);
			System.out.println("Number of elements in the table "+hf.getRecCnt());
			System.out.print("\n");
		} catch (InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	}catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
		System.err.println("*** Could not create heap file\n");
		e.printStackTrace();
	}
  }
  
  public void print_table() throws InvalidTupleSizeException {
	  for( int i=0; i<table_num_attr; i++ ) {
		  System.out.print(table_attr_name[i]+table_sep);
	  }
	  System.out.println();
	  /* open the data heap file */
  	  Heapfile heap_file;
  	  Scan data_scan;
  	  Tuple t, temp_t;
  	  RID rid = new RID();
	try {
		heap_file = new Heapfile(table_heapfile);
		//System.out.println("Number of records in the file: "+heap_file.getRecCnt());
		/* open a scan on the heap file */
	    data_scan = heap_file.openScan();
	    t = new Tuple(table_tuple_size);
	    temp_t = new Tuple(table_tuple_size);
        t.setHdr( (short)table_num_attr, table_attr_type, table_attr_size);
        temp_t.setHdr( (short)table_num_attr, table_attr_type, table_attr_size);
        temp_t = data_scan.getNext(rid);
        while ( temp_t != null ) {
        	t.tupleCopy(temp_t);
        	for ( int i=0; i<table_num_attr; i++) {
        		switch (table_attr_type[i].attrType) {
        			case AttrType.attrInteger:
        				System.out.print(t.getIntFld(i+1) + table_sep);
        				break;
        			case AttrType.attrString:
        				System.out.print(t.getStrFld(i+1) + table_sep);
        				break;
        			default:
        				System.out.println("Error in the system");
        				System.exit(0);
        				break;
        		}
        	}
        	System.out.println();
        	temp_t = data_scan.getNext(rid);
        }
	} catch (HFException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HFBufMgrException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HFDiskMgrException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (Exception e) {
        e.printStackTrace();
    }
  	  
  }
  
  /* attr_number --> 1,2,3,4 */
  private void create_btree_index(int attr_number ) {
	  try {
		/* open the data heapfile */
		Heapfile hf = new Heapfile(this.table_heapfile);
		Scan scan = hf.openScan();
		
		/* keep the key ready for insertion */
		KeyClass key;
		
		// create the index file
		BTreeFile btf  = new BTreeFile(this.get_unclustered_index_filename(attr_number, "btree"),
										table_attr_type[attr_number-1].attrType, 
										table_attr_size[attr_number-1],
										1/* delete */);
		
		/* start scanning each record and insert it to the btree */
		RID rid = new RID();
		Tuple t = TupleUtils.getEmptyTuple(table_attr_type, table_attr_size);
		Tuple temp = scan.getNext(rid);
		while ( temp != null ) {
			t.tupleCopy(temp);
			if ( table_attr_type[attr_number-1].attrType == AttrType.attrInteger ) {
				key = new IntegerKey(t.getIntFld(attr_number));
			}
			else {
				key = new StringKey(t.getStrFld(attr_number));
			}
			btf.insert(key, rid);
			temp = scan.getNext(rid);
		}
		scan.closescan();
		//BT.printBTree(btf.getHeaderPage());
		BT.printAllLeafPages(btf.getHeaderPage());
		btf.close();
		
		/* mark the unclustered index exist key */
		btree_unclustered_attr[attr_number-1] = true;
	} catch (HFException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HFBufMgrException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HFDiskMgrException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidTupleSizeException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidTypeException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (GetFileEntryException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ConstructPageException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (AddFileEntryException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (FieldNumberOutOfBoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (KeyTooLongException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (KeyNotMatchException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (LeafInsertRecException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IndexInsertRecException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (UnpinPageException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (PinPageException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (NodeNotMatchException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ConvertException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (DeleteRecException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IndexSearchException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IteratorException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (LeafDeleteException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InsertException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (PageUnpinnedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidFrameNumberException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HashEntryNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ReplacerException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
  
  /* attr_number --> 1,2,3,4 */
  private void create_hash_index(int attr_number ) {
	  try {
		/* open the data heapfile */
		Heapfile hf = new Heapfile(this.table_heapfile);
		Scan scan = hf.openScan();
		
		// create the index file
		HIndex hasher = new HIndex(this.get_unclustered_index_filename(attr_number, "hash"),
							  	   this.table_attr_type[attr_number-1].attrType, 
							  	    this.table_attr_size[attr_number-1],
							  	    TARGET_UTILISATION);
		
		/* start scanning each record and insert it to the btree */
		RID rid = new RID();
		Tuple t = TupleUtils.getEmptyTuple(table_attr_type, table_attr_size);
		Tuple temp = scan.getNext(rid);
		while ( temp != null ) {
			HashKey key;
			t.tupleCopy(temp);
			if ( table_attr_type[attr_number-1].attrType == AttrType.attrInteger ) {
				key = new HashKey(t.getIntFld(attr_number));
			}
			else {
				key = new HashKey(t.getStrFld(attr_number));
			}
			hasher.insert(key, rid);
			temp = scan.getNext(rid);
		}
		scan.closescan();
		hasher.print_bucket_names();
		hasher.close();
		
		/* mark the unclustered index exist key */
		hash_unclustered_attr[attr_number-1] = true;
	} catch (HFException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HFBufMgrException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HFDiskMgrException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidTupleSizeException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidTypeException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (GetFileEntryException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ConstructPageException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (AddFileEntryException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (FieldNumberOutOfBoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (KeyTooLongException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (KeyNotMatchException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (LeafInsertRecException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IndexInsertRecException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (UnpinPageException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (PinPageException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (NodeNotMatchException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ConvertException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (DeleteRecException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IndexSearchException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IteratorException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (LeafDeleteException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InsertException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (PageUnpinnedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidFrameNumberException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (HashEntryNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ReplacerException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
  
  /* attr_number --> 1,2,3,4... */
  public void create_unclustered_index( int attr_number, String file_type ) {
	  if ( file_type.equals("btree") ) {
		  create_btree_index(attr_number);
	  }
	  else {
		  //TBD create an unclustered hash index
		  create_hash_index(attr_number);
	  }
  }
  
  /* attr_number --> 0,2,3,4... */
  private String get_unclustered_index_filename( int attr_number, boolean[] unclustered_exist, String file_type, String file_ext ) {
	  return ( tablename + Integer.toString(attr_number) + file_ext );
  }
  
  /* attr_number --> 1,2,3,4... */
  public String get_unclustered_index_filename(int attr_number, String unclustered_index_type) {
	  if ( unclustered_index_type.equals("btree") ) {
		  return get_unclustered_index_filename(attr_number-1, btree_unclustered_attr, "btree", btree_unclustered_file_ext);
	  }
	  else if ( unclustered_index_type.equals("hash") ) {
		  return get_unclustered_index_filename(attr_number-1, hash_unclustered_attr, "hash", hash_unclustered_file_ext);
	  }
	  else {
		  return null;
	  }
  }
  
  /* attr_number --> 1,2,3,4... */
  public String get_clustered_index_filename(int attr_number, String clustered_index_type) {
	  if ( clustered_index_type.equals("btree") ) {
		  return ( this.tablename + Integer.toString(attr_number) + this.btree_clustered_file_ext );
	  }
	  else if ( clustered_index_type.equals("hash") ) {
		  return ( this.tablename + Integer.toString(attr_number) + this.hash_clustered_file_ext );
	  }
	  else {
		  return null;
	  }
  }
  
  /* attr_number --> 0,2,3,4... */
  private  boolean unclustered_index_exist( int attr_number, boolean[] unclustered_exist ) {
	  return unclustered_exist[attr_number];
  }
  
  /* attr_number --> 1,2,3,4... */
  public boolean unclustered_index_exist( int attr_number, String unclustered_index_type ) {
	  if ( unclustered_index_type.equals("btree") ) {
		  return unclustered_index_exist(attr_number-1, btree_unclustered_attr);
	  }
	  else if ( unclustered_index_type.equals("hash") ) {
		  return unclustered_index_exist(attr_number-1, hash_unclustered_attr);
	  }
	  else {
		  return false;
	  }
  }
  
  /* attr_number --> 1,2,3,4... */
  public boolean clustered_index_exist( String clustered_index_type ) {
	  if ( clustered_index_type.equals("btree") ) {
		  if ( this.clustered_btree_attr == -1 ) {
			  return false;
		  }
		  else {
			  return true;
		  }
	  }
	  else if ( clustered_index_type.equals("hash") ) {
		  if ( this.clustered_hash_attr == -1 ) {
			  return false;
		  }
		  else {
			  return true;
		  }
	  }
	  else {
		  return false;
	  }
  }
  
  
  /* attr_number --> 1,2,3,4... */
  public boolean clustered_index_exist( int attr_number, String clustered_index_type ) {
	  if ( clustered_index_type.equals("btree") ) {
		  if ( this.clustered_btree_attr == -1 ) {
			  return false;
		  }
		  else if ( this.clustered_btree_attr == attr_number ) {
			  return true;
		  }
	  }
	  else if ( clustered_index_type.equals("hash") ) {
		  if ( this.clustered_hash_attr == -1 ) {
			  return false;
		  }
		  else if ( this.clustered_hash_attr == attr_number ) {
			  return true;
		  }
	  }
	  else {
		  return false;
	  }
	  return false;
  }
  
  /* inserts data into an already existing table */
  public void insert_data( String filename ) {
	  /* created a temp heap file of the data */
	  //create_table(this.temp_heap_file, filename);
	  
	  /* print out the table name under process */
		System.out.println("Inserting into table "+this.tablename);
		
	try {
		/* initialising the heapfile for the table */
		ClusteredHeapfile hf = new ClusteredHeapfile(this.table_heapfile);
		
		/* opening the data file for reading */
		File file = new File(data_folder + filename);
	    Scanner sc = new Scanner(file);
	    
	    /* initialising the number of attributes in the table */
	    assert ( this.table_num_attr == sc.nextInt() );
	    
	    /* moving to next line to skip the first line read above */
	    sc.nextLine();
	    
	    /* parse the attributes from the data file */
	    int counter = 0;
	    while ( sc.hasNextLine() && ( counter < table_num_attr ) ) {
	    	String next_line = sc.nextLine();
	    	String[] tokens_next_line = next_line.split("\\s+");
	    	assert ( table_attr_name[counter] == tokens_next_line[0] );
	    	assert (table_attr_type[counter].attrType == (tokens_next_line[1].equals("STR") ? AttrType.attrString : AttrType.attrInteger) );
	    	counter++;
	    }
	    
        try {
        	Tuple t = TupleUtils.getEmptyTuple(table_attr_type, table_attr_size);
        	
        	/* parse the data and store it in the heapfile */
		    while ( sc.hasNextLine() ) {
		    	String temp_next_line = sc.nextLine().trim();
		    	String[] token_next_line = temp_next_line.split("\\s+");
		    	for ( int i=0; i<table_num_attr; i++ ) {
		    		try {
			    		switch ( table_attr_type[i].attrType ) {
			    			case AttrType.attrString:
			    				t.setStrFld(i+1, token_next_line[i]);
			    				break;
			    			case AttrType.attrInteger:
			    				t.setIntFld(i+1, Integer.parseInt(token_next_line[i]));
			    				break;
			    			default:
			    				break;	    			
			    		}
		    		} catch (Exception e) {
	                    e.printStackTrace();
	                }
		    	}
		    	RID rid = new RID();
		    	try {
		    		if ( clustered_index_exist("btree") ) {
		    			rid = hf.insertRecord(t, this.table_attr_type, 
		    								  this.table_attr_size, 
		    								  this.clustered_btree_attr, 
		    								  this.get_clustered_index_filename(this.clustered_btree_attr, "btree") );
		    		}
		    		else if ( clustered_index_exist("hash") ) {
		    			
		    		}
		    		else {
		    			rid = hf.insertRecord(t.returnTupleByteArray());
		    		}
					update_unclustered_index(t, rid);
				} catch (InvalidSlotNumberException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InvalidTupleSizeException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (SpaceNotAvailableException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
        }
        catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
        }
	} catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
  
  private void update_unclustered_index( Tuple t, RID rid ) {
	  // TBD/* update all the btree clustered indexes */
	  
	  /* update the unclustered btree indexes */
	  try {
		  /* keep the key ready for insertion */
		  KeyClass key;
		  for ( int i=0; i<btree_unclustered_attr.length; i++ ) {
			  if ( btree_unclustered_attr[i] ) {
					BTreeFile btf  = new BTreeFile(this.get_unclustered_index_filename(i+1, "btree"),
								table_attr_type[i].attrType, 
								table_attr_size[i],
								1/* delete */);
					if ( table_attr_type[i].attrType == AttrType.attrInteger ) {
						key = new IntegerKey(t.getIntFld(i+1));
					}
					else {
						key = new StringKey(t.getStrFld(i+1));
					}
					btf.insert(key, rid);
					//BT.printBTree(btf.getHeaderPage());
					//BT.printAllLeafPages(btf.getHeaderPage());
					btf.close();
			  }
			  if ( hash_unclustered_attr[i] ) {
				  
				  	HIndex hasher = new HIndex(this.get_unclustered_index_filename(i+1, "hash") );
					HashKey keyh;
					if ( table_attr_type[i].attrType == AttrType.attrInteger ) {
						keyh = new HashKey(t.getIntFld(i+1));
					}
					else {
						keyh = new HashKey(t.getStrFld(i+1));
					}
					hasher.insert(keyh, rid);
					//hasher.print_bucket_names();
					hasher.close();
			  }
		  }
	  }catch (GetFileEntryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConstructPageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AddFileEntryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FieldNumberOutOfBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeyTooLongException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeyNotMatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (LeafInsertRecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IndexInsertRecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnpinPageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PinPageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NodeNotMatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ConvertException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DeleteRecException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IndexSearchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IteratorException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (LeafDeleteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InsertException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HashEntryNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidFrameNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (PageUnpinnedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ReplacerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
  }

  public void create_table_struct( Scanner sc ) {
  	/* initialising the number of attributes in the table */
  	table_num_attr = sc.nextInt();
  
  	/* initialise the btree unclustered attr array i.e. no unclustered index exist at the time of creating the table for the first time*/
	btree_unclustered_attr = new boolean[table_num_attr];
	Arrays.fill(btree_unclustered_attr, false);
	
	/* initialise the hash unclustered attr array i.e. no unclustered index exist at the time of creating the table for the first time*/
	hash_unclustered_attr = new boolean[table_num_attr];
	Arrays.fill(hash_unclustered_attr, false);
	
	/* initialising the attr type array of attributes */
	table_attr_type = new AttrType[table_num_attr];
	
	/* initialising the names of the attributes array */
	table_attr_name = new String[table_num_attr];
	
	/* moving to next line to skip the first line read above */
    sc.nextLine();
    
    /* parse the attributes from the data file */
    int counter = 0;
    while ( sc.hasNextLine() && ( counter < table_num_attr ) ) {
    	String next_line = sc.nextLine();
    	String[] tokens_next_line = next_line.split("\\s+");
    	table_attr_name[counter] = tokens_next_line[0];
    	table_attr_type[counter] = new AttrType(tokens_next_line[1].equals("STR") ? AttrType.attrString : AttrType.attrInteger);
    	counter++;
    }
    
    /* create the tuple and calculate the size of the tuple */
    table_attr_size = new short[table_num_attr];
    for(int i=0; i<table_attr_size.length; i++){
    	table_attr_size[i] = STRSIZE;
    }
  }
  
  /* This method is used to create a table and create a corresponding clustered
   * tree with key, rid pairs.
   * clustered_attr_num --> 1,2,3... */
  public void create_table(int clustered_attr_num) {
	  /* print out the table name under process */
	  System.out.println("Creating table "+tablename);
	  
	  /* saving the clustered btree attr */
	  this.clustered_btree_attr = clustered_attr_num;
	  
	  /* adding the sorted data to the temp heap file 
	   * After calling this function, we have the sorted data in the heap file.
	   * Now we need to build a clustered btree on this data
	   * */
	  create_sorted_heap_file();
	  
	  SystemDefs.JavabaseDB.add_to_relation_queue(this);
	  System.out.print("\n");
	  
	  /* create a custered btree now */
	  //print_table_attr();
  }
  
  private void print_table_attr() {
	  System.out.println("Table attrypes: "+ Arrays.toString(this.table_attr_type));
	  System.out.println("Table num attr: "+ Integer.toString(table_num_attr));
  }
  
  /* writes the sorted data into the table heap file and returns
   * also creates a clustered index in parellel
   *  */
  private void create_sorted_heap_file() {
	  try {
		
		/* initialising the heapfile for the table */
		Heapfile hf = new Heapfile(this.temp_heap_file);
		
		/* opening the data file for reading */
		File file = new File(data_folder + table_data_file);
	    Scanner sc = new Scanner(file);
	    
	    /* initialising the number of attributes in the table */
	    create_table_struct(sc);

	    Tuple t = TupleUtils.getEmptyTuple(this.table_attr_type, this.table_attr_size);
	    
	    this.table_tuple_size = t.size();
	    
	    /* initialise a btree to insert the data records */
	    ClusteredBTreeFile btf  = new ClusteredBTreeFile(this.get_clustered_index_filename(this.clustered_btree_attr, "btree"),
										table_attr_type[this.clustered_btree_attr-1].attrType, 
										table_attr_size[this.clustered_btree_attr-1],
										1/* delete */);
		
        /* parse the data and store it in the heapfile */
	    while ( sc.hasNextLine() ) {
	    	String temp_next_line = sc.nextLine().trim();
	    	String[] token_next_line = temp_next_line.split("\\s+");
	    	for ( int i=0; i<table_num_attr; i++ ) {
	    		try {
		    		switch ( table_attr_type[i].attrType ) {
		    			case AttrType.attrString:
		    				t.setStrFld(i+1, token_next_line[i]);
		    				break;
		    			case AttrType.attrInteger:
		    				t.setIntFld(i+1, Integer.parseInt(token_next_line[i]));
		    				break;
		    			default:
		    				break;	    			
		    		}
	    		} catch (Exception e) {
                    e.printStackTrace();
                }
	    	}
	    	RID rid = new RID();
	    	try {
				rid = hf.insertRecord(t.returnTupleByteArray());
			} catch (InvalidSlotNumberException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvalidTupleSizeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SpaceNotAvailableException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    sc.close();
        FldSpec[] projlist = new FldSpec[this.table_num_attr];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for( int i=0; i<projlist.length; i++ )
        {
            projlist[i] = new FldSpec(rel, i+1);
        }

        AttrType[] attrType_for_proj = new AttrType[this.table_num_attr];

        for(int i=0;i<this.table_num_attr;i++)
            attrType_for_proj[i] = new AttrType(this.table_attr_type[i].attrType);

        Sort sort_c = null;
        FileScan fscan = null;
        fscan = new FileScan(this.temp_heap_file,
        					 this.table_attr_type, 
        					 this.table_attr_size, 
        					 (short)this.table_num_attr,
        					 (short)this.table_num_attr,
        					 projlist,
        					 null);
        
        sort_c = new Sort(this.table_attr_type,
        				  (short)this.table_num_attr,
        				  this.table_attr_size,
        				  fscan,
        				  this.clustered_btree_attr,
        				  new TupleOrder(TupleOrder.Ascending),
        				  this.table_attr_size[this.clustered_btree_attr-1],
        				  100);
        Tuple t_s = TupleUtils.getEmptyTuple(this.table_attr_type, this.table_attr_size);
        Tuple t1 = sort_c.get_next();

        /* keep the key ready for insertion */
		KeyClass key, prev_key;
		prev_key = null;
		key = null;
        ClusteredHeapfile hf1 = new ClusteredHeapfile(this.table_heapfile);
        RID curr_rid = new RID();
        RID prev_rid = new RID(new PageId(INVALID_PAGE), INVALID_SLOT);
        while ( t1 != null ) {
        	t_s.tupleCopy(t1);
        	if ( table_attr_type[this.clustered_btree_attr-1].attrType == AttrType.attrInteger ) {
				key = new IntegerKey(t_s.getIntFld(this.clustered_btree_attr));
			}
			else {
				key = new StringKey(t_s.getStrFld(this.clustered_btree_attr));
			}
        	curr_rid = hf1.insertRecord(t_s.getTupleByteArray(), this.table_attr_type, this.table_attr_size);
        	if ( ( prev_rid.pageNo.pid != curr_rid.pageNo.pid ) && ( prev_rid.pageNo.pid != INVALID_PAGE ) ) {
        		btf.insert(prev_key, prev_rid);
        	}
        	prev_rid.copyRid(curr_rid);
        	if ( key instanceof IntegerKey )
        		prev_key = new IntegerKey(((IntegerKey) key).getKey());
        	else
        		prev_key = new StringKey(((StringKey) key).getKey());
        	t1 = sort_c.get_next();
        }
        if ( ( prev_rid.pageNo.pid == curr_rid.pageNo.pid ) && ( prev_rid.pageNo.pid != INVALID_PAGE ) ) {
    		btf.insert(key, curr_rid);
    	}
        sort_c.close();
        fscan.close();
	    hf.deleteFile();
	    System.out.println("Number of elements in the table "+hf1.getRecCnt());
	    BT.printAllLeafPages(btf.getHeaderPage());
	    btf.close();
	}catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
		System.err.println("*** Could not create heap file\n");
		e.printStackTrace();
	} catch (InvalidTypeException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (InvalidTupleSizeException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (InvalidSlotNumberException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (FileScanException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (TupleUtilsException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InvalidRelation e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (SortException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (UnknowAttrType e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (LowMemException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (JoinsException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }
  
  /* this function deletes the records from the existing data heap files and index 
   * Main function to be called in driver for delete
   * */
  public void delete_table( String delete_data_file_name ) {
	  try {
			/* print out the table name under process */
			System.out.println("Deleting elements from table "+tablename);
			
			/* opening the data file for reading 
			 * TBD might need a change in case we need to input paths of the files*/
			File file = new File(data_folder + delete_data_file_name);
		    Scanner sc = new Scanner(file);
		    
		    /* initialising the number of attributes in the table */
		    assert ( this.table_num_attr == sc.nextInt() );
		    
		    /* moving to next line to skip the firs tline read above */
		    sc.nextLine();
		    
		    /* parse the attributes from the data file */
		    int counter = 0;
		    while ( sc.hasNextLine() && ( counter < table_num_attr ) ) {
		    	String next_line = sc.nextLine();
		    	String[] tokens_next_line = next_line.split("\\s+");
		    	assert ( table_attr_name[counter] == tokens_next_line[0] );
		    	assert (table_attr_type[counter].attrType == (tokens_next_line[1].equals("STR") ? AttrType.attrString : AttrType.attrInteger) );
		    	counter++;
		    }
		    
		    Tuple t = TupleUtils.getEmptyTuple(this.table_attr_type, this.table_attr_size);
		    if ( this.clustered_index_exist("btree") ) 
		    {
		    	/* parse the data and store it in the heapfile */
		    	ClusteredHeapfile hf = new ClusteredHeapfile(this.table_heapfile);
			    while ( sc.hasNextLine() ) 
			    {
			    	String temp_next_line = sc.nextLine().trim();
			    	String[] token_next_line = temp_next_line.split("\\s+");
			    	for ( int i=0; i<table_num_attr; i++ ) {
			    		try {
				    		switch ( table_attr_type[i].attrType ) {
				    			case AttrType.attrString:
				    				t.setStrFld(i+1, token_next_line[i]);
				    				break;
				    			case AttrType.attrInteger:
				    				t.setIntFld(i+1, Integer.parseInt(token_next_line[i]));
				    				break;
				    			default:
				    				break;	    			
				    		}
			    		} catch (Exception e) {
		                    e.printStackTrace();
		                }
			    	}
			    	RID rid = new RID();
			    	try {
						boolean status = hf.deleteRecord(t, this.get_clustered_index_filename(this.clustered_btree_attr, "btree"),
											 this.table_attr_type, this.table_attr_size, this.clustered_btree_attr);
					} catch (InvalidSlotNumberException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InvalidTupleSizeException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (SpaceNotAvailableException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			    }
		    }
		    
	  }catch (Exception e1) {
		  e1.printStackTrace();
	  }
  }
  
  /* prints all the indexes on the attribute */
  public void print_index(int att_number) throws Exception {
	  System.out.println("*******************Printing all indices on attribute "+this.table_attr_name[att_number-1]+" *******************\n\n");
	  
	  /* print the btree index first */
	  if ( unclustered_index_exist(att_number, "btree") ) {
		  System.out.println("*******************Printing unclustered btree index*******************");
		  String btsfilename = get_unclustered_index_filename(att_number, "btree");
		  BTreeFile btf  = new BTreeFile(btsfilename);
		  BT.printBTree(btf.getHeaderPage());
		  BT.printAllLeafPages(btf.getHeaderPage());
		  btf.close();
	  }
	  if ( clustered_index_exist(att_number, "btree") ) {
		  System.out.println("********************Printing clustered btree index********************");
		  String btsfilename = get_clustered_index_filename(att_number, "btree");
		  ClusteredBTreeFile btf  = new ClusteredBTreeFile(btsfilename);
		  BT.printBTree(btf.getHeaderPage());
		  BT.printAllLeafPages(btf.getHeaderPage());
		  btf.close();
	  }
	  if ( unclustered_index_exist(att_number, "hash") ) {
		  System.out.println("*******************Printing unclustered hash index*******************");
		  String hsfilename = get_unclustered_index_filename(att_number, "hash");
		  HIndex hasher = new HIndex(hsfilename);
		  hasher.print_bucket_names();
		  hasher.close();
	  }
	  //TBD clustered hash index print remaining 
  }
  
  public void test() throws Exception {
	  /* make a fake tuple and insert onto the data heap file */
	  Tuple t = TupleUtils.getEmptyTuple(table_attr_type, table_attr_size);
	  t.setStrFld(1, "Yash");
	  t.setIntFld(2, 5);
	  t.setIntFld(3, 3);
	  t.setIntFld(4, 3);
	  t.setIntFld(5, 3);
	  t.setStrFld(6, "200954");
	  t.print(table_attr_type);
	  
	  Tuple t1 = TupleUtils.getEmptyTuple(table_attr_type, table_attr_size);
	  t1.setStrFld(1, "Zebra");
	  t1.setIntFld(2, 5);
	  t1.setIntFld(3, 3);
	  t1.setIntFld(4, 3);
	  t1.setIntFld(5, 3);
	  t1.setStrFld(6, "200954");
	  t1.print(table_attr_type);
	  
	  Tuple t2 = TupleUtils.getEmptyTuple(table_attr_type, table_attr_size);
	  t2.setStrFld(1, "Modi");
	  t2.setIntFld(2, 5);
	  t2.setIntFld(3, 3);
	  t2.setIntFld(4, 3);
	  t2.setIntFld(5, 3);
	  t2.setStrFld(6, "201954");
	  t2.print(table_attr_type);
	  
	  Tuple t3 = TupleUtils.getEmptyTuple(table_attr_type, table_attr_size);
	  t3.setStrFld(1, "Payal");
	  t3.setIntFld(2, 5);
	  t3.setIntFld(3, 3);
	  t3.setIntFld(4, 3);
	  t3.setIntFld(5, 3);
	  t3.setStrFld(6, "201954");
	  t3.print(table_attr_type);
	  
	  ClusteredHeapfile hp = new ClusteredHeapfile(this.table_heapfile);
	  RID rid = hp.insertRecord(t, this.table_attr_type, this.table_attr_size, this.clustered_btree_attr, get_clustered_index_filename(clustered_btree_attr, "btree"));
	  System.out.println("RID page no. "+rid.pageNo.pid);
	  rid = hp.insertRecord(t1, this.table_attr_type, this.table_attr_size, this.clustered_btree_attr, get_clustered_index_filename(clustered_btree_attr, "btree"));
	  System.out.println("RID page no. "+rid.pageNo.pid);
	  rid = hp.insertRecord(t2, this.table_attr_type, this.table_attr_size, this.clustered_btree_attr, get_clustered_index_filename(clustered_btree_attr, "btree"));
	  System.out.println("RID page no. "+rid.pageNo.pid);
	  rid = hp.insertRecord(t3, this.table_attr_type, this.table_attr_size, this.clustered_btree_attr, get_clustered_index_filename(clustered_btree_attr, "btree"));
	  System.out.println("RID page no. "+rid.pageNo.pid);
  }
}


