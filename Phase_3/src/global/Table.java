package global;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

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

/** 
 * Enumeration class for TupleOrder
 * 
 */

public class Table implements GlobalConst{

	/* data folder */
	private  static String data_folder = "data/";
	
	/* table separateer in printing the table */
	private static String table_sep = "\t\t";
	
	/* extension for the heapfile */
	private static String heapfile_ext = ".in";
	
	/* extension for the data filename */
	private static String data_file_ext = ".txt";
	
	/* list of all the tables that exist in this DB */
	private static List<String> DBtablenames;
	
	/* number of tables in the DB */
	private static int num_tables;
	
	/* name of this table */
	private String tablename;
	
	/* name of the heap file containing the heapfile for the tabe data */
	private String table_heapfile;
	
	public String getTable_heapfile() {
		return table_heapfile;
	}

	public void setTable_heapfile(String table_heapfile) {
		this.table_heapfile = table_heapfile;
	}

	/* name of the data file given by the user when creating the table */
	private String table_data_file;
	
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
  
  public Table( String tablename, int table_num_attr, AttrType[] table_attr_type, String[] table_attr_name, int table_tuple_size ) {
	  this.tablename = tablename;
	  this.table_heapfile = tablename + heapfile_ext;
	  this.table_num_attr = table_num_attr;
	  this.table_attr_type = table_attr_type;
	  this.table_attr_name = table_attr_name;
	  this.table_tuple_size = table_tuple_size;
	  /* create the tuple and calculate the size of the tuple */
	  table_attr_size = new short[table_num_attr];
	  for(int i=0; i<table_attr_size.length; i++){
		  table_attr_size[i] = STRSIZE;
      }
  }
  
  /* create a table from the data file and stores it in the heap file */
  public void create_table() {
	  try {
		/* print out the table name under process */
		System.out.println("Creating table "+tablename);
		/* initialising the heapfile for the table */
		Heapfile hf = new Heapfile(table_heapfile);
		
		/* opening the data file for reading */
		File file = new File(data_folder + table_data_file);
	    Scanner sc = new Scanner(file);
	    
	    /* initialising the number of attributes in the table */
	    table_num_attr = sc.nextInt();
	    
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
	    	SystemDefs.JavabaseDB.add_to_relation_tables(this);
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

}


