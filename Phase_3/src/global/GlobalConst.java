package global;

public interface GlobalConst {

  public static final int MINIBASE_MAXARRSIZE = 50;
  public static final int NUMBUF = 50;

  /** Size of page. */
  public static int MINIBASE_PAGESIZE = 1024;           // in bytes

  /** Size of each frame. */
  public static final int MINIBASE_BUFFER_POOL_SIZE = 1024;   // in Frames

  public static final int MAX_SPACE = 1024;   // in Frames
  
  /**
   * in Pages => the DBMS Manager tells the DB how much disk 
   * space is available for the database.
   */
  public static final int MINIBASE_DB_SIZE = 10000;           
  public static final int MINIBASE_MAX_TRANSACTIONS = 100;
  public static final int MINIBASE_DEFAULT_SHAREDMEM_SIZE = 1000;
  
  /**
   * also the name of a relation
   */
  public static final int MAXFILENAME  = 15;          
  public static final int MAXINDEXNAME = 40;
  public static final int MAXATTRNAME  = 15;    
  public static final int MAX_NAME = 50;
  
  /* max size of the string attributes in a tuple */
  public static final int STRSIZE = 150;
  
  /* number of pages in the DB */
  public static final int NUMDBPAGES = 160000;
  
  /* number of BM pages in any DB */
  public static final int NUMBFPAGES = 3000;
  
  /* replacer algorithm in the DB */
  public static final String DBREPLACER = "Clock";
  
  public static final int INVALID_PAGE = -1;
}
