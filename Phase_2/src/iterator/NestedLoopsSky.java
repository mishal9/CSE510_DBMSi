package iterator;

import heap.*;
import global.*;
import bufmgr.*;

import java.lang.*;
import java.io.*;
import java.util.Arrays;

/**
 *use the iterator and relationName to compute the skyline using nested loop method
 *output file, call get_next to get all tuples
 */
public class NestedLoopsSky extends Iterator
{
    private AttrType[]  _in1;
    private short        _len_in1;
    private short[]     _t1_str_sizes;
    //private FileScan    _outer_iterator;
    //private FileScan    _inner_iterator;
    private String      _relation_name;
    private int[]       _pref_list;
    private int         _pref_list_length;
    private int         _n_pages;
    private Tuple       _next_skyline_element;
    private Heapfile    _heap_file;
    private boolean     _status;
    private Scan        _scan;
    private Scan        _outer_scan;


    /**
     *constructor
     *@param in1  array showing what the attributes of the input fields are.
     *@param len_in1  number of attributes in the input tuple
     *@param t1_str_sizes  shows the length of the string fields
     *@param relationName heapfile to be opened
     *@param n_out_flds  number of fields in the out tuple
     *@param proj_list  shows what input fields go where in the output tuple
     *@param outFilter  select expressions
     *@exception IOException some I/O fault
     *@exception FileScanException exception from this class
     *@exception TupleUtilsException exception from this class
     *@exception InvalidRelation invalid relation
     */
    public  NestedLoopsSky
    (
            AttrType[] in1,
            int len_in1,
            short[] t1_str_sizes,
            Iterator am1,
            String relationName,
            int[] pref_list,
            int pref_list_length,
            int n_pages
    )
            throws	IOException,
            FileScanException,
            TupleUtilsException,
            InvalidRelation
    {
        this._in1 = in1;
        this._len_in1 = (short)len_in1;
        this._t1_str_sizes = t1_str_sizes;
        //this._outer_iterator = (FileScan)am1;
        this._relation_name = relationName;
        this._pref_list = pref_list;
        this._pref_list_length = pref_list_length;
        this._n_pages = n_pages;
        this._next_skyline_element = null;
        this._scan = null;

        System.out.println("Attr  types"+ Arrays.toString(this._in1));
        System.out.println("Attr types length "+ (this._len_in1));
        System.out.println("Str sizes "+ Arrays.toString(this._t1_str_sizes));
        System.out.println("Prefernce list "+ Arrays.toString(this._pref_list));
        System.out.println("N pages"+ _n_pages);

        try {
        	SystemDefs.JavabaseBM.limit_memory_usage(true, this._n_pages);
        	this._heap_file = new Heapfile(this._relation_name);
            this._status = true;
        }
        catch (Exception e) {
            System.err.println("Could not open the heapfile");
            e.printStackTrace();
        }

        /*outer_candidate = this._outer_iterator.get_next();*/
        if ( this._status == true )
        {
            try
            {
                this._outer_scan = this._heap_file.openScan();
            }
            catch (Exception e)
            {
                this._status = false;
                System.err.println ("*** Error opening scan\n");
                e.printStackTrace();
            }
        }


    }



    /**
     *@return the result tuple
     *@exception JoinsException some join exception
     *@exception IOException I/O errors
     *@exception InvalidTupleSizeException invalid tuple size
     *@exception InvalidTypeException tuple type not valid
     *@exception PageNotReadException exception from lower layer
     *@exception PredEvalException exception from PredEval class
     *@exception UnknowAttrType attribute type unknown
     *@exception FieldNumberOutOfBoundException array out of bounds
     *@exception WrongPermat exception for wrong FldSpec argument
     */
    public Tuple get_next()
            throws JoinsException,
            IOException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            PredEvalException,
            UnknowAttrType,
            FieldNumberOutOfBoundException,
            WrongPermat, TupleUtilsException 
    {
        /* read the next tuple from outer_iterator
         * Assumption here is that inner iterator is a filescan iterator */
        Tuple outer_candidate = new Tuple();
        outer_candidate.setHdr(this._len_in1, this._in1, this._t1_str_sizes);
        int size = outer_candidate.size();
        outer_candidate = new Tuple(size);
        outer_candidate.setHdr(this._len_in1, this._in1, this._t1_str_sizes);
        Tuple inner_candidate = new Tuple(size);
        inner_candidate.setHdr(this._len_in1, this._in1, this._t1_str_sizes);
        RID temp = new RID();
        Tuple outer_candidate_temp, inner_candidate_temp;
        while (true)
        {
            
            /*outer_candidate = this._outer_iterator.get_next();*/
            outer_candidate_temp = this._outer_scan.getNext(temp);
            if (outer_candidate_temp == null)
            {
                System.out.println("No more records in skyline. All records already scanned.");
                SystemDefs.JavabaseBM.limit_memory_usage(false, this._n_pages);
                return null;
            }
            //outer_candidate1.print(this._in1);
            outer_candidate.tupleCopy(outer_candidate_temp);
            /* open a scan on the heapfile/relationname for inner loop */
            if ( this._status == true )
            {
                try
                {
                    this._scan = this._heap_file.openScan();
                }
                catch (Exception e)
                {
                    this._status = false;
                    System.err.println ("*** Error opening scan\n");
                    e.printStackTrace();
                }
            }
            boolean inner_scan_complete = false;
            boolean inner_dominates_outer = false;
            while (!inner_scan_complete)
            {
                inner_candidate_temp = this._scan.getNext(temp);
                //System.out.println("Comparing "+inner_candidate_temp);
                if (inner_candidate_temp == null)
                {
                    inner_scan_complete = true;
                }
                else
                {
                    /* compare the outer loop tuple with inner loop tuple */
                	inner_candidate.tupleCopy(inner_candidate_temp);
                	//inner_candidate.print(this._in1);
                    inner_dominates_outer = TupleUtils.Dominates(inner_candidate,
                            									 this._in1,
                            									 outer_candidate,
                            									 this._in1,
                            									 this._len_in1,
                            									 this._t1_str_sizes,
                            									 this._pref_list,
                            									 this._pref_list_length);

                    if (inner_dominates_outer) {
                        break;
                    }
                }
            }
            if (inner_dominates_outer == false)
            {
                return outer_candidate;
            }
            this._scan.closescan();
        }
    }

    /**
     *implement the abstract method close() from super class Iterator
     *to finish cleaning up
     */
    public void close()
    {
        if (!closeFlag)
        {
            closeFlag = true;
        }

       // _scan.closescan();
        //_outer_scan.closescan();
    }

}

