package iterator;

import bufmgr.PageNotReadException;
import diskmgr.PCounter;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.TupleOrder;
import heap.*;
import index.IndexException;
import iterator.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

public class SortFirstSky extends Iterator implements GlobalConst {

    private static int _n_pages;
    private static String _relationName;
    private static int[] _pref_list;
    private static int _pref_list_length;
    private FileScan _fscan;
    private SortPref _sort;
    private static AttrType[] _in;
    private static short _len_in;
    private static short[] _str_sizes;
    private static Heapfile temp;
    private static FileScan _tscan; // for scanning the temp heap file
    boolean status = OK;
    private static short REC_LEN = 32;
    private static AttrType[] _attrType;
    private static short[] _attrSize;
    private static FldSpec[] _projlist;
    //private static LinkedHashSet<Tuple> _window;
    private static Tuple[] _window;
    private static short _tuple_size;
    public static int counter;

    public SortFirstSky(AttrType[] in1, int len_in1, short[] t1_str_sizes,
                        Iterator am1, short tuple_size, java.lang.String
                                relationName, int[] pref_list, int pref_list_length,
                        int n_pages) throws IOException {

        _in = in1;
        _len_in = (short)len_in1;
        _str_sizes = t1_str_sizes;
        _fscan = (FileScan) am1;
        _tuple_size = tuple_size;

        _attrType = new AttrType[_len_in];
        _attrSize = new short[_len_in];

        for(int i=0; i<_attrType.length; i++){
            _attrType[i] = new AttrType(AttrType.attrReal);
        }

        for(int i=0; i<_attrSize.length; i++){
            _attrSize[i] = REC_LEN;
        }

        // create an iterator by open a file scan
        _projlist = new FldSpec[_len_in];
        RelSpec rel = new RelSpec(RelSpec.outer);

        for(int i=0; i<_projlist.length; i++){
            _projlist[i] = new FldSpec(rel, i+1);;
        }

        _relationName = relationName;
        _pref_list = pref_list;
        _pref_list_length = pref_list_length;
        _n_pages = n_pages-1; // (let one out for spare in case of temp heap)
        // _window = new LinkedHashSet<Tuple>(_n_pages);
        _window = new Tuple[(MINIBASE_PAGESIZE / _tuple_size) * _n_pages];

        // Sort "test1sortPref.in"
        try {
            _sort = new SortPref(_attrType, (short) _len_in, _attrSize, _fscan,  new TupleOrder(TupleOrder.Descending), _pref_list, _pref_list_length, 2000);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            // temp heap file to store overflown skyline objects
            temp = new Heapfile("sortFirstSkyTemp.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        System.out.println("----------   SORT FIRST SKY INIT VARS   -------------");
        System.out.println("Attributes: "+Arrays.toString(_in));
        System.out.println("Attributes length: "+_len_in);
        System.out.println("Relation name: "+_relationName);
        System.out.println("Preferences list: "+ Arrays.toString(_pref_list));
        System.out.println("Preferences list length: "+_pref_list_length);
        System.out.println("Size of each tuple: "+_tuple_size);
        System.out.println("Length of the buffer: "+_window.length);
        System.out.println("-----------------------------------------------------");

        if ( status == OK )
            computeSkylines(_sort);

        close();
    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, Exception {
        if(counter < _window.length){
            return _window[counter++];
        }else{
            return _tscan.get_next();
        }
    }

    public void close(){
        _fscan.close();
    }

    public void computeSkylines(SortPref sort) throws IOException {

        /*
        SORT FIRST SKY:

        1. Put the first <n_pages> sorted tuples in main memory
        2. Compare the rest against the ones in main memory
            1. If tuple in heap file is dominated by at least one in main memory - simply discard it from heap
            2. If tuple in heap file dominates any one in main memory - replace the tuple with the one in main memory
            3. If tuple is not dominated by any tuple in main memory - put into temp heap file
        3. At end - return both temp heap + main memory objects


        Scenarios:

        If tuple heap_tuple (dominated by):
	        dominated by tuple memory_tuple -> discard heap_tuple
	        not dominated by any of tuple memory_tuple -> put into temp heap
        If tuple heap_tuple  (dominates):
	        dominates memory_tuple -> discard memory_tuple
         */

        // create a tuple of appropriate size
        Tuple t = new Tuple();

        try {
            // check if there's atleast one tuple
            t = sort.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int count = 0;

        while (t != null && count < _window.length) {
            Tuple temp = new Tuple(t);
            _window[count] = temp;
            // _window.add(temp);
            count++;

            try {
                t = sort.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }


        // AT THIS point: in-memory window is formed

        System.out.println("In memory objects");
        for(int i=0; i<_window.length; i++) {
            if(_window[i] != null)
                _window[i].print(_attrType);
        }

        count  = 0;

        RID rid = null;

        while (t != null) {
            boolean isDominatedBy = false;
            System.out.println("=======");
            Tuple htuple = new Tuple(t);

            try {
                /*
                Compare the rest against the ones in main memory
                1. If tuple in heap file is dominated by at least one in main memory - simply discard it from heap
                2. If tuple in heap file dominates any one in main memory - replace the tuple with the one in main memory
                3. If tuple is not dominated by any tuple in main memory - put into temp heap file
                */

                for(int i=0; i<_window.length; i++){
                    if (TupleUtils.Dominates(htuple, _attrType, _window[i], _attrType, _len_in, _str_sizes, _pref_list, _pref_list_length)) {
                        // 2. If tuple in heap file dominates any one in main memory - replace the tuple with the one in main memory
                        System.out.println("Heap tuple");
                        htuple.print(_attrType);
                        System.out.println("Dominates ");
                        _window[i].print(_attrType);
                        System.out.println(" ");
                        _window[i] = htuple;
                    } else {
                        // 1. If tuple in heap file is dominated by at least one in main memory - simply move to the next element
                        isDominatedBy = true;
                        System.out.println("Heap tuple");
                        htuple.print(_attrType);
                        System.out.println("Dominated by ");
                        _window[i].print(_attrType);
                        System.out.println(" ");
                        break;
                    }
                }

                // Also compare with elements in the heap
                if(temp.getRecCnt() > 0) {
                    try {
                        _tscan = new FileScan("sortFirstSkyTemp.in", _attrType, _attrSize, (short) _len_in, _len_in, _projlist, null);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }


                    try {
                        Tuple tempHeapTuple = _tscan.get_next();

                            while (tempHeapTuple != null) {
                                Tuple tTuple = new Tuple(tempHeapTuple);

                                // check for dominance here
                                System.out.println("Compare ");
                                htuple.print(_attrType);
                                System.out.println("Against ");
                                tTuple.print(_attrType);

                                try {
                                    tempHeapTuple = _tscan.get_next();
                                }
                                catch (Exception e) {
                                    status = FAIL;
                                    e.printStackTrace();
                                }
                            }
                        } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                // Create unsorted data file "test1.in"

                if(!isDominatedBy){
                    // 3. If tuple is not dominated by any tuple in main memory - put into temp heap file
                    // Tuple remained un-dominated -> potential skyline object
                    // check if space left in window
                    // else put in temp heap
                    // Inserting potential skyline candidate in the temp heap file
                    htuple.print(_attrType);

                    if(count < _window.length){
                        _window[count] = htuple;
                    }else{
                        try {
                            rid = temp.insertRecord(htuple.returnTupleByteArray());
                        }
                        catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }
                        System.out.println("Is not Dominated by any window objects");
                    }

                }

            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            count ++;

            try {
                t = sort.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        // reset

        try {
            _tscan = new FileScan("sortFirstSkyTemp.in", _attrType, _attrSize, (short) _len_in, _len_in, _projlist, null);
        } catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        return;
    }
}