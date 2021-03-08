package skylines;

import global.AttrType;
import global.GlobalConst;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashSet;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

public class SortFirstSky implements GlobalConst {

    private static int _n_pages;
    private static String _relationName;
    private static int[] _pref_list;
    private static int _pref_list_length;
    private Scan _fscan;
    private SortPref _sort;
    private static AttrType[] _in;
    private static short _len_in;
    private static Heapfile _input;
    private static short[] _str_sizes;
    private static Heapfile temp;
    boolean status = OK;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 32;
    private static short REC_LEN3 = 32;
    private static short REC_LEN4 = 32;
    private static short REC_LEN5 = 32;
    private static AttrType[] _attrType = new AttrType[6];
    private static short[] _attrSize = new short[6];
    private static FldSpec[] _projlist;
    //private static LinkedHashSet<Tuple> _window;
    private static Tuple[] _window;

    public SortFirstSky(AttrType[] in1, int len_in1, short[] t1_str_sizes,
                        Iterator am1, java.lang.String
                         relationName, int[] pref_list, int pref_list_length,
                        int n_pages) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {

        _in = in1;
        _len_in = (short)len_in1;
        _str_sizes = t1_str_sizes;
        _sort = (SortPref) am1;
        _fscan = null;

        _attrType[0] = new AttrType(AttrType.attrReal);
        _attrType[1] = new AttrType(AttrType.attrReal);
        _attrType[2] = new AttrType(AttrType.attrReal);
        _attrType[3] = new AttrType(AttrType.attrReal);
        _attrType[4] = new AttrType(AttrType.attrReal);

        _attrSize[0] = REC_LEN1;
        _attrSize[1] = REC_LEN2;
        _attrSize[2] = REC_LEN3;
        _attrSize[3] = REC_LEN4;
        _attrSize[4] = REC_LEN5;

        // create an iterator by open a file scan
        _projlist = new FldSpec[5];
        RelSpec rel = new RelSpec(RelSpec.outer);
        _projlist[0] = new FldSpec(rel, 1);
        _projlist[1] = new FldSpec(rel, 2);
        _projlist[2] = new FldSpec(rel, 3);
        _projlist[3] = new FldSpec(rel, 4);
        _projlist[4] = new FldSpec(rel, 5);

        _relationName = relationName;
        _pref_list = pref_list;
        _pref_list_length = pref_list_length;
        _n_pages = n_pages;
        // _window = new LinkedHashSet<Tuple>(_n_pages);
        _window = new Tuple[_n_pages];

        temp = new Heapfile("sortFirstSkyTemp.in");

        System.out.println("----------   SORT FIRST SKY INIT VARS   -------------");
        System.out.println("Attributes: "+Arrays.toString(_in));
        System.out.println("Attributes length: "+_len_in);
        System.out.println("Relation name: "+_relationName);
        System.out.println("Preferences list: "+ Arrays.toString(_pref_list));
        System.out.println("Preferences list length: "+_pref_list_length);
        System.out.println("Number of buffer pages: "+_n_pages);
        System.out.println("-----------------------------------------------------");

        if ( status == OK )
            computeSkylines(_relationName, _sort, temp);
    }

    public void computeSkylines(String file, SortPref _sort, Heapfile tmp) throws IOException {

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
            t = _sort.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int count = 0;

        while (t != null && count < _n_pages) {
            Tuple temp = new Tuple(t);
             _window[count] = temp;
            // _window.add(temp);
            count++;

            try {
                t = _sort.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }


        // AT THIS point: in-memory window is formed

        System.out.println("In memory objects");
        for(int i=0; i<_window.length; i++) {
            _window[i].print(_attrType);
        }

        FileScan fscan = null;

        try {
            fscan = new FileScan(file, _attrType, _attrSize, (short) 5, 5, _projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            t = fscan.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

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
                    if(TupleUtils.Dominates(htuple, _attrType, _window[i], _attrType, _len_in, _str_sizes, _pref_list, _pref_list_length) == 1){
                        // 2. If tuple in heap file dominates any one in main memory - replace the tuple with the one in main memory
                        System.out.println("Heap tuple");
                        htuple.print(_attrType);
                        System.out.println("Dominates ");
                        _window[i].print(_attrType);
                        System.out.println(" ");
                    }else{
                        // 1. If tuple in heap file is dominated by at least one in main memory - simply discard it from heap
                        isDominatedBy = true;
                        System.out.println("Heap tuple");
                        htuple.print(_attrType);
                        System.out.println("Dominated by ");
                        _window[i].print(_attrType);
                        System.out.println(" ");
                        break;
                    }
                }

                if(!isDominatedBy){
                    // 3. If tuple is not dominated by any tuple in main memory - put into temp heap file
                    // Tuple remained un-dominated -> potential skyline object
                    // check if space left in window
                    // else put in temp heap
                    htuple.print(_attrType);
                    System.out.println("Is not Dominated by any window objects");
                }

            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                t = fscan.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        return;
    }
}
