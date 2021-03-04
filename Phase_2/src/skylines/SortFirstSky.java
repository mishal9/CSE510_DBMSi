package skylines;

import global.AttrType;
import global.GlobalConst;
import heap.*;
import iterator.*;

import java.io.IOException;
import java.util.Arrays;

import static tests.TestDriver.FAIL;
import static tests.TestDriver.OK;

public class SortFirstSky implements GlobalConst {

    private static int _n_pages;
    private static String _relationName;
    private static int[] _pref_list;
    private static int _pref_list_length;
    private Iterator _fscan;
    private Sort _sort;
    private static AttrType[] _in;
    private static int _len_in;
    private static short[] _str_sizes;
    private static Tuple[] window;
    private static Heapfile temp;
    boolean status = OK;
    private static short REC_LEN1 = 160;
    private static short REC_LEN2 = 160;
    private static short REC_LEN3 = 160;
    private static short REC_LEN4 = 160;
    private static short REC_LEN5 = 160;

    public SortFirstSky(AttrType[] in1, int len_in1, short[] t1_str_sizes,
                        Iterator am1, java.lang.String
                         relationName, int[] pref_list, int pref_list_length,
                        int n_pages) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {

        _in = in1;
        _len_in = len_in1;
        _str_sizes = t1_str_sizes;
        _sort = (Sort) am1;
        _fscan = null;

        AttrType[] attrType = new AttrType[5];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        attrType[2] = new AttrType(AttrType.attrReal);
        attrType[3] = new AttrType(AttrType.attrReal);
        attrType[4] = new AttrType(AttrType.attrReal);

        short[] attrSize = new short[5];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        attrSize[2] = REC_LEN3;
        attrSize[3] = REC_LEN4;
        attrSize[4] = REC_LEN5;

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[5];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);
        projlist[4] = new FldSpec(rel, 5);

        try {
            // this file will already be sorted
            _fscan = new FileScan("test1sortFirstSky.in", attrType, attrSize, (short) 5, 5, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        _relationName = relationName;
        _pref_list = pref_list;
        _pref_list_length = pref_list_length;
        _n_pages = n_pages;
        temp = new Heapfile("sortFirstSkyTemp.in");
        window = new Tuple[_n_pages];

        System.out.println("----------   SORT FIRST SKY INIT VARS   -------------");
        System.out.println("Relation name: "+_relationName);
        System.out.println("Preferences list: "+ Arrays.toString(_pref_list));
        System.out.println("Preferences list length: "+_pref_list_length);
        System.out.println("Number of buffer pages: "+_n_pages);
        System.out.println("Window: "+window.length);
        System.out.println("-----------------------------------------------------");

        computeSkylines(_fscan, _sort);
    }

    public void computeSkylines(Iterator _fscan, Sort _sort){

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



        return;
    }
}
