package iterator;

import bufmgr.PageNotReadException;
import diskmgr.PCounter;
import global.AggType;
import global.AttrType;
import global.RID;
import global.TupleOrder;
import heap.*;
import index.IndexException;
import java.io.IOException;
import java.util.Arrays;

import static global.GlobalConst.MINIBASE_PAGESIZE;

public class GroupByWithSort extends Iterator{
    public static Tuple[] _result;
    private static Sort _sort;
    private static AttrType[] _attrType;
    private static FldSpec _group_by_attr;
    private static int _len_in;
    private static boolean status = true;
    private static short[] _attr_sizes;
    private static AggType _agg_type;
    private static FldSpec[] _agg_list;
    private static int idx;

    // number of tuples the queue can hold
    private int          _window_size;

    // heap file containing our data on which skyline is computed
    private Heapfile _skyline_grp_heap;

    // size of the tuples in the skyline/window/temporary_heap_file
    private int          _tuple_size;

    // Value of the aggregation attribute in the last tuple
    private static float _lastPolled = 0.0f;

    // buffer pages allocation
    private int _n_pages;

    // get the next immediate tuple
    private Tuple _next;

    private static float _aggr_val;
    private static int _group_size;
    private static float _grp_result;

    public GroupByWithSort(
            AttrType[] in1, int len_in1, short[] t1_str_sizes,
            Iterator am1,
            FldSpec group_by_attr,
            FldSpec[] agg_list,
            AggType agg_type,
            FldSpec[] proj_list,
            int n_out_flds,
            int n_pages
    ){

        _attrType = in1;
        _len_in = len_in1;
        _attr_sizes = t1_str_sizes;
        _agg_type = agg_type;
        _n_pages = n_pages;
        _agg_list = agg_list;
        _group_by_attr = group_by_attr;

        _aggr_val = _agg_type.aggType == AggType.AVG ? 0.0f : _agg_type.aggType == AggType.MIN ? Float.MAX_VALUE : -Float.MIN_VALUE;
        _group_size = 1;
        _grp_result = 0.0f;

        int buffer_pages = _n_pages/2;

        /* initialise tuple size */
        try {
            Tuple tuple_candidate = new Tuple();
            tuple_candidate.setHdr((short) this._len_in, this._attrType, this._attr_sizes);
            this._tuple_size = tuple_candidate.size();
        } catch (InvalidTypeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        _window_size = ((int)(MINIBASE_PAGESIZE/this._tuple_size))*(_n_pages - buffer_pages);
        _result = new Tuple[_window_size];

        try {
            _skyline_grp_heap = new Heapfile("skyline_group_by.in");
        }
        catch (Exception e) {
            System.err.println("Could not open the skyline heapfile");
            e.printStackTrace();
        }

        try {
            _sort = new Sort(_attrType, (short) _len_in, _attr_sizes, (FileScan) am1, group_by_attr.offset, new TupleOrder(TupleOrder.Descending), 32, buffer_pages);
        }
        catch (Exception e) {
            status = false;
            e.printStackTrace();
        }

        if(status) {
            System.out.println("Group by operator has been initialised with params");
            System.out.println("Window size: "+_window_size);
            System.out.println("Buffer for sorting: "+buffer_pages);
            System.out.println("Group by attribute: "+group_by_attr.offset);
            System.out.println("Aggregation type: "+_agg_type);
            System.out.println("Aggregation attribute: "+Arrays.toString(agg_list));
            System.out.println("Projection list: "+ Arrays.toString(proj_list));
        }else{
            System.exit(-1);
        }

    }

    public void skyline_Aggregation(String skyline_grp_heap, FldSpec[] pref_list, AttrType[] attrType, short[] attrSize, int buffer){
        BlockNestedLoopsSky blockNestedLoopsSky = null;
        int numSkyEle = 0;

        int[] preference_list = new int[pref_list.length];
        for(int i=0; i<pref_list.length; i++){
            preference_list[i] = pref_list[i].offset;
        }

        try {
            blockNestedLoopsSky = new BlockNestedLoopsSky(attrType,
                    (short)3,
                    attrSize,
                    null,
                    skyline_grp_heap,
                    preference_list,
                    preference_list.length,
                    buffer);

            System.out.println("Printing the Block Nested Loop Skyline");
            Tuple temp;
            try {
                temp = blockNestedLoopsSky.get_next();
                while (temp!=null) {
                    float[] outval = new float[3];
                    outval[0] = temp.getFloFld(1);
                    outval[1] = temp.getFloFld(2);
                    outval[2] = temp.getFloFld(3);

                    System.out.println("Skyline Result: " + outval[0] + " " + outval[1] + " " + outval[2]);
                    numSkyEle++;
                    temp = blockNestedLoopsSky.get_next();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (IOException | FileScanException | TupleUtilsException | InvalidRelation e) {
            e.printStackTrace();
        } finally {
            blockNestedLoopsSky.close();
        }
        System.out.println("Skyline Length: "+numSkyEle);
    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException, FieldNumberOutOfBoundException, FileAlreadyDeletedException, HFBufMgrException, InvalidSlotNumberException, HFDiskMgrException {

        Tuple t = null;

        try {
            t = _next == null ? _sort.get_next() : _next;
            _lastPolled = t.getFloFld(_group_by_attr.offset);
        }
        catch (Exception e) {
            status = false;
            e.printStackTrace();
        }

        RID rid;

        while(t != null && t.getFloFld(_group_by_attr.offset) == _lastPolled){
            Tuple outer = new Tuple(this._tuple_size);
            try {
                outer.setHdr((short) _len_in, _attrType, _attr_sizes);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InvalidTypeException e) {
                e.printStackTrace();
            } catch (InvalidTupleSizeException e) {
                e.printStackTrace();
            }
            outer.tupleCopy(t);

            _group_size += 1;

            if (_agg_type.aggType == AggType.MIN) {
                _aggr_val = Math.min(_aggr_val, outer.getFloFld(_agg_list[0].offset));
                _grp_result = _aggr_val;
            } else if (_agg_type.aggType == AggType.MAX) {
                _aggr_val = Math.max(_aggr_val, outer.getFloFld(_agg_list[0].offset));
                _grp_result = _aggr_val;
            } else if (_agg_type.aggType == AggType.AVG) {
                _aggr_val += outer.getFloFld(_agg_list[0].offset);
                _grp_result = _aggr_val / _group_size;
            } else if (_agg_type.aggType == AggType.SKYLINE) {
                // add to skyline group heap
                try {
                    rid = _skyline_grp_heap.insertRecord(outer.returnTupleByteArray());
                }
                catch (Exception e) {
                    status = false;
                    e.printStackTrace();
                }
            }

            _lastPolled = outer.getFloFld(_group_by_attr.offset);

            try {
                t = _sort.get_next();
                _next = t;
            }
            catch (Exception e) {
                status = false;
                e.printStackTrace();
            }
        }

        // reset here
        System.out.println("Aggregation in grp " + _lastPolled + " : "+_grp_result);

        _aggr_val = _agg_type.aggType == AggType.AVG ? 0.0f : _agg_type.aggType == AggType.MIN ? Float.MAX_VALUE : -Float.MIN_VALUE;
        _group_size = 1;
        _grp_result = 0.0f;

        // Compute Skyline here
        skyline_Aggregation("skyline_group_by.in", _agg_list, _attrType, _attr_sizes, 20);

        // delete heap file
        _skyline_grp_heap.deleteFile();

        // create heap file again
        try {
            _skyline_grp_heap = new Heapfile("skyline_group_by.in");
        }
        catch (Exception e) {
            System.err.println("Could not open the skyline heapfile");
            e.printStackTrace();
        }

        // Construct result tuple here

        System.out.println();

        return null;
    }

    @Override
    public void close() throws IOException, SortException {
        _sort.close();
    }
}
