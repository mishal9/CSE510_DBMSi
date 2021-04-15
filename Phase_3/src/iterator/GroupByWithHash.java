package iterator;

import global.AggType;
import global.AttrType;
import heap.Tuple;

import java.util.LinkedList;

public class GroupByWithHash {
    public static Tuple[] _result;
    private static AttrType[] _attrType;
    private static int _len_in;
    private static boolean status = true;
    private static short[] _attr_sizes;
    private static AggType _agg_type;
    private static int idx;
    private int _n_pages;

    GroupByWithHash(
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
        int buffer_pages = _n_pages/2;

    }
}
