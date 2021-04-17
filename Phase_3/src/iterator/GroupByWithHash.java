package iterator;

import global.AggType;
import global.AttrType;

import hashindex.HashIndexWindowedScan;
import heap.Tuple;

import java.io.IOException;

public class GroupByWithHash extends Iterator{
    public static Tuple[] _result;
    private static AttrType[] _attrType;
    private static int _len_in;
    private static boolean status = true;
    private static short[] _attr_sizes;
    private static AggType _agg_type;
    private static int idx;
    private int _n_pages;
    FldSpec[] _agg_list, _proj_list;
    int _n_out_flds;
    int fld;

    HashIndexWindowedScan _hiwfs;
    GroupByWithSort grpSort;

    public GroupByWithHash(
            AttrType[] in1, int len_in1, short[] t1_str_sizes,
            HashIndexWindowedScan am1,
            FldSpec group_by_attr,
            FldSpec[] agg_list,
            AggType agg_type,
            FldSpec[] proj_list,
            int n_out_flds,
            int n_pages
        )  {
        _attrType = in1;
        _len_in = len_in1;
        _attr_sizes = t1_str_sizes;
        _agg_type = agg_type;
        _n_pages = n_pages;
        _agg_list = agg_list;
        _proj_list = proj_list;
        _n_out_flds = n_out_flds;
        _hiwfs = am1;
        fld = group_by_attr.offset;
    }


    public Tuple get_next() throws Exception {
        Tuple tup;
        Iterator it;
        if((it=_hiwfs.get_next())!=null){
            grpSort = new GroupByWithSort(_attrType,_len_in, _attr_sizes, it, new FldSpec(new RelSpec(RelSpec.outer), fld),
                    _agg_list, _agg_type, _proj_list, _n_out_flds, _n_pages);
            while((tup=grpSort.get_next())!=null){
                tup.setHdr((short)3, _attrType, _attr_sizes);
                tup.print(_attrType);
            }
        }

        System.out.println();
        return null;
    }

    @Override
    public void close() throws IOException, SortException {

    }
}
