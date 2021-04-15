package iterator;

import btree.KeyClass;
import global.AggType;
import global.AttrType;
import global.IndexType;
import global.RID;
import hashindex.HIndex;
import hashindex.HashIndexWindowedScan;
import hashindex.HashKey;
import heap.*;

import java.io.IOException;
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
    FldSpec[] _agg_list, _proj_list;
    int _n_out_flds;
    int fld, key_size = 4;  // TODO: not sure of the key size.
    private int target_utilization = 80;
    HashIndexWindowedScan hiwfs;
    Iterator it;
    GroupByWithSort grpSort;
    Iterator _am;
    String temp_heap_name = "temp_heap_aeiou1.in",
            hash_index_name = "temp_hash_index.in";  // should and cannot be used by anyone else.

    public GroupByWithHash(
            AttrType[] in1, int len_in1, short[] t1_str_sizes,
            Iterator am1,
            FldSpec group_by_attr,
            FldSpec[] agg_list,
            AggType agg_type,
            FldSpec[] proj_list,
            int n_out_flds,
            int n_pages
        ) throws Exception {
        _attrType = in1;
        _len_in = len_in1;
        _attr_sizes = t1_str_sizes;
        _agg_type = agg_type;
        _n_pages = n_pages;
        _agg_list = agg_list;
        _proj_list = proj_list;
        _n_out_flds = n_out_flds;
        _am = am1;
        fld = group_by_attr.offset;

        int buffer_pages = _n_pages/2;

        create_temp_heap();

        hiwfs = new HashIndexWindowedScan(new IndexType(IndexType.Hash), temp_heap_name, hash_index_name,
                                                                _attrType, _attr_sizes, _len_in, n_out_flds, proj_list, null,
                                                                fld, false);
    }

    public Tuple get_next() throws Exception {
        if(it == null){
            return null;
        }
        else{
            if(grpSort==null){
                it = hiwfs.get_next();
                grpSort = new GroupByWithSort(_attrType,_len_in, _attr_sizes, it, new FldSpec(new RelSpec(RelSpec.outer), fld),
                        _agg_list, _agg_type, _proj_list, _n_out_flds, _n_pages);
            }
            else{
                return grpSort.get_next();
            }
            System.out.println("\n New Bucket ");
        }
        return null;
    }

    public void create_temp_heap()
            throws Exception
    {
        HIndex hif = new HIndex(hash_index_name, AttrType.attrInteger, key_size,target_utilization);
        Heapfile hf = new Heapfile(temp_heap_name);
        Tuple tup = new Tuple();
        RID rid = new RID();
        KeyClass key = null;
        while((tup= _am.get_next())!=null){
            tup.setHdr((short)_len_in, _attrType, _attr_sizes);
            rid = hf.insertRecord(tup.returnTupleByteArray());
            switch(_attrType[fld-1].attrType){
                case AttrType.attrInteger:
                    key = new HashKey(tup.getIntFld(fld));
                    break;
                case AttrType.attrReal:
                    key = new HashKey(tup.getFloFld(fld));
                    break;
                case AttrType.attrString:
                    key = new HashKey(tup.getStrFld(fld));
                    break;
                default:
                    System.out.println("Unrecognized attr type");
            }
            hif.insert(key, rid);
        }
    }
}
