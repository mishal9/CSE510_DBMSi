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
import index.IndexException;

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
    int fld, key_size = 4;  // TODO: not sure of the key size.
    private int target_utilization = 80;
    // size of the tuples in the skyline/window/temporary_heap_file
    private int          _tuple_size;
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
        RID rid;
        KeyClass key = null;
        Tuple t = null;

        try {
            t = _am.get_next();
        }
        catch (Exception e) {
            status = false;
            e.printStackTrace();
        }

        while(t != null){
            Tuple tuple = new Tuple(this._tuple_size);
            try {
                tuple.setHdr((short) _len_in, _attrType, _attr_sizes);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InvalidTypeException e) {
                e.printStackTrace();
            } catch (InvalidTupleSizeException e) {
                e.printStackTrace();
            }
            tuple.tupleCopy(t);

            float[] outval = new float[3];
            outval[0] = tuple.getFloFld(1);
            outval[1] = tuple.getFloFld(2);
            outval[2] = tuple.getFloFld(3);

            System.out.println("Iteration tuple: " + outval[0] + " " + outval[1] + " " + outval[2]);

            rid = hf.insertRecord(tuple.returnTupleByteArray());
            switch(_attrType[fld-1].attrType){
                case AttrType.attrInteger:
                    key = new HashKey(tuple.getIntFld(fld));
                    break;
                case AttrType.attrReal:
                    key = new HashKey(tuple.getFloFld(fld));
                    break;
                case AttrType.attrString:
                    key = new HashKey(tuple.getStrFld(fld));
                    break;
                default:
                    System.out.println("Unrecognized attr type");
            }
            hif.insert(key, rid);

            try {
                t = _am.get_next();
            }
            catch (Exception e) {
                status = false;
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws IOException, SortException {

    }
}
