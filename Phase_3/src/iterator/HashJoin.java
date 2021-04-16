package iterator;


import hashindex.HIndex;
import hashindex.HashIndexWindowedScan;
import hashindex.HashKey;
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import iterator.*;
import btree.*;

import java.awt.*;
import java.lang.*;
import java.io.*;

/**
 * This file contains an implementation of the index nested loops join
 * The algorithm is extremely simple:
 * <p>
 * foreach tuple r in R do
 * foreach s in index lookup in S do
 * if (ri == sj) then add (r, s) to the result.
 */

public class HashJoin extends Iterator {
    private AttrType _in1[], _in2[];
    private int in1_len, in2_len;
    private Iterator outer;
    private short t2_str_sizescopy[], t1_str_sizescopy[];
    private CondExpr OutputFilter[], rightFilter[];
    private CondExpr RightFilter[];
    private int n_buf_pgs;        // # of buffer pages available.
    private boolean done,         // Is the join complete
            get_from_outer;                 // if TRUE, a tuple is got from outer
    private Tuple outer_tuple, inner_tuple;
    private Tuple Jtuple;           // Joined tuple
    private FldSpec perm_mat[];
    private int nOutFlds;
    private Heapfile hf;
    private Scan inner;

    private Operand temp_op;
    private int fld1, fld2;  // index filed for inner relation
    private boolean index_found;
    private String inner_hash_index_name="hash-join-inner-index.unclustered",
                    outer_hash_index_name= "hash-join-outer-index.unclustered",
                    outer_temp_heap_name = "hash-join-outer-heap.in",
                    temp_temp_inner_heap_name = "hash-join-inner-temp-temp-heap.in";
    private int inner_relation_attrType;
    private int key_size, target_utilization;
    private int n_win1, n_win2, n_current;
    private HIndex outer_h, h;
    private HashIndexWindowedScan outer_hiws, inner_hiws;
    private FldSpec[] outer_projection, inner_projection;
    Iterator it=null;

    /**
     * constructor
     * Initialize the two relations which are joined, including relation type,
     *
     * @param in1          Array containing field types of R.
     * @param len_in1      # of columns in R.
     * @param t1_str_sizes shows the length of the string fields.
     * @param in2          Array containing field types of S
     * @param len_in2      # of columns in S
     * @param t2_str_sizes shows the length of the string fields.
     * @param amt_of_mem   IN PAGES
     * @param am1          access method for left i/p to join
     * @param relationName access hfapfile for right i/p to join
     * @param outFilter    select expressions
     * @param rightFilter  reference to filter applied on right i/p
     * @param proj_list    shows what input fields go where in the output tuple
     * @param n_out_flds   number of outer relation fileds
     * @throws IOException         some I/O fault
     * @throws NestedLoopException exception from this class
     */
    public HashJoin(AttrType in1[],
                               int len_in1,
                               short t1_str_sizes[],
                               AttrType in2[],
                               int len_in2,
                               short t2_str_sizes[],
                               int amt_of_mem,
                               Iterator am1,
                               String relationName,
                               CondExpr outFilter[],
                               CondExpr rightFilter[],
                               FldSpec proj_list[],
                               int n_out_flds
    ) throws Exception {

        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1, 0, _in1, 0, in1.length);
        System.arraycopy(in2, 0, _in2, 0, in2.length);
        in1_len = len_in1;
        in2_len = len_in2;

        outer = am1;
        t2_str_sizescopy = t2_str_sizes;
        t1_str_sizescopy = t1_str_sizes;
        inner_tuple = new Tuple();
        Jtuple = new Tuple();
        OutputFilter = outFilter;
        RightFilter = rightFilter;

        n_buf_pgs = amt_of_mem;
        inner = null;
        done = false;
        get_from_outer = true;

        AttrType[] Jtypes = new AttrType[n_out_flds];
        short[] t_size;

        perm_mat = proj_list;
        nOutFlds = n_out_flds;

        int outer_count=0;
        for(int i=0;i<perm_mat.length;i++){
            if(perm_mat[i].relation.key == RelSpec.outer)
                outer_count++;
        }
        outer_projection = new FldSpec[outer_count];
        inner_projection = new FldSpec[n_out_flds-outer_count];
        for(int i=0;i<perm_mat.length;i++){
            if(perm_mat[i].relation.key == RelSpec.outer)
                outer_projection[i] = new FldSpec(new RelSpec(RelSpec.outer), perm_mat[i].offset);
            else
                inner_projection[i] = new FldSpec(new RelSpec(RelSpec.outer), perm_mat[i].offset);
        }

        fld1 = OutputFilter[0].operand1.symbol.offset;
        fld2 = OutputFilter[0].operand2.symbol.offset;

        index_found = false;
        // check if index exists on the inner relation
        // TODO: ??
        /*
        Table t = new Table(relationName);
        if(t.getHash_unclustered_attr()[fld]){
            // index exists
            index_found = true;
            hash_index_name = "" ????
        }
        */
        index_found = false;

        inner_relation_attrType = _in2[fld2].attrType;

        h = new HIndex(inner_hash_index_name, inner_relation_attrType, key_size,target_utilization);
        HashKey key=null;
        Tuple tup;
        RID rid = new RID();
        if(!index_found){
            // will have to create hash index on inner.
            // creating hash index on inner relation.
            Scan s = (new Heapfile(relationName)).openScan();
            while((tup=s.getNext(rid))!=null){
                tup.setHdr((short)in2_len, _in2, t2_str_sizes);
                switch(inner_relation_attrType) {
                    case AttrType.attrInteger:
                        key = new HashKey(tup.getIntFld(fld2));
                        break;
                    case AttrType.attrReal:
                        key = new HashKey(tup.getFloFld(fld2));
                        break;
                    case AttrType.attrString:
                        key = new HashKey(tup.getStrFld(fld2));
                        break;
                    default:
                        System.out.println("Not supposted type for inner relation index.");
                }
                h.insert(key, rid);
            }
        }
        n_win1 = h.get_number_of_buckets();
        // create heap file for outer relation and create hash index on it.
        Heapfile temp_hf = new Heapfile(outer_temp_heap_name);
        outer_h = new HIndex(outer_hash_index_name, inner_relation_attrType, key_size, target_utilization);
        while((tup=am1.get_next())!=null){
            tup.setHdr((short)len_in1, _in1, t1_str_sizes);
            switch(inner_relation_attrType) {
                case AttrType.attrInteger:
                    key = new HashKey(tup.getIntFld(fld2));
                    break;
                case AttrType.attrReal:
                    key = new HashKey(tup.getFloFld(fld2));
                    break;
                case AttrType.attrString:
                    key = new HashKey(tup.getStrFld(fld2));
                    break;
                default:
                    System.out.println("Not supposted type for inner relation index.");
            }
            rid = temp_hf.insertRecord(tup.returnTupleByteArray());
            outer_h.insert(key, rid);
        }
        n_win2 = outer_h.get_number_of_buckets();
        n_current = 0;
        outer_hiws = new HashIndexWindowedScan(new IndexType(IndexType.Hash), outer_temp_heap_name, outer_hash_index_name,
                                                _in1, t1_str_sizes, in1_len, outer_projection.length, outer_projection,
                                        null, fld1, false);
        inner_hiws = new HashIndexWindowedScan(new IndexType(IndexType.Hash), relationName, inner_hash_index_name,
                                                _in2, t2_str_sizes, in2_len, inner_projection.length, inner_projection,
                                        null, fld2, false);
    }

    /**
     * @return The joined tuple is returned
     * @throws IOException               I/O errors
     * @throws JoinsException            some join exception
     * @throws IndexException            exception from super class
     * @throws InvalidTupleSizeException invalid tuple size
     * @throws InvalidTypeException      tuple type not valid
     * @throws PageNotReadException      exception from lower layer
     * @throws TupleUtilsException       exception from using tuple utilities
     * @throws PredEvalException         exception from PredEval class
     * @throws SortException             sort exception
     * @throws LowMemException           memory error
     * @throws UnknowAttrType            attribute type unknown
     * @throws UnknownKeyTypeException   key type unknown
     * @throws Exception                 other exceptions
     */
    public Tuple get_next()
            throws IOException,
            JoinsException,
            IndexException,
            InvalidTupleSizeException,
            InvalidTypeException,
            PageNotReadException,
            TupleUtilsException,
            PredEvalException,
            SortException,
            LowMemException,
            UnknowAttrType,
            UnknownKeyTypeException,
            Exception
    {
        if(n_current > Math.max(n_win1, n_win2) ){
            return null;
        }
        else{
            if(){
                create_heap_for_inner_iterator();
                it = new NestedLoopsJoins(_in1, in1_len, t1_str_sizescopy,
                                            _in2, in2_len, t2_str_sizescopy,
                                            outer_hiws.get_next(), temp_temp_inner_heap_name, OutputFilter, RightFilter, perm_mat, nOutFlds);
            }

        }


    }

    private void create_heap_for_inner_iterator(){

    }

    /*
     * uses the filter on Output table to get the key for index lookup.
     */
    private void set_keys(Tuple outer) {
        CondExpr temp_ptr = OutputFilter[0];
        try {
            switch (temp_ptr.op.attrOperator) {
                case AttrOperator.aopEQ:
                    break;
                case AttrOperator.aopNE:
                    System.out.println("{NE} This operator is not supported.");
                    break;
                case AttrOperator.aopLT:
                    break;
                case AttrOperator.aopLE:
                    break;
                case AttrOperator.aopGT:
                    AttrType temp_ = temp_ptr.type1;
                    temp_ptr.type1 = temp_ptr.type2;
                    temp_ptr.type2 = temp_;

                    temp_op = temp_ptr.operand1;
                    temp_ptr.operand1 = temp_ptr.operand2;
                    temp_ptr.operand2 = temp_op;

                    temp_ptr.op.attrOperator = AttrOperator.aopLT;
                    break;
                case AttrOperator.aopGE:
                    AttrType temp__ = temp_ptr.type1;
                    temp_ptr.type1 = temp_ptr.type2;
                    temp_ptr.type2 = temp__;

                    temp_op = temp_ptr.operand1;
                    temp_ptr.operand1 = temp_ptr.operand2;
                    temp_ptr.operand2 = temp_op;

                    temp_ptr.op.attrOperator = AttrOperator.aopLE;
                    break;
                case AttrOperator.aopNOT:
                    System.out.println("{NOT} This operator is not supported.");
                    break;
                default:
                    break;
            }
            switch (temp_ptr.type1.attrType) {
                case AttrType.attrInteger:
                    break;
                case AttrType.attrReal:
                    break;
                case AttrType.attrString:
                    break;
                case AttrType.attrSymbol:
                    int fld1 = temp_ptr.operand1.symbol.offset;
                    switch (_in1[fld1 - 1].attrType) {
                        case AttrType.attrInteger:
                            break;
                        case AttrType.attrReal:
                            break;
                        case AttrType.attrString:
                            break;
                        default:
                    }
                    break;
            }
            switch (temp_ptr.type2.attrType) {
                case AttrType.attrInteger:
                    break;
                case AttrType.attrReal:
                    break;
                case AttrType.attrString:
                    break;
                case AttrType.attrSymbol:
                    int fld2 = temp_ptr.operand2.symbol.offset;
                    switch (_in1[fld2 - 1].attrType) {
                        case AttrType.attrInteger:
                            break;
                        case AttrType.attrReal:
                            break;
                        case AttrType.attrString:
                            break;
                        default:
                    }
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * implement the abstract method close() from super class Iterator
     * to finish cleaning up
     *
     * @throws IOException    I/O error from lower layers
     * @throws JoinsException join error from lower layers
     * @throws IndexException index access error
     */
    public void close() throws JoinsException, IOException, IndexException {
        if (!closeFlag) {
            try {
                outer.close();
                if(index_found){
//                    btreefile.close();
                }
            } catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}
