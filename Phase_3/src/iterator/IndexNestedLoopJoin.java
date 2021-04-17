package iterator;


import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;
import iterator.*;
import btree.*;

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

public class IndexNestedLoopJoin extends Iterator {
    private AttrType _in1[], _in2[];
    private int in1_len, in2_len, fld1, fld2;
    private Iterator outer;
    private short t2_str_sizescopy[];
    private CondExpr OutputFilter[];
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

    private BTreeFile btreefile;
    private BTFileScan b_inner;
    private KeyClass hi_key = null, lo_key = null;
    private Operand temp_op;
    private boolean index_found = false;  //TODO: Write code to modify this variable
    private int inner_proj_count;
    private FldSpec inner_proj[];
    private FldSpec outer_proj[];
    private String RelationName;
    private String index_name;
    private CondExpr[] outFilter;
    IndexScan iscan;
    int indexType;
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
    public IndexNestedLoopJoin(AttrType in1[],
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
    ) throws IOException, NestedLoopException, InvalidTupleSizeException {

        _in1 = new AttrType[in1.length];
        _in2 = new AttrType[in2.length];
        System.arraycopy(in1, 0, _in1, 0, in1.length);
        System.arraycopy(in2, 0, _in2, 0, in2.length);
        in1_len = len_in1;
        in2_len = len_in2;

        outer = am1;
        t2_str_sizescopy = t2_str_sizes;
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
        RelationName = relationName;
        perm_mat = proj_list;
        nOutFlds = n_out_flds;

        try {
            t_size = TupleUtils.setup_op_tuple(Jtuple, Jtypes,
                    in1, len_in1, in2, len_in2,
                    t1_str_sizes, t2_str_sizes,
                    proj_list, nOutFlds);
        } catch (TupleUtilsException e) {
            throw new NestedLoopException(e, "TupleUtilsException is caught by NestedLoopsJoins.java");
        }
        try{
            fld2 = OutputFilter[0].operand2.symbol.offset;

            inner_proj_count = 0;
            for (FldSpec fldSpec : proj_list) {
                if (fldSpec.relation.key == RelSpec.innerRel) {
                    inner_proj_count += 1;
                }
            }

            inner_proj = new FldSpec[inner_proj_count];
            outer_proj = new FldSpec[proj_list.length - inner_proj_count];
            int j = 0, k = 0;
            for (FldSpec fldSpec : proj_list) {
                if (fldSpec.relation.key == RelSpec.innerRel) {
                    inner_proj[j] = new FldSpec(new RelSpec(RelSpec.outer), fldSpec.offset);
                    j += 1;
                } else {
                    outer_proj[k] = fldSpec;
                    k += 1;
                }
            }

            Table table = SystemDefs.JavabaseDB.get_relation(relationName);
            if ( table == null) {       // TODO: removing extra booleans after integreating with task6
                System.err.println("ERROR: Table does not exist**");
                return;
            }
            if ( table.getBtree_unclustered_attr()[fld2] ) {  // TODO: removing extra booleans after integreating with task6
                // unclustered btree exists on fld2
                indexType = IndexType.B_Index;
                index_name = table.get_unclustered_index_filename(fld2, "btree");
                index_found = true;
            }
            else if( table.getHash_unclustered_attr()[fld2] ) {
                // unclustered hash exist on fld2
                indexType = IndexType.Hash;
                index_name = table.get_unclustered_index_filename(fld2, "hash");
                index_found = true;
            }
            else if( table.getClustered_btree_attr() == fld2 ){
                // we have btree clustered
                indexType = IndexType.B_Index;
                index_name = table.get_clustered_index_filename(fld2, "btree");
                index_found = true;
            }
            else if( table.getClustered_hash_attr() == fld2 ){
                // we have btree clustered
                indexType = IndexType.Hash;
                index_name = table.get_clustered_index_filename(fld2, "hash");
                index_found = true;
            }
            else{
                index_found = false;
                hf = new Heapfile(relationName);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
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
            Exception {
        // This is a DUMBEST form of a join, not making use of any key information...


        if (done)
            return null;

        do {
            // If get_from_outer is true, Get a tuple from the outer, delete
            // an existing scan on the file, and reopen a new scan on the file.
            // If a get_next on the outer returns DONE?, then the nested loops
            //join is done too.

            if (get_from_outer == true) {
                get_from_outer = false;
                if (inner != null)     // If this not the first time,
                {
                    // close scan
                    inner = null;
                }
                if ((outer_tuple = outer.get_next()) == null) {
                    done = true;
                    if (inner != null) {
                        inner = null;
                    }
                    return null;
                }
                if (index_found == true) {
                    set_keys(outer_tuple);
                    switch(indexType) {
                        case IndexType.B_Index:
                            iscan = new IndexScan(new IndexType(IndexType.B_Index), RelationName, index_name, _in2, t2_str_sizescopy, in2_len, inner_proj_count, inner_proj, outFilter, fld2, false);
                            break;
                        default:
                            System.out.println("What index~");
                    }
                }
            }  // ENDS: if (get_from_outer == TRUE)


            // The next step is to get a tuple from the inner,
            // while the inner is not completely scanned && there
            // is no match (with pred),get a tuple from the inner.

            RID rid = null;
            if (index_found) {
                inner_tuple = iscan.get_next();
            } else {
                rid = new RID();
                inner_tuple = inner.getNext(rid);
            }
            while (inner_tuple != null) {
                inner_tuple.setHdr((short) in2_len, _in2, t2_str_sizescopy);
                // these inner checks makes sure of conditions other than the index attribute => having or not having index doesnot matter
                if (PredEval.Eval(RightFilter, inner_tuple, null, _in2, null)) {
                    if (PredEval.Eval(OutputFilter, outer_tuple, inner_tuple, _in1, _in2)) {
                        // Apply a projection on the outer and inner tuples.
                        Projection.Join(outer_tuple, _in1,
                                inner_tuple, _in2,
                                Jtuple, perm_mat, nOutFlds);
                        return Jtuple;
                    }
                }
                if (index_found) {
                    inner_tuple = iscan.get_next();
                } else {
                    rid = new RID();
                    inner_tuple = inner.getNext(rid);
                }
            }
            // There has been no match. (otherwise, we would have
            //returned from t//he while loop. Hence, inner is
            //exhausted, => set get_from_outer = TRUE, go to top of loop

            get_from_outer = true; // Loop back to top and get next outer tuple.
        } while (true);
    }

    /*
     * uses the filter on Output table to get the key for index lookup.
     */
    private void set_keys(Tuple outer) {
        CondExpr temp_ptr = OutputFilter[0];
        outFilter = new CondExpr[2];
        outFilter[0] = new CondExpr();
        outFilter[0].next = null;
        outFilter[1] = null;

        try {

            switch (temp_ptr.op.attrOperator) {
                case AttrOperator.aopNE:
                    System.out.println("{NE} This operator is not supported.");
                    break;
                case AttrOperator.aopNOT:
                    System.out.println("{NOT} This operator is not supported.");
                    break;
                case AttrOperator.aopGT:
                    outFilter[0].op = new AttrOperator(AttrOperator.aopLT);
                    break;
                case AttrOperator.aopGE:
                    outFilter[0].op = new AttrOperator(AttrOperator.aopLE);
                    break;
                case AttrOperator.aopLT:
                    outFilter[0].op = new AttrOperator(AttrOperator.aopGT);
                    break;
                case AttrOperator.aopLE:
                    outFilter[0].op = new AttrOperator(AttrOperator.aopGE);
                    break;
                default:
                    outFilter[0].op = new AttrOperator(temp_ptr.op.attrOperator);
                    break;
            }

            outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
            switch (temp_ptr.type2.attrType) {
                case AttrType.attrSymbol:
                    fld2 = temp_ptr.operand2.symbol.offset;
                    outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), fld2);
                    break;
                default:
                    System.out.println("This should be a symbol");

            }

            switch (temp_ptr.type1.attrType) {
                case AttrType.attrSymbol:
                    fld1 = temp_ptr.operand1.symbol.offset;
                    switch (_in1[fld1-1].attrType){
                        case AttrType.attrInteger:
                            outFilter[0].type2 = new AttrType(AttrType.attrInteger);
                            outFilter[0].operand2.integer = outer.getIntFld(fld1);
                            break;
                        case AttrType.attrString:
                            outFilter[0].type2 = new AttrType(AttrType.attrString);
                            outFilter[0].operand2.string = outer.getStrFld(fld1);
                            break;
                        case AttrType.attrReal:
                            outFilter[0].type2 = new AttrType(AttrType.attrReal);
                            outFilter[0].operand2.real = outer.getFloFld(fld1);
                            break;
                        default:
                            System.out.println("Unknown type: " + temp_ptr.type1.attrType);
                    }
                    break;
                default:
                    System.out.println("type 2 cannot be symbol");
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
                    iscan.close();
                }
            } catch (Exception e) {
                throw new JoinsException(e, "NestedLoopsJoin.java: error in closing iterator.");
            }
            closeFlag = true;
        }
    }
}





