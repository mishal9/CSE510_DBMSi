package iterator;

import bufmgr.PageNotReadException;
import global.AggType;
import global.AttrType;
import global.RID;
import global.TupleOrder;
import heap.*;
import index.IndexException;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

import static global.GlobalConst.MINIBASE_PAGESIZE;

public class GroupByWithSort extends Iterator{
    public static Tuple[] _result;
    private static Sort _sort;
    private static AttrType[] _attrType;
    private static int _len_in;
    private static boolean status = true;
    private static short[] _attr_sizes;
    private static AggType _agg_type;
    private static int idx;

    /* Windows which is used to store some elements for iteration */
    private Queue<Tuple> _queue;

    /* number of tuples the queue can hold */
    private int          _window_size;

    /* tuples used for aggregation -- for debug purposes */
    private Tuple        tuple_candidate;

    /* heap file containing our data on which skyline is computed */
    private Heapfile _skyline_grp_heap;

    /* size of the tuples in the skyline/window/temporary_heap_file */
    private int          _tuple_size;

    private int _n_pages;

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
        _queue = new LinkedList<>();
        _n_pages = n_pages;
        int buffer_pages = _n_pages/2;

        /* initialise tuple size */
        try {
            this.tuple_candidate = new Tuple();
            this.tuple_candidate.setHdr((short) this._len_in, this._attrType, this._attr_sizes);
            this._tuple_size = this.tuple_candidate.size();
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

        // workflow for MIN | MAX | AVG
        /*
        1. Keep adding the elements to the queue while queue is still not full | sort has some elements left
        2. Keep polling the elements from the queue
        3. For minimum on the aggregation attribute, keep checking against the new global minimum (same applies for max)
            3a. For avg, keep taking the avg until new group found
            3b. if new group found - reset the vars and put the result of the previous group onto the buffer
        */

        Tuple t = null;
        float lastPolled = 0.0f;
        try {
            t = _sort.get_next();
        }
        catch (Exception e) {
            status = false;
            e.printStackTrace();
        }

        while(t != null){
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

            try {
                if(outer.getFloFld(group_by_attr.offset) != lastPolled && lastPolled != 0.0f){
                    // reset here
                    float aggr_val = _agg_type.aggType == AggType.AVG ? 0.0f : _agg_type.aggType == AggType.MIN ? Float.MAX_VALUE : -Float.MIN_VALUE;
                    int group_size = 1;
                    float grp_result = 0.0f;
                    try {
                        _skyline_grp_heap = new Heapfile("skyline_group_by.in");
                    }
                    catch (Exception e) {
                        System.err.println("Could not open the skyline heapfile");
                        e.printStackTrace();
                    }

                    RID rid;

                    while(!_queue.isEmpty()){
                        Tuple inner = _queue.poll();

                        if(_agg_type.aggType == AggType.MIN) {
                            aggr_val = Math.min(aggr_val, inner.getFloFld(agg_list[0].offset));
                            grp_result = aggr_val;
                        }
                        else if(_agg_type.aggType == AggType.MAX) {
                            aggr_val = Math.max(aggr_val, inner.getFloFld(agg_list[0].offset));
                            grp_result = aggr_val;
                        }
                        else if(_agg_type.aggType == AggType.AVG) {
                            aggr_val += inner.getFloFld(agg_list[0].offset);
                            grp_result = aggr_val / group_size;
                        }else if(_agg_type.aggType == AggType.SKYLINE){
                            // pass on the result tuples to the existing skyline computation method

                            try {
                                rid = _skyline_grp_heap.insertRecord(inner.returnTupleByteArray());
                            }
                            catch (Exception e) {
                                status = false;
                                e.printStackTrace();
                            }


                        }
                        group_size += 1;
                    }

                    System.out.println("Aggregation in grp " + lastPolled + " : "+grp_result);
                    System.out.println("Skyline in group "+lastPolled+" : " +_skyline_grp_heap.getRecCnt());

                    // Construct result tuple
                    Tuple result = new Tuple(this._tuple_size);
                    try {
                        result.setHdr((short) _len_in, _attrType, _attr_sizes);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InvalidTypeException e) {
                        e.printStackTrace();
                    } catch (InvalidTupleSizeException e) {
                        e.printStackTrace();
                    }
                    result.tupleCopy(outer);

                    result.setFloFld(group_by_attr.offset, lastPolled);
                    result.setFloFld(agg_list[0].offset, grp_result);

                    if(idx < _window_size)
                        _result[idx++] = result;

                    _skyline_grp_heap.deleteFile();
                    System.out.println();
                }

                _queue.offer(outer);
                lastPolled = outer.getFloFld(group_by_attr.offset);
            }
            catch (Exception e) {
                status = false;
                e.printStackTrace();
            }

            try {
                t = _sort.get_next();
            }
            catch (Exception e) {
                status = false;
                e.printStackTrace();
            }
        }

        // still the last group remains in the queue
        float aggr_val = _agg_type.aggType == AggType.AVG ? 0.0f : _agg_type.aggType == AggType.MIN ? Float.MAX_VALUE : -Float.MIN_VALUE;
        int group_size = 1;
        float grp_result = 0.0f;
        RID rid;
        try {
            _skyline_grp_heap = new Heapfile("skyline_group_by.in");
        }
        catch (Exception e) {
            System.err.println("Could not open the skyline heapfile");
            e.printStackTrace();
        }

        while(!_queue.isEmpty()){
            Tuple inner = _queue.poll();

            try {
                if(_agg_type.aggType == AggType.MIN) {
                    aggr_val = Math.min(aggr_val, inner.getFloFld(agg_list[0].offset));
                    grp_result = aggr_val;
                }
                else if(_agg_type.aggType == AggType.MAX) {
                    aggr_val = Math.max(aggr_val, inner.getFloFld(agg_list[0].offset));
                    grp_result = aggr_val;
                }
                else if(_agg_type.aggType == AggType.AVG) {
                    aggr_val += inner.getFloFld(agg_list[0].offset);
                    grp_result = aggr_val / group_size;
                }else if(_agg_type.aggType == AggType.SKYLINE){
                    // pass on the result tuples to the existing skyline computation method

                    try {
                        rid = _skyline_grp_heap.insertRecord(inner.returnTupleByteArray());
                    }
                    catch (Exception e) {
                        status = false;
                        e.printStackTrace();
                    }

                }

            group_size += 1;

            } catch (IOException e) {
                e.printStackTrace();
            } catch (FieldNumberOutOfBoundException e) {
                e.printStackTrace();
            }
        }

        try {
            System.out.println("Skyline in group "+lastPolled+" : " +_skyline_grp_heap.getRecCnt());
        } catch (InvalidSlotNumberException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            _skyline_grp_heap.deleteFile();
        } catch (InvalidSlotNumberException e) {
            e.printStackTrace();
        } catch (FileAlreadyDeletedException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (HFBufMgrException e) {
            e.printStackTrace();
        } catch (HFDiskMgrException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        // Construct result tuple
        Tuple result = new Tuple(this._tuple_size);
        try {
            result.setHdr((short) _len_in, _attrType, _attr_sizes);
            result.setFloFld(group_by_attr.offset, lastPolled);
            result.setFloFld(agg_list[0].offset, grp_result);

            if(idx < _window_size)
                _result[idx++] = result;

            System.out.println("Aggregation in grp " + lastPolled + " : "+grp_result);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        }

        // workflow for SKYLINE
        /*
        1. Keep polling the queue until empty
        2. While the aggregation attribute value same as the previous one -> keep accumulating current group in the heap file
        3. once grouping breaks - pass on the heap file to the skyline method
        */

        for(int i=0; i<_result.length; i++){
            Tuple res = _result[i];
            if(res != null) {
                try {
                    float[] outval = new float[3];
                    outval[0] = res.getFloFld(1);
                    outval[1] = res.getFloFld(2);
                    outval[2] = res.getFloFld(3);

                    System.out.println("Result: " + outval[0] + " " + outval[1] + " " + outval[2]);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (FieldNumberOutOfBoundException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    public Tuple get_next() throws IOException, JoinsException, IndexException, InvalidTupleSizeException, InvalidTypeException, PageNotReadException, TupleUtilsException, PredEvalException, SortException, LowMemException, UnknowAttrType, UnknownKeyTypeException {
        return null;
    }

    @Override
    public void close() throws IOException, SortException {
        _sort.close();
    }
}
