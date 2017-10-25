package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    private Type gbFieldType;
    private int afield;
    private Op aggregatorOp;

    private HashMap<Field, Integer> integerAggregator;
    private HashMap<Field, Integer> counter;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.afield = afield;
        this.aggregatorOp = what;

        integerAggregator = new HashMap<Field, Integer>();
        counter = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbKey = tup.getField(this.gbField);
        Field gbValue = tup.getField(this.afield);

        if (integerAggregator.containsKey(gbKey)) {
            integerAggregator.put(gbKey, aggreate(gbKey, integerAggregator.get(gbKey), ((IntField)gbValue).getValue(), false));
        } else {
            integerAggregator.put(gbKey, aggreate(gbKey,0, ((IntField)gbValue).getValue(), true));
        }
    }

    private Integer aggreate(Field gbKey, Integer prevValue, Integer curValue, boolean initFlag) {
        Integer ret = null;
        if (initFlag) {
            switch (this.aggregatorOp){
                case MIN:
                case MAX:
                case SUM:
                    ret = curValue;
                    break;
                case AVG:
                    ret = curValue;
                    counter.put(gbKey, 1);
                    break;
                case COUNT:
                    ret = 1;
                    break;
                default:
                    ret = null;
                    break;
            }
        } else {
            switch (this.aggregatorOp) {
                case MIN:
                    if (prevValue > curValue) {
                        ret = curValue;
                    }
                    break;
                case MAX:
                    if (prevValue < curValue) {
                        ret = curValue;
                    }
                    break;
                case SUM:
                    ret = prevValue + curValue;
                    break;
                case AVG:
                    ret = (curValue + prevValue * counter.get(gbKey)) / (counter.get(gbKey) + 1);
                    counter.put(gbKey, counter.get(gbKey) + 1);
                    break;
                case COUNT:
                    ret = prevValue;
                    break;
            }
        }
        return ret;
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

    }

}
