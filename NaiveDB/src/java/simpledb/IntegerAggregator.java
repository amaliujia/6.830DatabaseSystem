package simpledb;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import lombok.Getter;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    @Getter
    private Type gbFieldType;
    private int afield;
    private Op aggregatorOp;

    @Getter
    private TreeMap<Field, Integer> integerAggregator;
    private TreeMap<Field, Integer> counter;

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

        integerAggregator = new TreeMap<Field, Integer>();
        counter = new TreeMap<Field, Integer>();
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
            integerAggregator.put(gbKey, aggreate(gbKey,Integer.MAX_VALUE, ((IntField)gbValue).getValue(), true));
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
                    } else {
                        ret = prevValue;
                    }
                    break;
                case MAX:
                    if (prevValue < curValue) {
                        ret = curValue;
                    } else {
                        ret = prevValue;
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
        return new IntegerAggregatorIterator(this);
    }

    private class IntegerAggregatorIterator implements OpIterator {
        private TreeMap<Field, Integer> integerAggregator;
        private boolean isOpen;
        private Tuple nextTuple;
        private Iterator<Map.Entry<Field, Integer>> keyValueIter;

        private TupleDesc tupleDesc;


        public IntegerAggregatorIterator(IntegerAggregator aggregator) {
            integerAggregator = aggregator.getIntegerAggregator();
            isOpen = false;
            nextTuple = null;

            tupleDesc = new TupleDesc(new Type[] {aggregator.getGbFieldType(), Type.INT_TYPE});
        }

        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
            keyValueIter = integerAggregator.entrySet().iterator();
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                throw new DbException("Have not opened this iterator");
            }

            if (nextTuple == null) {
                nextTuple = fetchNext();
                if (nextTuple == null) {
                    return false;
                }
            }

            return true;
        }

        private Tuple fetchNext() {
            if (keyValueIter.hasNext()) {
                Map.Entry<Field, Integer> nextKeyValuePair = keyValueIter.next();
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, nextKeyValuePair.getKey());
                tuple.setField(1, new IntField(nextKeyValuePair.getValue()));
                return tuple;
            } else {
                return null;
            }
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext()) {
                Tuple ret = nextTuple;
                nextTuple = fetchNext();
                return ret;
            } else {
                throw new NoSuchElementException("Next tuple does not exist");
            }

        }

        public void rewind() throws DbException, TransactionAbortedException {
            keyValueIter = integerAggregator.entrySet().iterator();
            nextTuple = null;
        }

        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        public void close() {
            isOpen = false;
            nextTuple = null;
            keyValueIter = null;
        }
    }

}
