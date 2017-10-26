package simpledb;

import java.util.*;
import lombok.Getter;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;
    @Getter
    private Type gbFieldType;
    private int afield;
    private Op aggregatorOp;

    @Getter
    private TreeMap<Field, Set<String>> stringAggregator;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Only support COUNT type");
        }

        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.afield = afield;
        this.aggregatorOp = what;
        stringAggregator = new TreeMap<Field, Set<String>>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbKey = tup.getField(this.gbField);
        StringField gbValue = (StringField)tup.getField(this.afield);

        switch (aggregatorOp) {
            case COUNT:
                if (stringAggregator.containsKey(gbKey)) {
                    stringAggregator.get(gbKey).add(gbValue.getValue());
                } else {
                    HashSet<String> set = new HashSet<String>();
                    set.add(gbValue.getValue());
                    stringAggregator.put(gbKey, set);
                }
                break;
            default:
                break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggregatorIterator(this);
    }

    private class StringAggregatorIterator implements OpIterator {
        private TreeMap<Field, Set<String>> stringAggregator;
        private boolean isOpen;
        private Tuple nextTuple;
        private Iterator<Map.Entry<Field, Set<String>>> keyValueIter;

        private TupleDesc tupleDesc;

        public StringAggregatorIterator(StringAggregator aggregator) {
            stringAggregator = aggregator.getStringAggregator();
            tupleDesc = new TupleDesc(new Type[] {aggregator.getGbFieldType(), Type.INT_TYPE});
        }

        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
            nextTuple = null;
            keyValueIter = stringAggregator.entrySet().iterator();
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
                Map.Entry<Field, Set<String>> nextKeyValuePair = keyValueIter.next();
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, nextKeyValuePair.getKey());
                tuple.setField(1, new IntField(nextKeyValuePair.getValue().size()));
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
            keyValueIter = stringAggregator.entrySet().iterator();
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
