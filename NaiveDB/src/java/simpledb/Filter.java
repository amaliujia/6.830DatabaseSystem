package simpledb;

import javafx.scene.layout.TilePane;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate predicate;
    private OpIterator childIter;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        predicate = p;
        childIter = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return childIter.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        childIter.open();
        super.open();
    }

    public void close() {
        // some code goes here
        childIter.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        childIter.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        Tuple tuple = null;
        while (childIter.hasNext()) {
            Tuple aTuple = childIter.next();
            if (predicate.filter(aTuple)) {
                tuple = aTuple;
                break;
            }
        }
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
       return new OpIterator[]{childIter};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        childIter = children[0];
    }

}
