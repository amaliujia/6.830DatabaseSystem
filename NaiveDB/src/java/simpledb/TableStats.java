package simpledb;

import javafx.util.Pair;
import org.apache.commons.lang3.tuple.MutablePair;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private TreeMap<Integer, IntHistogram> intHistogramTreeMap;
    private TreeMap<Integer, StringHistogram> stringHistogramTreeMap;
    private int tableid;
    private int ioCostPerPage;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;

        intHistogramTreeMap = new TreeMap<>();
        stringHistogramTreeMap = new TreeMap<>();

        DbFile file = Database.getCatalog().getDatabaseFile(tableid);
        TupleDesc tupleDesc = file.getTupleDesc();
        int numFields = tupleDesc.numFields();
        DbFileIterator iterator = file.iterator(new TransactionId());
        ArrayList<MutablePair<Integer, Integer>> extrameValues = new ArrayList<>();
        for (int i = 0; i < numFields; i++) {
            extrameValues.add(new MutablePair<>(Integer.MAX_VALUE, Integer.MIN_VALUE));
        }


        // first scan to find all extreme values for Int Type.
        try {
            iterator.open();
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                for (int i = 0; i < numFields; i++) {
                    Field field = tuple.getField(i);
                    if (field.getType() == Type.INT_TYPE) {
                        IntField f = (IntField)field;
                        if (f.getValue() < extrameValues.get(i).left) {
                            extrameValues.get(i).left = f.getValue();
                        }

                        if (f.getValue() > extrameValues.get(i).right) {
                            extrameValues.get(i).right = f.getValue();
                        }
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }

        // create histogram for all
        for (int i = 0; i < numFields; i++) {
            if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                intHistogramTreeMap.put(i, new IntHistogram(NUM_HIST_BINS, extrameValues.get(i).left, extrameValues.get(i).right));
            } else {
                stringHistogramTreeMap.put(i, new StringHistogram(NUM_HIST_BINS));
            }
        }

        // second scan to add values to histogram
        try {
            iterator.rewind();
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                for (int i = 0; i < numFields; i++) {
                    Field field = tuple.getField(i);
                    if (field.getType() == Type.INT_TYPE) {
                        IntField f = (IntField) field;
                        intHistogramTreeMap.get(i).addValue(f.getValue());
                    } else {
                        StringField f = (StringField) field;
                        stringHistogramTreeMap.get(i).addValue(f.getValue());
                    }
                }
            }
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return ((HeapFile)Database.getCatalog().getDatabaseFile(tableid)).numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {

        return (int)(getNumberTupleInTable() * selectivityFactor);
    }

    private int getNumberTupleInTable() {
        if (intHistogramTreeMap.size() > 0) {
            IntHistogram intHistogram = intHistogramTreeMap.values().iterator().next();
            return intHistogram.getNumTuples();
        } else {
            return 0;
        }
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (intHistogramTreeMap.containsKey(field)) {
            return intHistogramTreeMap.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        } else if (stringHistogramTreeMap.containsKey(field)) {
            return stringHistogramTreeMap.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        } else {
            return 0.0;
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return 0;
    }

}
