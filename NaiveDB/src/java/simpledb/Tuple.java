package simpledb;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeMap;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {
    private static Logger LOG = Logger.getLogger(Tuple.class);
    private static final long serialVersionUID = 1L;

    private TupleDesc tupleDesc;
    private TreeMap<Integer, Field> fieldTreeMap;
    private RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        tupleDesc = td;
        fieldTreeMap = new TreeMap<Integer, Field>();
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        fieldTreeMap.put(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return fieldTreeMap.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        return fieldTreeMap.values().iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        this.tupleDesc = td;
    }


    public static Tuple merge(Tuple t1, Tuple t2) {
        TupleDesc mergedTupleDesc = TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc());
        Tuple ret = new Tuple(mergedTupleDesc);
        Iterator<Field> fieldsOfT1 = t1.fields();

        int i = 0;
        while (fieldsOfT1.hasNext()) {
            Field field = fieldsOfT1.next();
            ret.setField(i, field);
            i++;
        }

        Iterator<Field> fieldsOfT2 = t2.fields();

        while (fieldsOfT2.hasNext()) {
            Field field = fieldsOfT2.next();
            ret.setField(i, field);
            i++;
        }

        return ret;
    }
}
