package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    private TreeMap<Integer, TDItem> tdItemTreeMap;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public boolean equals(Object o) {
            if (o == null) {
                return false;
            }

            if (!(o instanceof TDItem)) {
                return false;
            }

            TDItem aItem = (TDItem)o;

            if (aItem.fieldType != this.fieldType) {
                return false;
            }

            if (aItem.fieldName == null && this.fieldName == null) {
                return true;
            } else if (aItem.fieldName == null || this.fieldName == null) {
                return false;
            } else {
                return aItem.fieldName.equals(this.fieldName);
            }
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        
        return tdItemTreeMap.values().iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with zero field type/name.
     */
    public TupleDesc() {
        tdItemTreeMap = new TreeMap<Integer, TDItem>();
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        
        tdItemTreeMap = new TreeMap<Integer, TDItem>();

        // Shouldn't have a schema with 0 type
        assert (typeAr.length > 0);
        // Should have the same number of column types and column names.
        assert (typeAr.length == fieldAr.length);

        for (int i = 0; i < typeAr.length; i++) {
            tdItemTreeMap.put(i, new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        
        // Shouldn't have a schema with 0 type
        tdItemTreeMap = new TreeMap<Integer, TDItem>();

        assert (typeAr.length > 0);

        for (int i = 0; i < typeAr.length; i++) {
            tdItemTreeMap.put(i, new TDItem(typeAr[i], null));
        }
    }

    /**
     * Add a filed to TupleDesc by passing in its filed type and filed name.
     * @param filedType
     *                  type of field.
     * @param filedName
     *                  name of field, could be null.
     */
    public void addField(Type filedType, String filedName) {
        if (tdItemTreeMap == null) {
            tdItemTreeMap = new TreeMap<Integer, TDItem>();
        }

        Integer key = tdItemTreeMap.lastKey() + 1;
        tdItemTreeMap.put(key, new TDItem(filedType, filedName));
    }

    /**
     * Add a filed to TupleDesc by passing in a TDItem.
     * @param item
     *            a TDItem
     */
    public void addField(TDItem item) {
        if (tdItemTreeMap == null) {
            tdItemTreeMap = new TreeMap<Integer, TDItem>();
        }

        if (tdItemTreeMap.size() == 0) {
            tdItemTreeMap.put(0, item);
        } else {
            Integer key = tdItemTreeMap.lastKey() + 1;
            tdItemTreeMap.put(key, item);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tdItemTreeMap.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        
        if (!tdItemTreeMap.containsKey(i)) {
            throw new NoSuchElementException(String.format("index %d out of bound", i));
        }
        return tdItemTreeMap.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        
        if (!tdItemTreeMap.containsKey(i)) {
            throw new NoSuchElementException(String.format("index %d out of bound", i));
        }
        return tdItemTreeMap.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        
        for (Map.Entry<Integer, TDItem> entry : tdItemTreeMap.entrySet()) {
            if (entry.getValue().fieldName != null &&
                    entry.getValue().fieldName.equals(name)) {
                return entry.getKey();
            }
        }
        throw new NoSuchElementException("No such field name in records");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        
        Iterator<TDItem> iterator = this.iterator();
        int ret = 0;
        while (iterator.hasNext()) {
            TDItem item = iterator.next();
            ret += item.fieldType.getLen();
        }
        return ret;
    }

    /**
     * @return TreeMap of TDItems.
     */
    public TreeMap<Integer, TDItem> getTdItemTreeMap() {
        return tdItemTreeMap;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        
        TupleDesc tupleDesc = new TupleDesc();
        Iterator<TDItem> iteratorOfTD1 = td1.iterator();
        while (iteratorOfTD1.hasNext()) {
            TDItem item = iteratorOfTD1.next();
            tupleDesc.addField(item);
        }

        Iterator<TDItem> iteratorOfTD2 = td2.iterator();
        while (iteratorOfTD2.hasNext()) {
            TDItem item = iteratorOfTD2.next();
            tupleDesc.addField(item);
        }
        return tupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // null doesn't equal to anything
        if (o == null) {
            return false;
        }

        if (o.getClass() != TupleDesc.class) {
            return false;
        }
        TreeMap map = ((TupleDesc) o).getTdItemTreeMap();

        return tdItemTreeMap.equals(map);
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        Iterator<TDItem> iterator = this.iterator();
        StringBuilder builder = new StringBuilder();
        while (iterator.hasNext()) {
            TDItem item = iterator.next();
            builder.append(item.fieldType);
            builder.append("(");
            builder.append(item.fieldName);
            builder.append("),");
        }
        return builder.toString();
    }
}
