package simpledb;

import org.apache.log4j.Logger;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {
    private static Logger LOG = Logger.getLogger(HeapPage.class);

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    private boolean isDirty;
    private TransactionId transactionId;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            LOG.error("NoSuchElementException", e);
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();

        isDirty = false;
        transactionId = null;
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here
        return (int)Math.floor((BufferPool.getPageSize() * 8.0) / (this.td.getSize() * 8 + 1));

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
        
        // some code goes here
        // _tuples per page_ = floor((_page size_ * 8) / (_tuple size_ * 8 + 1))
        return (int)Math.ceil(getNumTuples() / 8.0);

    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            LOG.error(e);
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        RecordId recordId = t.getRecordId();
        if (recordId.getPageId() == null) {
            throw new DbException("Page id not found from Tuple " + t.toString());
        }

        if (!isSlotUsed(recordId.getTupleNumber())) {
            throw new DbException("Tuple slot has been cleared or has never been used!");
        }

        markSlotUsed(recordId.getTupleNumber(), false);
        tuples[recordId.getTupleNumber()] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        if (getNumEmptySlots() == 0) {
            throw new DbException("No empty slot in HeapPage " + toString());
        }

        for (int i = 0; i < numSlots; i++) {
            if (isSlotUsed(i)) {
                continue;
            } else {
                markSlotUsed(i, true);
                tuples[i] = t;
                break;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        isDirty = true;
        transactionId = tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        return transactionId;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int count = numSlots;
        for (int i = 0; i < numSlots; i++) {
            if (((header[i/8] >> (i % 8)) & 1) == 1)
                count--;
        }
        return count;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        if (i >= numSlots) {
            return false;
        }

        int index = i / 8;
        int off = i % 8;

        byte target = header[index];

        if ((target & (1L << off)) != 0) {
            return true;
        } else {
            return false;
        }
    }

    public Tuple getTuple(int i) {
        if (i >= this.numSlots) {
            throw new NoSuchElementException("Not tuple " + i);
        }

        return this.tuples[i];
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        if (i >= numSlots) {
            return;
        }

        boolean used = false;
        if (isSlotUsed(i)) {
            used = true;
        }

        if (used == value) {
            return;
        }

        int index = i / 8;
        int off = i % 8;

        if (value) {
            header[index] |= 0x1 << off;
        } else {
            header[index] &= ~(0x1 << off);
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        return new HeapPageIterator(this);
    }

    /**
     * This is not thread safe.
     */
    private static class HeapPageIterator implements Iterator<Tuple> {
        private HeapPage page;
        private int numSlots;
        private int index;

        public HeapPageIterator(HeapPage page) {
            this.page = page;
            this.numSlots = page.getNumTuples();
            index = 0;
            fetchNext();
        }

        private void fetchNext() {
            while (index < numSlots && !page.isSlotUsed(index)) {
                index++;
            }
        }
        public boolean hasNext() {
            if (page.isSlotUsed(index)) {
                return true;
            } else {
                return false;
            }
        }

        public Tuple next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Not more elements");
            }

            Tuple ret = page.getTuple(index);
            // increase index by 1
            index++;
            // find next avalible slot.
            fetchNext();
            return ret;
        }

        public void remove() {
            throw new UnsupportedOperationException("remove() is not supported in HeapPageIterator");
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("table id: ");
        sb.append(getId().getTableId());
        sb.append("\n");
        sb.append("page no: ");
        sb.append(getId().getPageNumber());
        return sb.toString();
    }
}

