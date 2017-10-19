package simpledb;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private static Logger LOG = Logger.getLogger(HeapFile.class);

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        HeapPageId pageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());

        long offset = -1;
        byte[] buffer = new byte[BufferPool.getPageSize()];
        Page ret = null;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file.getAbsolutePath(), "r");
            offset = 1L * pid.getPageNumber() * BufferPool.getPageSize();
            randomAccessFile.seek(offset);
            randomAccessFile.read(buffer);
            ret = new HeapPage(pageId, buffer);
        } catch (FileNotFoundException e ) {
            LOG.error(e);
            e.printStackTrace();
        } catch (IOException e) {
            LOG.error(e);
            e.printStackTrace();
        }

        return ret;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long fileSize = file.length();
        int pageSize = Database.getBufferPool().getPageSize();
        return (int) (fileSize / pageSize);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    public class HeapFileIterator implements DbFileIterator {

        private HeapFile heapFile;
        private TransactionId transactionId;
        private int curIterator;
        private ArrayList<Iterator<Tuple>> pages;

        public HeapFileIterator(HeapFile heapFile, TransactionId transactionId) {
            this.heapFile = heapFile;
            this.transactionId = transactionId;
            pages = new ArrayList<Iterator<Tuple>>();
        }

        public void open() throws DbException, TransactionAbortedException {
            int numPages = heapFile.numPages();
            for (int i = 0; i < numPages; i++) {
                HeapPageId pageId = new HeapPageId(heapFile.getId(), i);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.transactionId, pageId, Permissions.READ_WRITE);
                pages.add(page.iterator());
            }

            curIterator = 0;
            moveToNext();
         }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (curIterator < pages.size()) {
                return pages.get(curIterator).hasNext();
            } else {
                return false;
            }
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (hasNext() == false) {
                throw new NoSuchElementException("No next tuple");
            }

            Tuple tuple = pages.get(curIterator).next();
            moveToNext();
            return tuple;
        }

        private void moveToNext() {
            while (curIterator < pages.size()) {
                if (!pages.get(curIterator).hasNext()) {
                    curIterator++;
                } else {
                    break;
                }
            }
        }

        public void rewind() throws DbException, TransactionAbortedException {
            pages.clear();
            open();
        }

        public void close() {
            curIterator = pages.size();
        }
    }

}

