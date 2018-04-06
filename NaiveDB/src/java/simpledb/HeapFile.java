package simpledb;

import com.sun.prism.impl.Disposer;
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
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        HeapPageId pageId = new HeapPageId(pid.getTableId(), pid.getPageNumber());
        Page ret = null;
        if (pageId.getPageNumber() == numPages()) {
            try {
                ret = new HeapPage(pageId, HeapPage.createEmptyPageData());
                writePage(ret);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            byte[] buffer = new byte[BufferPool.getPageSize()];
            try {
                RandomAccessFile randomAccessFile = new RandomAccessFile(file.getAbsolutePath(), "r");
                long offset = 1L * pid.getPageNumber() * BufferPool.getPageSize();
                randomAccessFile.seek(offset);
                randomAccessFile.read(buffer);
                ret = new HeapPage(pageId, buffer);
                randomAccessFile.close();
            } catch (FileNotFoundException e ) {
                LOG.error(e);
                e.printStackTrace();
            } catch (IOException e) {
                LOG.error(e);
                e.printStackTrace();
            }

        }
        return ret;
    }

    /**
     * Push the specified page to disk.
     *
     * @param page The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
     * @throws IOException if the write fails
     *
     */
    public void writePage(Page page) throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile(file.getAbsoluteFile(), "rw");
        randomAccessFile.seek(page.getId().getPageNumber() * Database.getBufferPool().getPageSize());
        randomAccessFile.write(page.getPageData());
        randomAccessFile.close();
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
        int numPage = numPages();
        for (int i = 0; i < numPage; i++) {
            PageId pageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                return new ArrayList<>(Arrays.asList(page));
            } else {
                Database.getBufferPool().releasePage(tid, pageId);
            }
        }
        HeapPageId newPageId = new HeapPageId(getId(), numPage);
        HeapPage newHeapPage = (HeapPage) Database.getBufferPool().getPage(tid, newPageId, Permissions.READ_WRITE);
        newHeapPage.insertTuple(t);
        return new ArrayList<>(Arrays.asList(newHeapPage));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        RecordId recordId = t.getRecordId();
        if (recordId.getPageId() == null) {
            return new ArrayList<>();
        }
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(Arrays.asList(page));
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
            pages = new ArrayList<>();
        }

        public void open() throws DbException, TransactionAbortedException {
            int numPages = heapFile.numPages();
            for (int i = 0; i < numPages; i++) {
                HeapPageId pageId = new HeapPageId(heapFile.getId(), i);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.transactionId, pageId, Permissions.READ_ONLY);
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

