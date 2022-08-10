package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
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
        //取文件的hash，当作一个heapFile的独有Id
        return file.getAbsoluteFile().hashCode();
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
        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();
        //随机IO
        RandomAccessFile f = null;
        try{
            //从文件中读取出来数据，放入bytes中，生成HeapPage
            f = new RandomAccessFile(file, "r");
            //判断是否超出文件范围
            if ((pgNo + 1) * BufferPool.getPageSize() > f.length()){
                f.close();
                throw new IllegalArgumentException(String.format("table:%d , page: %d not exist", tableId, pgNo));
            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            f.seek(pgNo * BufferPool.getPageSize());
            int read = f.read(bytes, 0, BufferPool.getPageSize());
            if (read != BufferPool.getPageSize()){
                throw new IllegalArgumentException(String.format("table:%d , page: %d not exist", tableId, pgNo));
            }
            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), bytes);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                f.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table:%d , page: %d not exist", tableId, pgNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        //获取页面id
        int pageId = page.getId().getPageNumber();
        if (pageId > numPages()){
            throw new IllegalArgumentException("超过最大页面数");
        }
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        //找到需要写的页面
        f.seek(pageId * BufferPool.getPageSize());
        //写入数据, 刷盘
        f.write(page.getPageData());
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // 页数 = 文件长度 / 每页字节数
        int res = (int) Math.floor(file.length() * 1.0 / BufferPool.getPageSize());
        return res;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        ArrayList<Page> list = new ArrayList<>();
        //从现有页面中查询空闲空间, pgNo是一张表中的page编号
        for (int pageNo = 0; pageNo < numPages(); pageNo++){
            HeapPageId pageId = new HeapPageId(getId(), pageNo);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId,Permissions.READ_WRITE);
            //判断是否有空闲空间
            if (page.getNumEmptySlots() != 0){
                page.insertTuple(t);
                list.add(page);
                return list;
            }
            //当page上没有空闲slow，释放锁
            else {
                Database.getBufferPool().unsafeReleasePage(tid, pageId);
            }
        }

        //当没有空闲页可以写入，需要创建新页面
        BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(file, true));
        //建空页
        byte[] emptyPage = HeapPage.createEmptyPageData();
        output.write(emptyPage);
        output.close();

        //创建page
        HeapPageId pageId = new HeapPageId(getId(), numPages() - 1);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.insertTuple(t);
        list.add(page);
        return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> list = new ArrayList<>();
        PageId pageId = t.getRecordId().getPageId();
        //找到要删除的page在bufferpool中的对应页
        HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.deleteTuple(t);
        list.add(page);
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    private static final class HeapFileIterator implements DbFileIterator{

        private final HeapFile heapFile;
        private final TransactionId tid;
        private Iterator<Tuple> iterator;
        private int whichPage;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid){
            this.heapFile = heapFile;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            whichPage = 0;
            iterator = getPageTuple(whichPage);
        }

        //获取一个页中对应的每一行迭代器
        private Iterator<Tuple> getPageTuple(int pageNumber) throws TransactionAbortedException, DbException{
            if (pageNumber >= 0 && pageNumber < heapFile.numPages()){
                //获取heapPageId: 表Id + 页Id定位
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNumber);
                //通过Pid从bufferPool中找出页
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }
            throw new DbException(String.format("heapFile %d not contain page %d", pageNumber, heapFile.getId()));
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator == null){
                return false;
            }
            if (!iterator.hasNext()){
                while (whichPage < heapFile.numPages() - 1){
                    whichPage++;
                    iterator = getPageTuple(whichPage);
                    if (iterator.hasNext()){
                        return true;
                    }
                }
                return false;
            }
            return true;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iterator == null || !iterator.hasNext()){
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            iterator = null;
        }
    }

}

