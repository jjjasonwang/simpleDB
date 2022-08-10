package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;


    //存储事务id和锁的类型
    class PageLock{
        private static final int SHARE = 0;
        private static final int EXCLUSIVE = 1;
        private TransactionId tid;
        private int type;

        public PageLock(TransactionId tid, int type){
            this.tid = tid;
            this.type = type;
        }

        public TransactionId getTid(){
            return tid;
        }

        public int getType(){
            return type;
        }

        public void setType(int type){
            this.type = type;
        }

    }

    class LockManager{
        //[页 ： [事务id ： 锁]]
        ConcurrentHashMap<PageId, ConcurrentHashMap<TransactionId, PageLock>> lockMap = new ConcurrentHashMap<>();

        /**
         * 获取锁
         */
        public synchronized boolean acquiredLock(PageId pageId, TransactionId tid, int requiredType){
            //当前页面没有锁
            if (lockMap.get(pageId) == null){
                PageLock pageLock = new PageLock(tid, requiredType);
                ConcurrentHashMap<TransactionId, PageLock> pageLocks = new ConcurrentHashMap<>();
                pageLocks.put(tid, pageLock);
                lockMap.put(pageId, pageLocks);
                return true;
            }
            //当前页面上被加锁
            //1.先获取等待队列
            ConcurrentHashMap<TransactionId, PageLock> pageLocks = lockMap.get(pageId);
            //2.判断当前事务对该页有没有锁
            //当该事务对该页没有锁
            if (pageLocks.get(tid) == null){
                //大于1说明该事务被上了共享锁
                if (pageLocks.size() > 1){
                    //该事务要上共享锁，可以上
                    if (requiredType == PageLock.SHARE){
                        PageLock pageLock = new PageLock(tid, PageLock.SHARE);
                        pageLocks.put(tid, pageLock);
                        lockMap.put(pageId, pageLocks);
                        return true;
                    //该事务要上独占锁，拒绝
                    }else if(requiredType == PageLock.EXCLUSIVE){
                        return false;
                    }
                }
                //等于1说明该事务被上了共享锁或独占锁
                if (pageLocks.size() == 1){
                    PageLock curLock = null;
                    for (PageLock lock : pageLocks.values()){
                        curLock = lock;
                    }
                    //当判断出该事务被加的是读锁
                    if (curLock.getType() == PageLock.SHARE){
                        //要加读锁可以加
                        if (requiredType == PageLock.SHARE){
                            PageLock pageLock = new PageLock(tid, PageLock.SHARE);
                            pageLocks.put(tid, pageLock);
                            lockMap.put(pageId, pageLocks);
                            return true;
                        //加写锁则拒绝
                        }else if(requiredType == PageLock.EXCLUSIVE){
                            return false;
                        }
                        //当判断出该事务被加的是写锁
                    }else if (curLock.getType() == PageLock.EXCLUSIVE){
                        return false;
                    }
                }
            //当该事务对该页有锁
            }else if (pageLocks.get(tid) != null){
                PageLock pageLock = pageLocks.get(tid);
                if (pageLock.getType() == PageLock.SHARE){
                    if (requiredType == PageLock.SHARE){
                        return true;
                    }
                }else if (requiredType == PageLock.EXCLUSIVE){
                    if (pageLocks.size() == 1){
                        pageLock.setType(PageLock.EXCLUSIVE);
                        pageLocks.put(tid, pageLock);
                        return true;
                    }else if (pageLocks.size() > 1){
                        return false;
                    }
                }
                return pageLock.getType() == PageLock.EXCLUSIVE;
            }
            return false;
        }

        public synchronized boolean releaseLock(TransactionId tid, PageId pageId){
            if (isHoldLock(tid, pageId)){
                ConcurrentHashMap<TransactionId, PageLock> locks = lockMap.get(pageId);
                locks.remove(tid);
                if (locks.size() == 0){
                    lockMap.remove(pageId);
                }
                return true;
            }
            return false;
        }

        public synchronized boolean isHoldLock(TransactionId tid, PageId pageId){
            ConcurrentHashMap<TransactionId, PageLock> locks = lockMap.get(pageId);
            if (locks == null){
                return false;
            }
            PageLock pageLock = locks.get(tid);
            if (pageLock == null){
                return false;
            }
            return true;
        }

        //当事务完成，释放所有锁
        public synchronized void completeTranslation(TransactionId tid){
            for (PageId pageId : lockMap.keySet()){
                releaseLock(tid, pageId);
            }
        }

    }


    private final int numPages;
    //PageId : Node(Page)
    private final ConcurrentHashMap<PageId, LinkedNode> pageStore;

    private LockManager lockManager;

    //记录页面访问顺序
    private static class LinkedNode{
        PageId pageId;
        Page page;
        LinkedNode prev;
        LinkedNode next;
        public LinkedNode(PageId pageId, Page page){
            this.pageId = pageId;
            this.page = page;
        }
    }
    LinkedNode head;
    LinkedNode tail;
    private void addToHead(LinkedNode node){
        node.prev = head;
        node.next = head.next;
        head.next.prev = node;
        head.next = node;
    }

    private void remove(LinkedNode node){
        node.prev.next = node.next;
        node.next.prev = node.prev;
    }

    private void moveToHead(LinkedNode node){
        remove(node);
        addToHead(node);
    }

    private LinkedNode removeTail(){
        LinkedNode node = tail.prev;
        remove(node);
        return node;
    }


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageStore = new ConcurrentHashMap<>();
        head = new LinkedNode(new HeapPageId(-1, -1), null);
        tail = new LinkedNode(new HeapPageId(-1, -1), null);
        head.next = tail;
        tail.prev = head;
        lockManager = new LockManager();

    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {

        int lockType = perm == Permissions.READ_ONLY ? PageLock.SHARE : PageLock.EXCLUSIVE;
        long startTime = System.currentTimeMillis();
        boolean isAcquired = false;

        //循环获取锁
        while (!isAcquired){
            isAcquired = lockManager.acquiredLock(pid, tid, lockType);
            long now = System.currentTimeMillis();
            //死锁检测
            if (now - startTime > 500){
                throw new TransactionAbortedException();
            }
        }


        if (!pageStore.containsKey(pid)){
            //多个page对应一个DbFile，一个DbFile对应一个tableId
            int tableId = pid.getTableId();
            DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
            Page page = dbFile.readPage(pid);
            //当页面数量超过上限
            if (pageStore.size() >= numPages){
                eviction();
            }
            LinkedNode node = new LinkedNode(pid, page);
            pageStore.put(pid, node);
            addToHead(node);
        }
        moveToHead(pageStore.get(pid));
        return pageStore.get(pid).page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if (commit){
            try {
                flushPages(tid);
            }catch (IOException e){
                e.printStackTrace();
            }
        }else {
            //当事务提交失败，回滚
            restorePages(tid);
        }
        lockManager.completeTranslation(tid);
    }

    //回滚页面
    public synchronized void restorePages(TransactionId tid){

        for (LinkedNode node : pageStore.values()){
            PageId pageId = node.pageId;
            Page page = node.page;
            //如果脏页上的事务是本事务
            if (tid.equals(page.isDirty())){
                int tableId = pageId.getTableId();
                //通过表读出当前页
                DbFile table = Database.getCatalog().getDatabaseFile(tableId);
                Page pageFromDisk = table.readPage(pageId);

                //回滚回内存
                node.page = pageFromDisk;
                pageStore.put(pageId, node);
                moveToHead(node);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        //获取数据库文件
        DbFile dbFile =  Database.getCatalog().getDatabaseFile(tableId);
        //页面刷入缓存
        updateBufferPool(dbFile.insertTuple(tid, t), tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        updateBufferPool(dbFile.deleteTuple(tid, t), tid);
    }

    private void updateBufferPool(List<Page> pageList, TransactionId tid) throws DbException {
        for (Page page : pageList){
            //标记为脏页
            page.markDirty(true, tid);
            //如果缓存池满了，执行淘汰策略
            if (pageStore.size() > numPages){
                evictPage();
            }
            LinkedNode node;
            if (pageStore.containsKey(page.getId())){
                node = pageStore.get(page.getId());
                node.page = page;
            }else{
                if (pageStore.size() >= numPages){
                    evictPage();
                }
                node = new LinkedNode(page.getId(), page);
                addToHead(node);
            }

            //更新缓存
            pageStore.put(page.getId(),node);
        }
    }

    private void eviction(){
        LinkedNode node = removeTail();
        pageStore.remove(node.pageId);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pageId : pageStore.keySet()){
            flushPage(pageId);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        remove(pageStore.get(pid));
        pageStore.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page page = pageStore.get(pid).page;
        //刷脏页入磁盘
        if (page.isDirty() != null){
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            //移除脏页标签
            page.markDirty(false,null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    //事务提交成功之后刷新
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        for (LinkedNode node : pageStore.values()){
            PageId pageId = node.pageId;
            Page page = node.page;
            if (tid.equals(page.isDirty())){
                flushPage(pageId);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    //若当前页是脏页，说明事务对当前页的修改还未提交，bushuax
    private synchronized  void evictPage() throws DbException {
        //遍历所有页面
        for (int i = 0; i < numPages; i++){
            //淘汰尾部节点
            LinkedNode node = removeTail();
            Page evictPage = node.page;
            //如果此页是脏页，继续留在BufferPool中
            if (evictPage.isDirty() != null){
                addToHead(node);
            }else{
                try {
                    flushPage(node.pageId);
                }catch (IOException e){
                    e.printStackTrace();
                }
                pageStore.remove(node.pageId);
                return;
            }
        }
        //过程中未中断，说明所有页面都是脏页
        throw new DbException("All Page Are Dirty Page");
    }

}
