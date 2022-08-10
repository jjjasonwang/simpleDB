package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    //每个直方图的缓存
    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
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
    //每个直方图分的桶数
    static final int NUM_HIST_BINS = 100;

    //读取每页的IO成本
    private int ioCostPerpage;
    //进行数据统计的表
    private DbFile dbFile;
    //表的属性行
    private TupleDesc tupleDesc;
    //表Id
    private int tableId;
    //一个表中页的数量
    private int pageNum;
    //一页中行的数量
    private int tupleNum;
    //一行中字段的数量
    private int fieldNum;
    //第i个整形字段 ： 第i个直方图
    private HashMap<Integer, IntHistogram> integerHashMap;
    //第i个字符型字段 ： 第i个直方图
    private HashMap<Integer, StringHistogram> stringHashMap;

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
        tupleNum = 0;
        this.tableId = tableid;
        this.ioCostPerpage = ioCostPerPage;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.pageNum = ((HeapFile) dbFile).numPages();
        integerHashMap = new HashMap<>();
        stringHashMap = new HashMap<>();
        this.tupleDesc = dbFile.getTupleDesc();
        //字段数
        this.fieldNum = tupleDesc.numFields();

        //获取行数
        Type[] types = getTypes(tupleDesc);
        int[] mins = new int[fieldNum];
        int[] maxs = new int[fieldNum];

        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, tableId);
        try {
            scan.open();
            for (int i = 0; i < fieldNum; i++){
                //跳过字符串
                if (types[i] == Type.STRING_TYPE){
                    continue;
                }
                int min = Integer.MAX_VALUE;
                int max = Integer.MIN_VALUE;
                while (scan.hasNext()){
                    if (i == 0){
                        tupleNum++;
                    }
                    Tuple tuple = scan.next();
                    IntField field = (IntField) tuple.getField(i);
                    int val = field.getValue();
                    max = Math.max(val, max);
                    min = Math.min(val, min);
                }
                scan.rewind();
                mins[i] = min;
                maxs[i] = max;
            }
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        } finally {
            scan.close();
        }

        //记录到map中生成直方图
        for (int i = 0; i < fieldNum; i++){
            if (types[i] == Type.INT_TYPE){
                integerHashMap.put(i, new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]));
            }else {
                stringHashMap.put(i, new StringHistogram(NUM_HIST_BINS));
            }
        }

        //填入直方图
        addValueToHist();
    }

    private Type[] getTypes(TupleDesc td){
        int numField = td.numFields();
        Type[] types = new Type[numField];
        for (int i = 0; i < numField; i++){
            Type t = td.getFieldType(i);
            types[i] = t;
        }
        return types;
    }

    private void addValueToHist(){
        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, tableId);
        try {
            scan.open();
            //依次填入每行的每个字段
            while (scan.hasNext()){
                Tuple tuple = scan.next();
                //一个字段对应一个直方图
                for (int i = 0; i < fieldNum; i++){
                    //得到每个字段的原子Field
                    Field field = tuple.getField(i);
                    //根据类型，将值填入表直方图
                    if (field.getType() == Type.INT_TYPE){
                        int intValue = ((IntField) field).getValue();
                        integerHashMap.get(i).addValue(intValue);
                    }else{
                        String strValue = ((StringField) field).getValue();
                        stringHashMap.get(i).addValue(strValue);
                    }
                }
            }
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        } finally {
            scan.close();
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
        return pageNum * ioCostPerpage;
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
        //cardinality:一行中的键种类数
        double cardinality = selectivityFactor * tupleNum;
        return (int) cardinality;
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
        if (tupleDesc.getFieldType(field) == Type.INT_TYPE){
            return integerHashMap.get(field).avgSelectivity();
        }else {
            return stringHashMap.get(field).avgSelectivity();
        }
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
        if(tupleDesc.getFieldType(field) == Type.INT_TYPE){
            return integerHashMap.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        }
        else{
            return stringHashMap.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return tupleNum;
    }

}
