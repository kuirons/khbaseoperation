package com.k.hbase;

import com.k.hbase.util.HBasePageModel;
import jodd.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class HBaseUtil {
    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    private static Configuration conf;
    private static Connection conn;

    private static final String[] SPLIT_KEYS = new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E"};

    /**
     * 加载配置文件
     * 默认读取resources目录下的hbase-site.xml配置文件
     */
    static {
        try {
            if (conf == null) {
                conf = HBaseConfiguration.create();
            }
        } catch (Exception e) {
            logger.error("加载Hbase配置文件失败:", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取连接
     *
     * @return HbaseConnection
     */
    public static synchronized Connection getConn() {
        try {
            if (conn == null || conn.isClosed()) {
                conn = ConnectionFactory.createConnection();
            }
        } catch (IOException e) {
            logger.error("获取Hbase连接失败:", e);
        }
        return conn;
    }

    /**
     * 关闭连接
     *
     * @param conn 需要关闭的连接
     * @throws IOException
     */
    public static void closeConnection(Connection conn) {
        if (null != conn) {
            try {
                conn.close();
            } catch (IOException e) {
                logger.error("关闭Hbase连接失败", e);
            }
        }
    }

    /**
     * 创建表的执行类，作为其他建表功能类的底层接口
     *
     * @param tableName      表名
     * @param columnFamilies 列簇
     * @param splitKeys      预划分region的keys数组
     * @throws IOException
     */
    private static void createTable(String tableName, String[] columnFamilies, byte[][] splitKeys) throws Exception {
        Connection connection = getConn();
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        try {
            if (admin.tableExists(tableName)) {
                logger.warn("表:{}已存在", tableName);
                return;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cfs :
                    columnFamilies) {
                tableAddFamilies(tableDescriptor, cfs);
            }
            admin.createTable(tableDescriptor, splitKeys);
            logger.info("Table:{}已建立", tableName);
        } finally {
            //不应该释放连接，而是由上层调用者决定什么时候释放连接，但是管理和表实例应该用完后就释放
            admin.close();
        }
    }

    /**
     * 创建表的执行类，作为其他建表功能类的底层接口，该方法使用默认一个region
     *
     * @param tableName      表名
     * @param columnFamilies 列簇
     * @throws IOException
     */
    private static void createTable(String tableName, String[] columnFamilies) throws Exception {
        Connection connection = getConn();
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        try {
            if (admin.tableExists(tableName)) {
                logger.warn("表:{}已存在", tableName);
                return;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cfs :
                    columnFamilies) {
                tableAddFamilies(tableDescriptor, cfs);
            }
            admin.createTable(tableDescriptor);
            logger.info("Table:{}已建立", tableName);
        } finally {
            admin.close();
        }
    }

    /**
     * 为表增加列簇
     *
     * @param tableDescriptor
     * @param cfs
     */
    private static void tableAddFamilies(HTableDescriptor tableDescriptor, String cfs) {
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cfs);
        //设置压缩方式
        hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
        //一个值只存一个版本，可以减少容量的压力，但是可能会对表设计有一定影响，比如无法查询当前值的历史版本
        hColumnDescriptor.setMaxVersions(1);
        tableDescriptor.addFamily(hColumnDescriptor);
    }

    /**
     * 创建表
     *
     * @param tableName      表名
     * @param columnFamilies 列簇
     * @param preBuildRegion 是否预划分region
     */
    public static void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception {
        if (preBuildRegion) {
            byte[][] splitKeys = new byte[14][];
            for (int i = 1; i < 15; i++) {
                splitKeys[i - 1] = Bytes.toBytes(SPLIT_KEYS[i - 1]);
            }
            createTable(tableName, columnFamilies, splitKeys);
        } else {
            createTable(tableName, columnFamilies);
        }
    }

    /**
     * 删除表
     *
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        Connection connection = getConn();
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
        try {
            if (!admin.tableExists(tableName)) {
                logger.error("表:{}不存在，删除失败", tableName);
                return;
            }
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            logger.info("表:{}删除成功", tableName);
        } finally {
            admin.close();
        }
    }

    /**
     * 获取表
     *
     * @param tableName
     * @return
     */
    public static Table getTable(String tableName) {
        try {
            Table table = getConn().getTable(TableName.valueOf(tableName));
            return table;
        } catch (Exception e) {
            logger.error("获取表:{}失败", tableName);
        }
        return null;
    }

    /**
     * 为表创建快照，快照可以用来恢复hbase至某一状态
     *
     * @param snapshotName
     * @param tableName
     */
    public static void snapshot(String snapshotName, TableName tableName) {
        try {
            Admin admin = getConn().getAdmin();
            try {
                admin.snapshot(snapshotName, tableName);
            } finally {
                admin.close();
            }
        } catch (IOException e) {
            logger.error("快照:{}创建失败", snapshotName, e);
        }
    }

    /**
     * 获取已有的快照
     *
     * @param snapshotNameRegex 过滤快照的正则表达式
     * @return
     */
    public static List<HBaseProtos.SnapshotDescription> listSnapshots(String snapshotNameRegex) {
        try {
            Admin admin = getConn().getAdmin();
            try {
                if (StringUtil.isNotBlank(snapshotNameRegex))
                    return admin.listSnapshots(snapshotNameRegex);
                else
                    return admin.listSnapshots();
            } finally {
                admin.close();
            }
        } catch (IOException e) {
            logger.error("获取快照:{}失败", snapshotNameRegex, e);
        }
        return null;
    }

    /**
     * 批量删除snapshot
     *
     * @param snapshotNameRegex
     */
    public static void deleteSnapshots(String snapshotNameRegex) {
        try {
            Admin admin = getConn().getAdmin();
            try {
                if (StringUtil.isNotBlank(snapshotNameRegex))
                    admin.deleteSnapshots(snapshotNameRegex);
                else
                    logger.error("snapshotNameRegex不能为空");
            } finally {
                admin.close();
            }
        } catch (IOException e) {
            logger.error("批量快照:{}删除失败", snapshotNameRegex, e);
        }
    }

    /**
     * 单个删除快照
     *
     * @param snapshotName
     */
    public static void deleteSnapshot(String snapshotName) {
        try {
            Admin admin = getConn().getAdmin();
            try {
                if (StringUtil.isNotBlank(snapshotName)) {
                    admin.deleteSnapshot(snapshotName);
                }
            } finally {
                admin.close();
            }
        } catch (IOException e) {
            logger.error("删除单个快照:{}失败", snapshotName, e);
        }
    }

    /**
     * 分页检索数据
     *
     * @param tableName   表名
     * @param startRowKey 起始行健（可以为空，如果为空，则从表中第一行开始检索）
     * @param endRowKey   结束行健（可以为空）
     * @param filterList  检索条件过滤集合，不包括分页过滤器，分页过滤器在函数中添加
     * @param maxVersion  最大版本数，如果为最大整数值，则检索所有版本，如果为最小整数值，则检索最小版本，否则只检索指定的版本数
     * @param pageModel   分页模型
     * @return
     */
    public static HBasePageModel scanResultByPageFilter(String tableName, byte[] startRowKey, byte[] endRowKey, FilterList filterList, int maxVersion, HBasePageModel pageModel) {
        if (pageModel == null) {
            //默认页大小为15
            pageModel = new HBasePageModel(15);
        }
        if (maxVersion <= 0) {
            //默认检索数据的最新版本
            maxVersion = Integer.MIN_VALUE;
        }
        pageModel.initStartTime();
        pageModel.initEndTime();
        if (StringUtil.isBlank(tableName)) {
            return pageModel;
        }
        Table table = null;

        try {
            table = getTable(tableName);
//            int tempPageSize = pageModel.getPageSize();
            boolean isEmptyStartRowKey = false;
            if (startRowKey == null) {
                //如果起始行健为空，则从表的第一行数据开始检索，
                Result firstResult = selectFirstResultRow(tableName, filterList);
                if (firstResult == null) {
                    return pageModel;
                }
                startRowKey = firstResult.getRow();
            }
            if (pageModel.getPageStarRowKey() == null) {
                isEmptyStartRowKey = true;
                pageModel.setPageStarRowKey(startRowKey);
            } else {
                if (pageModel.getPageEndRowKey() != null) {
                    pageModel.setPageStarRowKey(pageModel.getPageEndRowKey());
                }
                //从第二页开始，每次多去一条记录，因为第一条记录需要被删除
//                tempPageSize += 1;
            }
            Scan scan = new Scan();
            scan.setStartRow(pageModel.getPageStarRowKey());
            if (endRowKey != null) {
                scan.setStopRow(endRowKey);
            }
            PageFilter pageFilter = new PageFilter(pageModel.getPageSize() + 1);
            if (filterList != null) {
                filterList.addFilter(pageFilter);
                scan.setFilter(filterList);
            } else {
                scan.setFilter(pageFilter);
            }
            if (maxVersion == Integer.MAX_VALUE) {
                scan.setMaxVersions();
            } else if (maxVersion == Integer.MIN_VALUE) {

            } else {
                scan.setMaxVersions(maxVersion);
            }
            ResultScanner scanner = table.getScanner(scan);
            List<Result> resultList = new ArrayList<Result>();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int pageIndex = pageModel.getPageIndex() + 1;
        pageModel.setPageIndex(pageIndex);
        if (pageModel.getResultList().size() > 0) {
            //获取本次分页数据首行和末行的行健信息
            byte[] pageStartRowKey = pageModel.getResultList().get(0).getRow();
            byte[] pageEndRowKey = pageModel.getResultList().get(pageModel.getResultList().size() - 1).getRow();
            pageModel.setPageStarRowKey(pageStartRowKey);
            pageModel.setPageEndRowKey(pageEndRowKey);
        }
        int queryTotalCount = pageModel.getQueryTotalCount() + pageModel.getResultList().size();
        pageModel.setQueryTotalCount(queryTotalCount);
        pageModel.initEndTime();
        pageModel.printTimeInfo();
        return pageModel;

    }

    /**
     * 检索指定表的第一行记录（如果在创建此表时指定了非默认的命名空间，需要在给定表名的时候指定命名空间，格式为【namespace:tablename】）
     *
     * @param tableName
     * @param filterList
     * @return
     */
    public static Result selectFirstResultRow(String tableName, FilterList filterList) {
        if (StringUtil.isBlank(tableName)) {
            return null;
        }
        Table table = null;
        try {
            table = getTable(tableName);
            Scan scan = new Scan();
            if (filterList != null) {
                scan.setFilter(filterList);
            }
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            if (iterator.hasNext()) {
                Result rs = iterator.next();
                scanner.close();
                return rs;
            } else
                scanner.close();
//            这是原代码，如果出问题需要改回
//            int index = 0;
//            while(iterator.hasNext()){
//                Result rs = iterator.next();
//                if(index == 0){
//                    scanner.close();
//                    return rs;
//                }
//            }
        } catch (IOException e) {
            logger.error("获取表:{}第一行记录失败", tableName, e);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 异步往指定表中添加数据
     *
     * @param tableName
     * @param puts      需要添加的数据
     * @return 返回执行的时间
     * @throws Exception
     */
    public static long asynPut(String tableName, List<Put> puts) throws Exception {
        long currentTime = System.currentTimeMillis();
        Connection connection = getConn();
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator) throws RetriesExhaustedWithDetailsException {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    logger.error("异步添加数据:{}失败", e.getRow(i));
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName)).listener(listener);
        params.writeBufferSize(5 * 1024 * 1024);

        final BufferedMutator mutator = connection.getBufferedMutator(params);
        try {
            mutator.mutate(puts);
            mutator.flush();
        } finally {
            mutator.close();
        }
        return System.currentTimeMillis() - currentTime;
    }

    /**
     * 表异步添加数据
     *
     * @param tableName
     * @param put
     * @return
     * @throws Exception
     */
    public static long asynput(String tableName, Put put) throws Exception {
        return asynPut(tableName, Arrays.asList(put));
    }

    /**
     * 同步添加数据
     *
     * @param tableName
     * @param put
     * @return
     */
    public static long sycPut(String tableName, Put put) {
        long currentTime = System.currentTimeMillis();

        Table table = getTable(tableName);
        if (table != null) {
            try {
                table.put(put);
            } catch (IOException e) {
                logger.error("同步添加数据:{}失败", put.getRow(), e);
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
//        try {
//            table = connection.getTable(TableName.valueOf(tableName));
//            table.put(put);
//        } catch (IOException e) {
//            logger.error("同步添加数据:{}失败",put.getRow(),e);
//        }finally {
//            try {
//                table.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
        return System.currentTimeMillis() - currentTime;
    }

    /**
     * 删除单条数据
     *
     * @param tableName
     * @param row
     * @throws IOException
     */
    public static void delete(String tableName, String row) throws IOException {
        Table table = getTable(tableName);
        if (table != null) {
            try {
                Delete d = new Delete(row.getBytes());
                table.delete(d);
            } finally {
                table.close();
            }
        }
    }

    /**
     * 删除多条数据
     *
     * @param tableName
     * @param rows
     * @throws IOException
     */
    public static void delete(String tableName, String[] rows) throws IOException {
        Table table = getTable(tableName);
        if (table != null) {
            try {
                List<Delete> list = new ArrayList<Delete>();
                for (String row :
                        rows) {
                    Delete d = new Delete(row.getBytes());
                    list.add(d);
                }
                if (list.size() > 0) {
                    table.delete(list);
                }
            } finally {
                table.close();
            }
        }
    }

    /**
     * 后去单条数据
     * @param tableName
     * @param row
     * @return
     */
    public static Result getRow(String tableName, byte[] row) {
        Table table = getTable(tableName);
        Result rs = null;
        if(table!=null){
            try{
                Get get = new Get(row);
                rs = table.get(get);
            } catch (IOException e) {
                logger.error("获取数据失败",e);
            }finally {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return rs;
    }

    /**
     * 获取多行数据
     * @param tableName
     * @param rows
     * @param <T>
     * @return
     */
    public static <T> Result[] getRows(String tableName,List<T> rows){
        Table table = getTable(tableName);
        List<Get> gets = null;
        Result[] results = null;
        try{
            if(table!=null){
                gets = new ArrayList<Get>();
                for (T row :
                        rows) {
                    if (row != null){
                        gets.add(new Get(Bytes.toBytes(String.valueOf(row))));
                    }else{
                        throw new RuntimeException("表中没有数据");
                    }
                }
            }
            if(gets.size() > 0){
                results = table.get(gets);
            }
        }catch (IOException e){
            logger.error("获取数据失败",e);
        }finally {
            try {
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return results;
    }

    /**
     * 扫描整张表，返回一个结果迭代器，使用完一定要释放，不然资源会爆炸
     * @param tableName
     * @return
     */
    public static ResultScanner get(String tableName){
        Table table = getTable(tableName);
        ResultScanner results = null;
        if(table != null){
            try{
                Scan scan = new Scan();
                scan.setCaching(1000);
                results = table.getScanner(scan);
            } catch (IOException e) {
                logger.error("获取扫描器失败",e);
            }finally {
                try{
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return results;
    }

    /**
     * byte[] 类型的长整型数字转换为long类型
     * @param byteNum
     * @return
     */
    public static long bytesToLong(byte[] byteNum){
        long num = 0;
        for (int ix = 0; ix < 8; ++ix) {
            num <<= 8;
            num |= (byteNum[ix] & 0xff);
        }
        return num;
    }
}