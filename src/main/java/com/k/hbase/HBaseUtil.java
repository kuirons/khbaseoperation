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
import java.util.Iterator;
import java.util.List;
import java.util.ResourceBundle;

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

    public static HBasePageModel scanResultByPageFilter(String tableName, byte[] startRowKey, byte[] endRowKey, FilterList filterList, int maxVersion, HBasePageModel pageModel) {
        if (pageModel == null) {
            //默认页大小为15
            pageModel = new HBasePageModel(10);
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
            int tempPageSize = pageModel.getPageSize();
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
                tempPageSize += 1;
            }
            Scan scan = new Scan();
            scan.setStartRow(pageModel.getPageStarRowKey());
            if (endRowKey != null){
                scan.setStopRow(endRowKey);
            }
            PageFilter pageFilter = new PageFilter(pageModel.getPageSize()+1);
            if(filterList != null){
                filterList.addFilter(pageFilter);
                scan.setFilter(filterList);
            }else {
                scan.setFilter(pageFilter);
            }
            if(maxVersion == Integer.MAX_VALUE){
                scan.setMaxVersions();
            }else if(maxVersion == Integer.MIN_VALUE){

            }else {
                scan.setMaxVersions(maxVersion);
            }
            ResultScanner scanner = table.getScanner(scan);
            List<Result> resultList = new ArrayList<Result>();
        } catch (IOException e) {
            e.printStackTrace();
        }

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
}