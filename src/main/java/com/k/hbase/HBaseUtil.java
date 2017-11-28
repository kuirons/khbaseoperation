package com.k.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.attribute.standard.Compression;
import java.io.IOException;

public class HBaseUtil {
    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    private static Configuration conf;
    private static Connection conn;

    private static final String[] SPLIT_KEYS=new String[]{"1","2","3","4","5","6","7","8","9","A","B","C","D","E"};

    /**
     * 加载配置文件
     * 默认读取resources目录下的hbase-site.xml配置文件
     */
    static {
        try {
            if (conf == null) {
                conf = HBaseConfiguration.create();
            }
        }catch (Exception e){
            logger.error("加载Hbase配置文件失败:",e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取连接
     * @return HbaseConnection
     */
    public static synchronized Connection getConn(){
        try{
            if(conn == null || conn.isClosed()){
                conn = ConnectionFactory.createConnection();
            }
        }catch (IOException e){
            logger.error("获取Hbase连接失败:",e);
        }
        return conn;
    }

    /**
     * 创建表的执行类，作为其他建表功能类的底层接口
     * @param tableName 表名
     * @param columnFamilies 列簇
     * @param splitKeys 预划分region的keys数组
     * @throws IOException
     */
    private static void createTable(String tableName,String[] columnFamilies,byte[][] splitKeys) throws Exception{
        Connection connection = getConn();
        HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
        try{
            if(admin.tableExists(tableName)){
                logger.warn("表:{}已存在",tableName);
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cfs :
                    columnFamilies) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cfs);
                hColumnDescriptor.setCompressionType(Compression.Al)
            }
        }
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     * @param columnFamilies 列簇
     * @param preBuildRegion 是否预划分region
     */
    public static void createTable(String tableName,String[] columnFamilies,boolean preBuildRegion) throws Exception{
        if(preBuildRegion){
            byte[][] splitKeys = new byte[14][];
            for (int i = 1; i <15 ; i++) {
                splitKeys[i-1]= Bytes.toBytes(SPLIT_KEYS[i-1]);
            }
            createTable(tableName,);
        }
    }
}