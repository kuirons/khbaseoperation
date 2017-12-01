package com.k.hbase;

import com.k.hbase.util.ChineseUtils;
import com.k.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.List;

public class test {
    private static HBaseServiceImpl hBaseService=new HBaseServiceImpl();
    private final static Logger logger = LoggerFactory.getLogger(test.class);
    public static void main(String[] args) {
        String tableName = "tabletest";
        try {
            HBaseUtil.createTable(tableName,new String[]{"cfs1","cfs2"},true);
        } catch (Exception e) {
            logger.error("创建表:{}失败",tableName,e);
        }
        List<Put> puts=new ArrayList<Put>();
        int flag=1;
        for (int i = 0; i < 100; i++) {
            String family = "cfs"+flag;
            String rowKey="row"+i;
            if(flag==1){
                flag=2;
            }
            else{
                flag=1;
            }
            Put put=new Put(rowKey.getBytes());
            put.addColumn(family.getBytes(),"content".getBytes(), ChineseUtils.nextString(3).getBytes());
            puts.add(put);
        }
        hBaseService.batchAsyncPut(tableName,puts,false);
//        try {
//            HBaseUtil.asynPut(tableName,puts);
//        } catch (Exception e) {
//            logger.error("异步添加失败");
//        }
//        HBaseUtil.closeConnection();
        logger.error("这是一个标记");
    }
}