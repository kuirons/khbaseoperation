package com.k.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

public abstract class AbstractHBaseService implements HBaseService{

    public void put(String tableName, Put put, boolean waiting) {}

    public void batchPut(final String tableName, final List<Put> puts, boolean waiting) {}

    public <T> Result[] getRows(String tablename, List<T> rows) {return null;}

    public Result getRow(String tablename, byte[] row) {return null;}
}
