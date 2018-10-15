package com.datatrees.datacenter.compare;

import avro.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.datatrees.datacenter.table.HBaseTableInfo;
import com.datatrees.datacenter.utility.HBaseHelper;
import com.tree.finance.bigdata.hive.streaming.mutation.GenericRowIdUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class BatchGetFromHBase {
    private static Logger LOG = LoggerFactory.getLogger(BatchGetFromHBase.class);
    private static final int PARALLEL_FACTOR = 25;

    /**
     * 根据rowkey批量查询数据
     *
     * @param idList       id列表
     * @param tableName    hbase表名
     * @param columnFamily 列族
     * @param column       列
     * @return Map
     */
    public static Map<String, Long> getBatchDataFromHBase(List<String> idList, String tableName, String columnFamily, String column) {
        Map<String, Long> resultMap = null;
        if (null != idList && idList.size() > 0) {
            Table table = HBaseHelper.getTable(tableName);
            boolean tableExists = false;
            try {
                tableExists = HBaseHelper.getHBaseConnection().getAdmin().tableExists(table.getName());
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (tableExists) {
                List<Get> gets = new ArrayList<>();
                for (String anIdList : idList) {
                    Get get = new Get(Bytes.toBytes(anIdList));
                    get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
                    gets.add(get);
                }
                try {
                    Result[] results = table.get(gets);
                    if (null != results && results.length > 0) {
                        resultMap = new HashMap<>();
                        for (Result result : results) {
                            if (result != null && !result.isEmpty()) {
                                String rowKey = Bytes.toString(result.getRow());
                                long time = Bytes.toLong(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column)));
                                resultMap.put(rowKey, time);
                            }
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return resultMap;
    }

    static class BatchSearchCallable implements Callable<Map<String, Long>> {
        private List<String> ids;
        private String tableName;
        private String columnFamily;
        private String column;

        BatchSearchCallable(List<String> keys, String tableName, String columnFamily, String column) {
            this.ids = keys;
            this.tableName = tableName;
            this.columnFamily = columnFamily;
            this.column = column;
        }

        @Override
        public Map<String, Long> call() {
            return getBatchDataFromHBase(ids, tableName, columnFamily, column);
        }
    }

    /**
     * 多线程批量查询
     *
     * @param idList       需要查询的rowKey列表
     * @param tableName    表名
     * @param columnFamily 列簇
     * @param column       列名
     * @return map
     */
    public static Map<String, Long> parrallelBatchSearch(List<String> idList, String tableName, String columnFamily, String column) {
        Map<String, Long> dataMap = null;
        int parallel = (Runtime.getRuntime().availableProcessors() + 1) * 2;
        List<List<String>> batchIdList;
        if (null != idList && idList.size() > 0) {
            dataMap = new HashMap<>();
            if (idList.size() < parallel * PARALLEL_FACTOR) {
                batchIdList = new ArrayList<>(1);
                batchIdList.add(idList);
            } else {
                batchIdList = new ArrayList<>(parallel);
                List<String> lst;
                for (int i = 0; i < parallel; i++) {
                    lst = new ArrayList<>();
                    batchIdList.add(lst);
                }
                for (int i = 0; i < idList.size(); i++) {
                    batchIdList.get(i % parallel).add(idList.get(i));
                }
            }
            List<Future<Map<String, Long>>> futures = new ArrayList<>(parallel);
            ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
            builder.setNameFormat("parallelBatchQuery");
            ThreadFactory factory = builder.build();
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(parallel, factory);
            for (List<String> keys : batchIdList) {
                Callable<Map<String, Long>> callable = new BatchSearchCallable(keys, tableName, columnFamily, column);
                FutureTask<Map<String, Long>> future = (FutureTask<Map<String, Long>>) executor.submit(callable);
                futures.add(future);
            }
            executor.shutdown();
            try {
                boolean stillRuning = !executor.awaitTermination(30000, TimeUnit.MILLISECONDS);
                if (stillRuning) {
                    try {
                        executor.shutdownNow();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (InterruptedException e) {
                try {
                    Thread.currentThread().interrupt();
                } catch (Exception e1) {
                    e1.printStackTrace();
                }

            }
            for (Future f : futures) {
                try {
                    if (f.get() != null) {
                        dataMap.putAll((Map<String, Long>) f.get());
                    }
                } catch (InterruptedException e) {
                    try {
                        Thread.currentThread().interrupt();
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
        LOG.info("the data number read from avro is:" + (idList == null ? 0 : idList.size()));
        LOG.info("the record number find from HBase is :" + (dataMap == null ? 0 : dataMap.size()));
        return dataMap;
    }

    private static List<String> reHashRowKey(List<String> idList) {
        List<String> hashedIdList = null;
        if (null != idList && idList.size() > 0) {
            hashedIdList = new ArrayList<>();
            for (int i = 0; i < idList.size(); i++) {
                String id = idList.get(i);
                String idHashed = GenericRowIdUtils.addIdWithHash(id);
                hashedIdList.add(idHashed);
            }
        }
        return hashedIdList;
    }
    public static List<String> assembleRowKey(Map<String, Long> recordMap) {
        if (null != recordMap && recordMap.size() > 0) {
            List<String> rowKeyList = new ArrayList<>(recordMap.keySet());
            rowKeyList = BatchGetFromHBase.reHashRowKey(rowKeyList);
            return rowKeyList;
        } else {
            return null;
        }
    }

    private Map<String, Long> compareWithHBase(List<String> createIdList) {
        if (null != createIdList && createIdList.size() > 0) {
            return BatchGetFromHBase.parrallelBatchSearch(createIdList, HBaseTableInfo.TABLE_NAME, HBaseTableInfo.COLUMNFAMILY, HBaseTableInfo.LAST_UPDATE_TIME);
        }
        return null;
    }

    public static Map<String, Long> compareWithHBase(String dataBase, String tableName, List<String> createIdList) {
        if (null != createIdList && createIdList.size() > 0) {
            LOG.info("search data from HBase...");
            if ("bankbill".equals(dataBase)) {
                return BatchGetFromHBase.parrallelBatchSearch(createIdList, "bill" + "." + tableName + "_id", HBaseTableInfo.COLUMNFAMILY, HBaseTableInfo.LAST_UPDATE_TIME);
            } else {
                return BatchGetFromHBase.parrallelBatchSearch(createIdList, dataBase + "." + tableName + "_id", HBaseTableInfo.COLUMNFAMILY, HBaseTableInfo.LAST_UPDATE_TIME);
            }
        }
        return null;
    }

}
