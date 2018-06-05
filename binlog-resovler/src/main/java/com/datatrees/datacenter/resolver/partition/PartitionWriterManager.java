package com.datatrees.datacenter.resolver.partition;

import com.datatrees.datacenter.core.storage.FileStorage;
import com.datatrees.datacenter.core.utility.PropertiesUtility;
//import com.datatrees.datacenter.resolver.domain.BufferRecord;
import com.datatrees.datacenter.resolver.domain.Operator;
import com.google.common.collect.ImmutableSet;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class PartitionWriterManager {
    private FileStorage fileStorage;
    private HashMap<String, PartitionWriter> partitionWriterHashMap = new HashMap<>();
    private static String ROOT_PATH = "hdfs://dn0:8020/data/warehouse";
    private static String TMP_ROOT_PATH = "hdfs://dn0:8020/data/temp";

    private static Logger logger = LoggerFactory.getLogger(PartitionWriterManager.class);
    private ImmutableSet<Partitioner> partitioners;

    static {
        java.util.Properties value = PropertiesUtility.load("common.properties");
        TMP_ROOT_PATH = value.getProperty("hdfs.temp.url");
    }

    public void close() {
        partitionWriterHashMap.forEach((path, partitionWriter) -> {
            try {
                partitionWriter.close();
                String tempPath = path;
                String targetPath =
                        tempPath.replace("temp", "warehouse").
                                replace(tempPath.split("/")[6] + "/", "");
                fileStorage.commit(tempPath, targetPath);
            } catch (Exception e) {

                logger.error(e.getMessage());
            }
        });
    }

    private String createFullPath(String relativeFilePath, Partitioner partitioner, String identity, String[] fullSchema, Schema envelopSchema) {
        String fullPath = null;
        String database = fullSchema[1];
        String instance = fullSchema[0];
        if (fullSchema[1].contains("test") || fullSchema[1].contains("ecommerce") || fullSchema[1].contains("operator")) {
            database = database.replaceAll("\\d+", "");
        }
        if (relativeFilePath != null) {
            fullPath = String.
                    format("%s/%s/%s/%s/%s/%s/%s/%s.avro", TMP_ROOT_PATH, partitioner.getRoot(), identity, instance, database, envelopSchema.getName(), relativeFilePath, identity);
        } else {//没有分区
            fullPath = String.
                    format("%s/%s/%s/%s/%s/%s/%s.avro", TMP_ROOT_PATH, partitioner.getRoot(), identity, instance, database, envelopSchema.getName(), identity);

        }
        return fullPath;
    }

    public PartitionWriterManager(FileStorage fileStorage) {
        this.fileStorage = fileStorage;
        this.partitioners = ImmutableSet.<Partitioner>builder().add(new CreateDatePartitioner()).add(new UpdateDatePartitioner()).build();
        ScheduledFuture<?> schedule = Executors.newScheduledThreadPool(100, new ThreadFactory() {
            Thread thread;

            @Override
            public Thread newThread(Runnable r) {
                thread = new Thread(r);
                thread.setDaemon(true);
                return thread;
            }
        }).schedule(() -> partitionWriterHashMap.forEach((schemaStringSimpleEntry, partitionWriter) -> {
            try {
                partitionWriter.flush();
            } catch (IOException e) {

                logger.error(e.getMessage());
            }
        }), 5, TimeUnit.MINUTES);
    }

    public void write(Schema schema, String identity, Operator operator, Serializable before, Serializable after, Object result) throws IOException {
        GenericData.Record record = null;
        switch (operator) {
            case U:
            case C:
                record = (GenericData.Record) ((GenericData.Record) result).get("After");
                break;
            case D:
                record = (GenericData.Record) ((GenericData.Record) result).get("Before");
                break;
            default:
                break;
        }

        Schema envelopSchema = schema;
        String[] fullSchema = envelopSchema.getNamespace().split("\\.");
        PartitionWriter writer = null;
        if (record != null) {
            for (Partitioner partitioner : partitioners) {
                String relativeFilePath = partitioner.encodePartition(record);
                String fullPath = createFullPath(relativeFilePath, partitioner, identity, fullSchema, envelopSchema);
                if (partitionWriterHashMap.containsKey(fullPath)) {
                    writer = partitionWriterHashMap.get(fullPath);
                } else {
                    writer = new InternalPartitionWriter(envelopSchema, fileStorage.openWriter(fullPath), partitioner);
                    partitionWriterHashMap.put(fullPath, writer);
                }
                writer.write(result);
            }
        }
    }

    class InternalPartitionWriter implements PartitionWriter {
        private DataFileWriter<Object> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>());
        private Partitioner partitioner;

        public InternalPartitionWriter(Schema schema, OutputStream stream, Partitioner partitioner) throws IOException {
            this.dataFileWriter.create(schema, stream);
            this.partitioner = partitioner;
        }

        public void write(Object value) throws IOException {
            dataFileWriter.append(value);
        }

        @Override
        public void flush() throws IOException {
            dataFileWriter.flush();
        }

        @Override
        public void close() throws IOException {
            dataFileWriter.close();
        }

        @Override
        public Partitioner partitioner() {
            return partitioner;
        }
    }
}