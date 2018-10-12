package com.datatrees.datacenter.resolver;

import static com.datatrees.datacenter.resolver.DBbiz.updateRepairCorruptFilesLog;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Repair {

  private static Logger LOG = LoggerFactory.getLogger(Repair.class);
  //  private static String HDFS_ROOT = "hdfs://localhost:9000";
  private static String HDFS_ROOT = "hdfs://nameservice1:8020";
  //  private static String HDFS_TEMP_ROOT = "hdfs://localhost:9000/repair_temp";
  private static String HDFS_TEMP_ROOT = "hdfs://nameservice1:8020/repair_temp";


  public static void main(String[] args) throws IOException {

    if (args.length == 0) {
      return;
    }

    ExecutorService executorService = Executors
      .newFixedThreadPool(Runtime.getRuntime().availableProcessors());
//    RepairTaskPipeline pipeline = new RepairTaskPipeline(
//      "/create/",
    RepairTaskPipeline pipeline = new RepairTaskPipeline(args[0],
      repairTaskPipeline -> {
        Set<Entry<TaskRef, RepairRunner>> taskSets = repairTaskPipeline.getTaskRef().entrySet();
        taskSets.forEach(v -> {
          RepairRunner runner = v.getValue();
          TaskRef ref = v.getKey();
          try {
            executorService.submit(() -> {
              LOG.info("repairing avro file " + ref.getPath());
              runner.runRepair(ref);
              LOG.info("repaired avro file " + ref.getPath());
            });
          } catch (Exception e) {
            e.printStackTrace();
          }
        });

        try {
          executorService.shutdown();
          executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        return null;
      }
    );
  }


  protected interface RepairRunner {

    /**
     * 修复任务
     */
    void runRepair(TaskRef ref);
  }

  static class Runner implements RepairRunner {

    RecoverTool tool;

    public Runner(RecoverTool tool) {
      this.tool = tool;
    }

    @Override
    public void runRepair(TaskRef ref) {
      try {
        Path sourcePath = new Path(ref.getPath());
        Path destPath = new Path(ref.getRepairPath());
        updateRepairCorruptFilesLog(true, sourcePath.getName(), ref.getDir(),
          sourcePath.toUri().toString(),
          destPath.toUri().toString(), 0, null, null, null, null);
        RepairRecordRef repairRecordRef = this.tool
          .recover(sourcePath.toUri().toString(), destPath.toUri().toString(), true, true);

        updateRepairCorruptFilesLog(false, sourcePath.getName(), ref.getDir(),
          sourcePath.toUri().toString(),
          destPath.toUri().toString(),
          repairRecordRef.getIfCorrupt(),
          repairRecordRef.getNumBlocks(), repairRecordRef.getNumCorruptBlocks(),
          repairRecordRef.getNumRecords(), repairRecordRef.getNumCorruptRecords());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  static class RepairRecordRef implements Serializable {

    int numBlocks;
    int numCorruptBlocks;
    int numRecords;
    int numCorruptRecords;

    public RepairRecordRef(int numBlocks, int numCorruptBlocks, int numRecords,
      int numCorruptRecords) {
      this.numBlocks = numBlocks;
      this.numCorruptBlocks = numCorruptBlocks;
      this.numRecords = numRecords;
      this.numCorruptRecords = numCorruptRecords;
    }

    public int getNumBlocks() {
      return numBlocks;
    }

    public void setNumBlocks(int numBlocks) {
      this.numBlocks = numBlocks;
    }

    public int getNumCorruptBlocks() {
      return numCorruptBlocks;
    }

    public void setNumCorruptBlocks(int numCorruptBlocks) {
      this.numCorruptBlocks = numCorruptBlocks;
    }

    public int getNumRecords() {
      return numRecords;
    }

    public void setNumRecords(int numRecords) {
      this.numRecords = numRecords;
    }

    public int getNumCorruptRecords() {
      return numCorruptRecords;
    }

    public void setNumCorruptRecords(int numCorruptRecords) {
      this.numCorruptRecords = numCorruptRecords;
    }

    /**
     * 0 表示没有损坏 1 表示已经损坏
     */
    public int getIfCorrupt() {
      return getNumCorruptBlocks() == 0 && getNumCorruptRecords() == 0 ? 0 : 1;
    }
  }

  static class RecoverTool {

    public RepairRecordRef recover(String input, String output, boolean recoverPrior,
      boolean recoverAfter)
      throws IOException {
      File infile = new File(input);

      LOG.info("Recovering file: " + input);
      GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
      DataFileReader<Object> fileReader = new DataFileReader<Object>(infile,
        reader);
      try {
        Schema schema = fileReader.getSchema();
        String codecStr = fileReader.getMetaString(DataFileConstants.CODEC);
        CodecFactory codecFactory = CodecFactory.fromString("" + codecStr);
        List<String> metas = fileReader.getMetaKeys();
        if (recoverPrior || recoverAfter) {
          GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>();
          DataFileWriter<Object> fileWriter = new DataFileWriter<Object>(writer);
          try {
            File outfile = new File(output);
            for (String key : metas) {
              if (!key.startsWith("avro.")) {
                byte[] val = fileReader.getMeta(key);
                fileWriter.setMeta(key, val);
              }
            }
            fileWriter.setCodec(codecFactory);
            RepairRecordRef result = innerRecover(fileReader, fileWriter, recoverPrior,
              recoverAfter, schema, outfile);
            return result;
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return null;
          }
        } else {
          return innerRecover(fileReader, null, recoverPrior,
            recoverAfter, null, null);
        }

      } finally {
        fileReader.close();
      }
    }

    protected RepairRecordRef innerRecover(DataFileReader<Object> fileReader,
      DataFileWriter<Object> fileWriter,
      boolean recoverPrior, boolean recoverAfter, Schema schema, File outfile) {
      int numBlocks = 0;
      int numCorruptBlocks = 0;
      int numRecords = 0;
      int numCorruptRecords = 0;
      int recordsWritten = 0;
      long position = fileReader.previousSync();
      long blockSize = 0;
      long blockCount = 0;
      boolean fileWritten = false;
      RepairRecordRef repairRecordRef = new RepairRecordRef(numBlocks, numCorruptBlocks, numRecords,
        numCorruptRecords);
      try {
        while (true) {
          try {
            if (!fileReader.hasNext()) {
              LOG.info("File Summary: ");
              LOG.info("  Number of blocks: " + numBlocks + " Number of corrupt blocks: "
                + numCorruptBlocks);
              LOG.info("  Number of records: " + numRecords + " Number of corrupt records: "
                + numCorruptRecords);
              if (recoverAfter || recoverPrior) {
                LOG.info("  Number of records written " + recordsWritten);
              }
              repairRecordRef = new RepairRecordRef(numBlocks, numCorruptBlocks, numRecords,
                numCorruptRecords);
              return repairRecordRef;
            }
            position = fileReader.previousSync();
            blockCount = fileReader.getBlockCount();
            blockSize = fileReader.getBlockSize();
            numRecords += blockCount;
            long blockRemaining = blockCount;
            numBlocks++;
            boolean lastRecordWasBad = false;
            long badRecordsInBlock = 0;
            while (blockRemaining > 0) {
              try {
                Object datum = fileReader.next();
                if ((recoverPrior && numCorruptBlocks == 0) || (recoverAfter
                  && numCorruptBlocks > 0)) {
                  if (!fileWritten) {
                    try {
                      fileWriter.create(schema, outfile);
                      fileWritten = true;
                    } catch (Exception e) {
                      LOG.error(e.getMessage(), e);
                      return null;
                    }
                  }
                  try {
                    fileWriter.append(datum);
                    recordsWritten++;
                  } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                    throw e;
                  }
                }
                blockRemaining--;
                lastRecordWasBad = false;
              } catch (Exception e) {
                long pos = blockCount - blockRemaining;
                if (badRecordsInBlock == 0) {
                  // first corrupt record
                  numCorruptBlocks++;
                  LOG.info("Corrupt block: " + numBlocks + " Records in block: " + blockCount
                    + " uncompressed block size: " + blockSize);
                  LOG.info("Corrupt record at position: " + (pos));
                } else {
                  // second bad record in block, if consecutive skip block.
                  LOG.info("Corrupt record at position: " + (pos));
                  if (lastRecordWasBad) {
                    // consecutive bad record
                    LOG.info("Second consecutive bad record in block: " + numBlocks
                      + ". Skipping remainder of block. ");
                    numCorruptRecords += blockRemaining;
                    badRecordsInBlock += blockRemaining;
                    try {
                      fileReader.sync(position);
                    } catch (Exception e2) {
                      LOG.error("failed to sync to sync marker, aborting", e2);
                      return null;
                    }
                    break;
                  }
                }
                blockRemaining--;
                lastRecordWasBad = true;
                numCorruptRecords++;
                badRecordsInBlock++;
              }
            }
            if (badRecordsInBlock != 0) {
              LOG.info("** Number of unrecoverable records in block: " + (badRecordsInBlock));
            }
            position = fileReader.previousSync();
          } catch (Exception e) {
            LOG.error("Failed to read block " + numBlocks + ". Unknown record "
              + "count in block.  Skipping. Reason: " + e.getMessage(), e);
            numCorruptBlocks++;
            try {
              fileReader.sync(position);
            } catch (Exception e2) {
              LOG.error("failed to sync to sync marker, aborting", e);
              return null;
            }
          }
        }
      } finally {
        if (fileWritten) {
          try {
            fileWriter.close();
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return null;
          }
        }
        return repairRecordRef;
      }
    }
  }

  static class HdfsRecoverTool extends RecoverTool {

    String rootPath;

    public HdfsRecoverTool(String rootPath) {
      this.rootPath = rootPath;
    }

    @Override
    public RepairRecordRef recover(String input, String output, boolean recoverPrior,
      boolean recoverAfter) throws IOException {

      Configuration config = new Configuration(); // make this your Hadoop env config
      SeekableInput seekableInput = new FsInput(new Path(input), config);
      DatumReader<Object> reader = new GenericDatumReader<Object>();
      DataFileReader<Object> fileReader = new DataFileReader(seekableInput, reader);

      FileSystem innerFileSystem = FileSystem
        .get((new Path(HDFS_ROOT + this.rootPath)).toUri(), config);
      LOG.info("Recovering file: " + input);
      try {
        Schema schema = fileReader.getSchema();
        String codecStr = fileReader.getMetaString(DataFileConstants.CODEC);
        CodecFactory codecFactory = CodecFactory.fromString("" + codecStr);
        List<String> metas = fileReader.getMetaKeys();
        if (recoverPrior || recoverAfter) {
          GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>();
          DataFileWriter<Object> fileWriter = new DataFileWriter<Object>(writer);
          try {
//            OutputStream outfile = this.fileSystem.create(new Path(output), true);
            OutputStream outfile = innerFileSystem.create(new Path(output), true);
            for (String key : metas) {
              if (!key.startsWith("avro.")) {
                byte[] val = fileReader.getMeta(key);
                fileWriter.setMeta(key, val);
              }
            }
            fileWriter.setCodec(codecFactory);
            RepairRecordRef result = innerRecover2(fileReader, fileWriter, input, recoverPrior,
              recoverAfter, schema, outfile);
            return result;
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return null;
          }
        } else {
          return innerRecover2(fileReader, null, input, recoverPrior,
            recoverAfter, null, null);
        }

      } finally {
        fileReader.close();
      }
    }

    protected RepairRecordRef innerRecover2(DataFileReader<Object> fileReader,
      DataFileWriter<Object> fileWriter, String input,
      boolean recoverPrior, boolean recoverAfter, Schema schema, OutputStream outfile) {
      int numBlocks = 0;
      int numCorruptBlocks = 0;
      int numRecords = 0;
      int numCorruptRecords = 0;
      int recordsWritten = 0;
      long position = fileReader.previousSync();
      long blockSize = 0;
      long blockCount = 0;
      boolean fileWritten = false;
      RepairRecordRef repairRecordRef = new RepairRecordRef(numBlocks, numCorruptBlocks, numRecords,
        numCorruptRecords);
      try {
        while (true) {
          try {
            if (!fileReader.hasNext()) {
              LOG.info("File Summary: ");
              LOG.info("  Number of blocks: " + numBlocks + " Number of corrupt blocks: "
                + numCorruptBlocks);
              LOG.info("  Number of records: " + numRecords + " Number of corrupt records: "
                + numCorruptRecords);
              if (recoverAfter || recoverPrior) {
                LOG.info("  Number of records written " + recordsWritten);
              }
              repairRecordRef = new RepairRecordRef(numBlocks, numCorruptBlocks, numRecords,
                numCorruptRecords);
              return repairRecordRef;
            }
            position = fileReader.previousSync();
            blockCount = fileReader.getBlockCount();
            blockSize = fileReader.getBlockSize();
            numRecords += blockCount;
            long blockRemaining = blockCount;
            numBlocks++;
            boolean lastRecordWasBad = false;
            long badRecordsInBlock = 0;
            while (blockRemaining > 0) {
              try {
                Object datum = fileReader.next();
                if ((recoverPrior && numCorruptBlocks == 0) || (recoverAfter
                  && numCorruptBlocks > 0)) {
                  if (!fileWritten) {
                    try {
                      fileWriter.create(schema, outfile);
                      fileWritten = true;
                    } catch (Exception e) {
                      LOG.error(e.getMessage(), e);
                      return null;
                    }
                  }
                  try {
                    fileWriter.append(datum);
                    recordsWritten++;
                  } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                    throw e;
                  }
                }
                blockRemaining--;
                lastRecordWasBad = false;
              } catch (Exception e) {
                long pos = blockCount - blockRemaining;
                if (badRecordsInBlock == 0) {
                  // first corrupt record
                  numCorruptBlocks++;
                  LOG.info("Corrupt block: " + numBlocks + " Records in block: " + blockCount
                    + " uncompressed block size: " + blockSize);
                  LOG.info("Corrupt record at position: " + (pos));
                } else {
                  // second bad record in block, if consecutive skip block.
                  LOG.info("Corrupt record at position: " + (pos));
                  if (lastRecordWasBad) {
                    // consecutive bad record
                    LOG.info("Second consecutive bad record in block: " + numBlocks
                      + ". Skipping remainder of block. ");
                    numCorruptRecords += blockRemaining;
                    badRecordsInBlock += blockRemaining;
                    try {
                      fileReader.sync(position);
                    } catch (Exception e2) {
                      LOG.error("failed to sync to sync marker, aborting", e2);
                      return null;
                    }
                    break;
                  }
                }
                blockRemaining--;
                lastRecordWasBad = true;
                numCorruptRecords++;
                badRecordsInBlock++;
              }
            }
            if (badRecordsInBlock != 0) {
              LOG.info("** Number of unrecoverable records in block: " + (badRecordsInBlock));
            }
            position = fileReader.previousSync();
          } catch (Exception e) {
            LOG.error(
              "Input file : [" + input + "] Failed to read block " + numBlocks + ". Unknown record "
                + "count in block.  Skipping. Reason: " + e.getMessage(), e);
            numCorruptBlocks++;
            try {
              fileReader.sync(position);
            } catch (Exception e2) {
              LOG.error("failed to sync to sync marker, aborting", e);
              return null;
            }
          }
        }
      } finally {
        if (fileWritten) {
          try {
            fileWriter.close();
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            return null;
          }
        }
        return repairRecordRef;
      }
    }
  }

  static class RepairTaskPipeline {

    Configuration conf;
    ConcurrentHashMap<TaskRef, RepairRunner> taskRef;
    String rootPath;
    FileSystem fileSystem;
    Path path;
    Function<RepairTaskPipeline, Void> back;

    public RepairTaskPipeline(String rootPath,
      Function<RepairTaskPipeline, Void> back) {
      this.conf = new Configuration();
      this.taskRef = new ConcurrentHashMap<>();
      this.rootPath = rootPath;
      this.path = new Path(this.rootPath);
      this.back = back;
      this.init();
    }

    public ConcurrentHashMap<TaskRef, RepairRunner> getTaskRef() {
      return taskRef;
    }

    private void init() {
      try {
        this.fileSystem = FileSystem.get((new Path(HDFS_ROOT + this.rootPath)).toUri(), conf);
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = this.fileSystem
          .listFiles(this.path, true);
        while (fileStatusRemoteIterator.hasNext()) {
          LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
          taskRef.put(
            new TaskRef(fileStatus.getPath().getName(),
              fileStatus.getPath().getParent().toUri().toString(),
              fileStatus.getPath().toUri().toString(),
              fileStatus.getPath().toUri().toString().replaceAll(HDFS_ROOT, HDFS_TEMP_ROOT)),
//            new Runner(new HdfsRecoverTool(this.fileSystem))
            new Runner(new HdfsRecoverTool(this.rootPath))
          );
        }
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        this.back.apply(this);
      }
    }
  }

  /**
   *
   */
  static class TaskRef implements Serializable {

    String name;

    /**
     * 文件所在目录
     */
    String dir;
    /**
     * 完整文件路径
     */
    String path;

    /**
     * 完整修复文件路径
     */
    String repairPath;

    public TaskRef(String name, String dir, String path, String repairPath) {
      this.name = name;
      this.dir = dir;
      this.path = path;
      this.repairPath = repairPath;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public String getRepairPath() {
      return repairPath;
    }

    public void setRepairPath(String repairPath) {
      this.repairPath = repairPath;
    }

    public String getDir() {
      return dir;
    }

    public void setDir(String dir) {
      this.dir = dir;
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getDir(), getPath(), getRepairPath());
    }
  }
}
