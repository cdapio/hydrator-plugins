package org.apache.hadoop.hive.ql.io.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hive.hcatalog.common.HCatConstants;

import java.io.IOException;
import java.rmi.server.UID;
import java.util.Map;
import java.util.Properties;

/**
 * RecordReader optimized against Avro GenericRecords that returns to record
 * as the value of the k-v pair, as Hive requires.
 *
 * Patched so that it won't read the wrong schema when reading from and writing to different avro tables.
 */
public class AvroGenericRecordReader implements
  RecordReader<NullWritable, AvroGenericRecordWritable>, JobConfigurable {
  private static final Log LOG = LogFactory.getLog(AvroGenericRecordReader.class);

  final private org.apache.avro.file.FileReader<GenericRecord> reader;
  final private long start;
  final private long stop;
  protected JobConf jobConf;
  /**
   * A unique ID for each record reader.
   */
  final private UID recordReaderID;

  public AvroGenericRecordReader(JobConf job, FileSplit split, Reporter reporter) throws IOException {
    this.jobConf = job;
    Schema latest;

    String jobString = job.get(HCatConstants.HCAT_KEY_JOB_INFO);
    if (jobString == null) {
      throw new IOException("hcatalog job info not found in JobContext. Was HCatInputFormat.setInput() called?");
    }

    try {
      latest = getSchema(job, split);
    } catch (AvroSerdeException e) {
      throw new IOException(e);
    }
    LOG.info("Creating AvroGenericRecordReader", new RuntimeException());

    GenericDatumReader<GenericRecord> gdr = new GenericDatumReader<GenericRecord>();

    if(latest != null) {
      LOG.info("AvroGenericRecordReader schema = " + latest.toString(true), new RuntimeException());
      gdr.setExpected(latest);
    }

    this.reader = new DataFileReader<GenericRecord>(new FsInput(split.getPath(), job), gdr);
    this.reader.sync(split.getStart());
    this.start = reader.tell();
    this.stop = split.getStart() + split.getLength();
    this.recordReaderID = new UID();
  }

  /**
   * Attempt to retrieve the reader schema.  We have a couple opportunities
   * to provide this, depending on whether or not we're just selecting data
   * or running with a MR job.
   * @return  Reader schema for the Avro object, or null if it has not been provided.
   * @throws AvroSerdeException
   */
  private Schema getSchema(JobConf job, FileSplit split) throws AvroSerdeException, IOException {
    // Inside of a MR job, we can pull out the actual properties
    if(AvroSerdeUtils.insideMRJob(job)) {
      MapWork mapWork = Utilities.getMapWork(job);

      // Iterate over the Path -> Partition descriptions to find the partition
      // that matches our input split.
      for (Map.Entry<String,PartitionDesc> pathsAndParts: mapWork.getPathToPartitionInfo().entrySet()){
        String partitionPath = pathsAndParts.getKey();
        if(pathIsInPartition(split.getPath(), partitionPath)) {
          if(LOG.isInfoEnabled()) {
            LOG.info("Matching partition " + partitionPath +
                       " with input split " + split);
          }

          Properties props = pathsAndParts.getValue().getProperties();
          if(props.containsKey(AvroSerdeUtils.SCHEMA_LITERAL) || props.containsKey(AvroSerdeUtils.SCHEMA_URL)) {
            return AvroSerdeUtils.determineSchemaOrThrowException(job, props);
          }
          else {
            return null; // If it's not in this property, it won't be in any others
          }
        }
      }
      if(LOG.isInfoEnabled()) {
        LOG.info("Unable to match filesplit " + split + " with a partition.");
      }
    }

    // commenting this out. This is the patch.
    // AvroSerdeUtils.AVRO_SERDE_SCHEMA can have the schema of the other table if there are multiple tables involved
    // in this mapreduce job. So just force the reader to re-encode.

    // In "select * from table" situations (non-MR), we can add things to the job
    // It's safe to add this to the job since it's not *actually* a mapred job.
    // Here the global state is confined to just this process.
    /*String s = job.get(AvroSerdeUtils.AVRO_SERDE_SCHEMA);
    if(s != null) {
      LOG.info("Found the avro schema in the job: " + s);
      return AvroSerdeUtils.getSchemaFor(s);
    }*/
    // No more places to get the schema from. Give up.  May have to re-encode later.
    return null;
  }

  private boolean pathIsInPartition(Path split, String partitionPath) {
    boolean schemeless = split.toUri().getScheme() == null;
    if (schemeless) {
      String schemelessPartitionPath = new Path(partitionPath).toUri().getPath();
      return split.toString().startsWith(schemelessPartitionPath);
    } else {
      return split.toString().startsWith(partitionPath);
    }
  }


  @Override
  public boolean next(NullWritable nullWritable, AvroGenericRecordWritable record) throws IOException {
    if(!reader.hasNext() || reader.pastSync(stop)) {
      return false;
    }

    GenericData.Record r = (GenericData.Record)reader.next();
    record.setRecord(r);
    record.setRecordReaderID(recordReaderID);
    record.setFileSchema(reader.getSchema());

    return true;
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public AvroGenericRecordWritable createValue() {
    return new AvroGenericRecordWritable();
  }

  @Override
  public long getPos() throws IOException {
    return reader.tell();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public float getProgress() throws IOException {
    return stop == start ? 0.0f
      : Math.min(1.0f, (getPos() - start) / (float)(stop - start));
  }

  @Override
  public void configure(JobConf jobConf) {
    this.jobConf= jobConf;
  }
}
