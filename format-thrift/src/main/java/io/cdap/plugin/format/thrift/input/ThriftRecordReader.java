package io.cdap.plugin.format.thrift.input;

import com.google.common.annotations.VisibleForTesting;
import com.liveramp.types.parc.ParsedAnonymizedRecord;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.format.thrift.transform.CompactTransformer;
import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftRecordReader extends RecordReader<NullWritable, StructuredRecord.Builder> {

  private final RecordReader<BytesWritable, NullWritable> delegate;
  private final CompactTransformer recordTransformer;

  Logger LOG = LoggerFactory.getLogger(ThriftRecordReader.class);

  public ThriftRecordReader(RecordReader<BytesWritable, NullWritable> delegate) {
    this(delegate, new CompactTransformer());
  }

  @VisibleForTesting
  ThriftRecordReader(RecordReader<BytesWritable, NullWritable> delegate, CompactTransformer recordTransformer) {
    this.delegate = delegate;
    this.recordTransformer = recordTransformer;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    delegate.initialize(inputSplit, taskAttemptContext);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    return delegate.nextKeyValue();
  }

  @Override
  public NullWritable getCurrentKey() {
    return NullWritable.get();
  }

  @Override
  public StructuredRecord.Builder getCurrentValue() throws IOException, InterruptedException {
    BytesWritable key = delegate.getCurrentKey();
    TDeserializer deserializer = new TDeserializer(new Factory());
    ParsedAnonymizedRecord result = new ParsedAnonymizedRecord();
    try {
      deserializer.deserialize(result, key.getBytes());
    } catch (TException e) {
      String message = "Error in getCurrentValue for key : " + key;
      LOG.error(message, e);
      throw new IOException(message, e);
    }

    LOG.info("PAR is " + result);
    return recordTransformer.convertParToStructuredRecord(result);
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return delegate.getProgress();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  RecordReader<BytesWritable, NullWritable> getDelegate() {
    return delegate;
  }
}
