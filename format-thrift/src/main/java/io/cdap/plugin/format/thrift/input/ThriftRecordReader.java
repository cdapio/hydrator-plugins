package io.cdap.plugin.format.thrift.input;

import com.liveramp.types.parc.ParsedAnonymizedRecord;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
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
  private Schema schema;
  private CompactTransformer recordTransformer;

  Logger LOG = LoggerFactory.getLogger(ThriftRecordReader.class);

  public ThriftRecordReader(RecordReader<BytesWritable, NullWritable> delegate, Schema schema) {
    this.delegate = delegate;
    this.schema = schema;
    this.recordTransformer = new CompactTransformer();
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
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    BytesWritable text = delegate.getCurrentKey();
    LOG.info("current key text: " + text);
    LOG.info("current key to string: " + text.toString());
    return NullWritable.get();
  }

  @Override
  public StructuredRecord.Builder getCurrentValue() throws IOException, InterruptedException {
    NullWritable text = delegate.getCurrentValue();
    LOG.info("current value text: " + text);
    LOG.info("current value to string: " + text.toString());
    BytesWritable keyText = delegate.getCurrentKey();
    LOG.info("current key text: " + new String(keyText.getBytes()));
    LOG.info("current key to string: " + keyText.toString());

    TDeserializer tdes = new TDeserializer(new Factory());
    ParsedAnonymizedRecord result = new ParsedAnonymizedRecord();
    try {
      tdes.deserialize(result, keyText.getBytes());
      LOG.info("deserialized "+result);
    } catch (TException e) {
      LOG.error("error in getCurrentValue for text: " + text, e);
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
}
