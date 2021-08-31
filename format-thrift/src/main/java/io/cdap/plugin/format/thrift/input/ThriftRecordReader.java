package io.cdap.plugin.format.thrift.input;

import com.github.luben.zstd.Zstd;
import com.liveramp.types.parc.ParsedAnonymizedRecord;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.format.thrift.transform.CompactTransformer;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftRecordReader extends RecordReader<NullWritable, StructuredRecord.Builder> {

  private final RecordReader<LongWritable, Text> delegate;
  private Schema schema;
  private CompactTransformer recordTransformer;

  Logger LOG = LoggerFactory.getLogger(ThriftRecordReader.class);

  public ThriftRecordReader(RecordReader<LongWritable, Text> delegate, Schema schema) {
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
    return NullWritable.get();
  }

  @Override
  public StructuredRecord.Builder getCurrentValue() throws IOException, InterruptedException {
    Text text = delegate.getCurrentValue();

    TDeserializer tdes = new TDeserializer(new Factory());
    ParsedAnonymizedRecord result = new ParsedAnonymizedRecord();
    try {
      long size = Zstd.decompressedSize(text.getBytes());
      LOG.debug("decompressed size: " + size);
      LOG.debug("compressed size: " + text.getBytes().length);
      byte[] decompress = Zstd.decompress(text.getBytes(), (int) size * 10);
      LOG.debug("decompressed, decompressed size: " + decompress.length);
      tdes.deserialize(result, decompress);
      LOG.debug("deserialized "+result);
    } catch (TException e) {
      e.printStackTrace();
      LOG.error("error in getCurrentValue for text: " + text, e);
    }

    LOG.debug("PAR is " + result);
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
