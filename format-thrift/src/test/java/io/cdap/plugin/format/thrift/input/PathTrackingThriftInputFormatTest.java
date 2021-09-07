package io.cdap.plugin.format.thrift.input;

import static org.junit.Assert.assertTrue;

import io.cdap.cdap.api.data.format.StructuredRecord.Builder;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.junit.Test;

public class PathTrackingThriftInputFormatTest {

  @Test
  public void createRecordReader_readerIsCreatedWithDelegate_typeIsThriftRecordReader() {
    // GIVEN
    // WHEN
    PathTrackingThriftInputFormat pathTrackingThriftInputFormat = new PathTrackingThriftInputFormat();
    RecordReader<NullWritable, Builder> recordReader = pathTrackingThriftInputFormat
        .createRecordReader(null, null, null, null);

    // THEN
    assertTrue(recordReader instanceof ThriftRecordReader);
    assertTrue(((ThriftRecordReader) recordReader).getDelegate() instanceof SequenceFileRecordReader);
  }
}