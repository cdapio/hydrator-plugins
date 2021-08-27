package io.cdap.plugin.format.thrift.input;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.format.thrift.transform.CompactTransformer;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import org.apache.thrift.TBase;

public class ThriftRecordReader extends RecordReader<NullWritable, StructuredRecord.Builder> {

    private final RecordReader<Void, TBase> delegate;
    private Schema schema;
    private CompactTransformer recordTransformer;

    public ThriftRecordReader(RecordReader<Void, TBase> delegate, Schema schema) {
        this.delegate = delegate;
        this.schema = schema;
        this.recordTransformer = new CompactTransformer();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
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
        TBase tbase = delegate.getCurrentValue();

        return recordTransformer.convertParToStructuredRecord(tbase);
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
