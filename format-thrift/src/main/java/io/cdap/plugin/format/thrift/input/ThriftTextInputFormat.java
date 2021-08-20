package io.cdap.plugin.format.thrift.input;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.format.input.PathTrackingInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ThriftTextInputFormat extends CombineFileInputFormat<NullWritable, StructuredRecord> {

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        //TODO
        //This needs to be able to understand where exactly in a Thrift file to perform a "safe" split
        return Collections.emptyList();
    }

    @Override
    public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        CombineFileSplit combineSplit = (CombineFileSplit) inputSplit; //TODO maybe we need to extend CombineFileSplit for thrifts
        return new CombineFileRecordReader<>(combineSplit, taskAttemptContext, WrapperReader.class);
    }

    /**
     * A wrapper class that's responsible for delegating to a corresponding RecordReader in
     * {@link PathTrackingInputFormat}. All it does is pick the i'th path in the CombineFileSplit to create a
     * FileSplit and use the delegate RecordReader to read that split.
     */
    public static class WrapperReader extends CombineFileRecordReaderWrapper<NullWritable, StructuredRecord> {

        public WrapperReader(CombineFileSplit split, TaskAttemptContext context,
                             Integer idx) throws IOException, InterruptedException {
            super(new PathTrackingThriftInputFormat(), split, context, idx);
        }
    }
}
