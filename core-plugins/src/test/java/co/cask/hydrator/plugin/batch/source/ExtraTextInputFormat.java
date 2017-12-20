package co.cask.hydrator.plugin.batch.source;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class ExtraTextInputFormat extends TextInputFormat {

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
    final RecordReader<LongWritable, Text> reader = super.createRecordReader(split, context);
    final String extraText = context.getConfiguration().get("extra.text", "extra: ");
    return new RecordReader<LongWritable, Text>() {
      @Override
      public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
        throws IOException, InterruptedException {
        reader.initialize(inputSplit, taskAttemptContext);
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        return reader.nextKeyValue();
      }

      @Override
      public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return reader.getCurrentKey();
      }

      @Override
      public Text getCurrentValue() throws IOException, InterruptedException {
        return new Text(extraText + reader.getCurrentValue().toString());
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }
    };
  }
}
