/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static co.cask.hydrator.plugin.batch.CopyBookSource.COPYBOOK_INPUTFORMAT_DATA_HDFS_PATH;

/**
 * InputFormat class for CopybookReader plugin
 */
public class CopyBookInputFormat extends FileInputFormat<LongWritable, Map<String, String>> {

  @Override
  public RecordReader<LongWritable, Map<String, String>> createRecordReader(InputSplit split,
                                                                            TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new CopyBookRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    Configuration conf = context.getConfiguration();
    Path path = new Path(conf.get(COPYBOOK_INPUTFORMAT_DATA_HDFS_PATH));
    final CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(path);
    return (null == codec) ? true : codec instanceof SplittableCompressionCodec;
  }

  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    Configuration conf = job.getConfiguration();
    Path path = new Path(conf.get(COPYBOOK_INPUTFORMAT_DATA_HDFS_PATH));
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    FileSplit split = new FileSplit(path, 0, fs.getFileStatus(path).getLen(), null);
    List<InputSplit> fileSplits = new ArrayList<InputSplit>();
    fileSplits.add(split);
    return fileSplits;
  }
}
