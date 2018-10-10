/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.hydrator.format.input;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.format.FileFormat;
import co.cask.hydrator.format.plugin.FileSourceProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An input format that tracks which the file path each record was read from. This InputFormat is a wrapper around
 * underlying input formats. The responsibility of this class is to keep track of which file each record is reading
 * from, and to add the file URI to each record. In addition, for text files, it can be configured to keep track
 * of the header for the file, which underlying record readers can use.
 *
 * This is tightly coupled with {@link CombinePathTrackingInputFormat}.
 * TODO: (CDAP-14406) clean up File input formats.
 */
public class PathTrackingInputFormat extends FileInputFormat<NullWritable, StructuredRecord> {
  /**
   * This property is used to configure the record readers to emit the header as the first record read,
   * regardless of if it is actually in the input split.
   * This is a hack to support wrangler's parse-as-csv with header and will be removed once
   * there is a proper solution in wrangler.
   */
  public static final String COPY_HEADER = "path.tracking.copy.header";
  static final String PATH_FIELD = "path.tracking.path.field";
  private static final String FILENAME_ONLY = "path.tracking.filename.only";
  private static final String FORMAT = "path.tracking.format";
  private static final String SCHEMA = "path.tracking.schema";

  /**
   * Configure the input format to use the specified schema and optional path field.
   */
  public static void configure(Job job, FileSourceProperties properties, Map<String, String> pluginProperties) {
    Configuration conf = job.getConfiguration();
    String pathField = properties.getPathField();
    if (pathField != null) {
      conf.set(PATH_FIELD, pathField);
    }
    conf.setBoolean(FILENAME_ONLY, properties.useFilenameAsPath());
    FileFormat format = properties.getFormat();
    if (format == null) {
      throw new IllegalArgumentException("A format must be specified.");
    }
    conf.set(FORMAT, format.name());
    Schema schema = properties.getSchema();
    if (schema != null) {
      conf.set(SCHEMA, schema.toString());
    }

    FileInputFormatter inputFormatter = format.getFileInputFormatter(pluginProperties, schema);
    for (Map.Entry<String, String> entry : inputFormatter.getFormatConfig().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    FileFormat fileFormat = FileFormat.valueOf(context.getConfiguration().get(FORMAT));
    return fileFormat != FileFormat.BLOB;
  }

  @Deprecated
  public static Schema getTextOutputSchema(@Nullable String pathField) {
    return TextInputProvider.getDefaultSchema(pathField);
  }

  @Override
  public RecordReader<NullWritable, StructuredRecord> createRecordReader(InputSplit split,
                                                                         TaskAttemptContext context)
    throws IOException, InterruptedException {

    if (!(split instanceof FileSplit)) {
      // should never happen
      throw new IllegalStateException("Input split is not a FileSplit.");
    }
    FileSplit fileSplit = (FileSplit) split;
    Configuration hConf = context.getConfiguration();
    String pathField = hConf.get(PATH_FIELD);
    boolean filenameOnly = hConf.getBoolean(FILENAME_ONLY, false);
    FileFormat format = FileFormat.valueOf(hConf.get(FORMAT));
    String path = filenameOnly ? fileSplit.getPath().getName() : fileSplit.getPath().toUri().toString();
    String schema = hConf.get(SCHEMA);
    Schema parsedSchema = schema == null ? null : Schema.parseJson(schema);

    Map<String, String> properties = new HashMap<>();
    if (pathField != null) {
      properties.put(FileSourceProperties.PATH_FIELD, pathField);
    }
    FileInputFormatter inputFormatter = format.getFileInputFormatter(properties, parsedSchema);
    RecordReader<NullWritable, StructuredRecord.Builder> reader = inputFormatter.create(fileSplit, context);
    return new TrackingRecordReader(reader, pathField, path);
  }

  static class TrackingRecordReader extends RecordReader<NullWritable, StructuredRecord> {
    private final RecordReader<NullWritable, StructuredRecord.Builder> delegate;
    private final String pathField;
    private final String path;

    TrackingRecordReader(RecordReader<NullWritable, StructuredRecord.Builder> delegate,
                         @Nullable String pathField, String path) {
      this.delegate = delegate;
      this.pathField = pathField;
      this.path = path;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      delegate.initialize(split, context);
    }

    @Override
    public NullWritable getCurrentKey() {
      return NullWritable.get();
    }

    @Override
    public StructuredRecord getCurrentValue() throws IOException, InterruptedException {
      StructuredRecord.Builder recordBuilder = delegate.getCurrentValue();
      if (pathField != null) {
        recordBuilder.set(pathField, path);
      }
      return recordBuilder.build();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return delegate.getProgress();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return delegate.nextKeyValue();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }

}
