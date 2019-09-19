/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.spark;

import com.google.common.io.Files;
import io.cdap.cdap.api.data.format.FormatSpecification;
import io.cdap.cdap.api.data.format.RecordFormat;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.format.RecordFormats;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.tephra.TransactionFailureException;
import scala.Tuple2;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Set;

/**
 *
 */
public class TestClass {
  /**
   * Doesn't filter any files.
   */
  public static class NoFilter implements Function<Path, Boolean> {
    @Override
    public Boolean call(Path path) throws Exception {
      return true;
    }
  }

  /**
   * Filters out files that don't have one of the supported extensions.
   */
  public static class ExtensionFilter implements Function<Path, Boolean> {
    private final Set<String> extensions;

    ExtensionFilter(Set<String> extensions) {
      this.extensions = extensions;
    }

    @Override
    public Boolean call(Path path) throws Exception {
      String extension = Files.getFileExtension(path.getName());
      return extensions.contains(extension);
    }
  }

  /**
   * Transforms kafka key and message into a structured record when message format and schema are given.
   * Everything here should be serializable, as Spark Streaming will serialize all functions.
   */
  public static class FormatFunction implements Function<Tuple2<LongWritable, Text>, StructuredRecord> {
    private final String format;
    private final String schemaStr;
    private transient Schema schema;
    private transient RecordFormat<ByteBuffer, StructuredRecord> recordFormat;

    FormatFunction(String format, String schemaStr) {
      this.format = format;
      this.schemaStr = schemaStr;
    }

    @Override
    public StructuredRecord call(Tuple2<LongWritable, Text> in) throws Exception {
      // first time this was called, initialize schema and time, key, and message fields.
      if (recordFormat == null) {
        schema = Schema.parseJson(schemaStr);
        FormatSpecification spec = new FormatSpecification(format, schema, new HashMap<String, String>());
        recordFormat = RecordFormats.createInitializedFormat(spec);
      }

      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      StructuredRecord messageRecord = recordFormat.read(ByteBuffer.wrap(in._2().copyBytes()));
      for (Schema.Field messageField : messageRecord.getSchema().getFields()) {
        String fieldName = messageField.getName();
        builder.set(fieldName, messageRecord.get(fieldName));
      }
      return builder.build();
    }
  }

  public static JavaDStream<StructuredRecord> testMethod(StreamingContext context, FileStreamingSource.Conf conf)
    throws DatasetManagementException, TransactionFailureException {
    context.registerLineage(conf.referenceName);
    JavaStreamingContext jsc = context.getSparkStreamingContext();
    Function<Path, Boolean> filter =
      conf.extensions == null ? new TestClass.NoFilter() : new TestClass.ExtensionFilter(conf.getExtensions());

    jsc.ssc().conf().set("spark.streaming.fileStream.minRememberDuration", conf.ignoreThreshold + "s");
    return jsc.fileStream(conf.path, LongWritable.class, Text.class,
                          TextInputFormat.class, filter, false)
      .map(new FormatFunction(conf.format, conf.schema));
  }
}
