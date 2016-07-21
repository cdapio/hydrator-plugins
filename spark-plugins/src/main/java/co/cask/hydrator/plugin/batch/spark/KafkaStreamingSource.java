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

package co.cask.hydrator.plugin.batch.spark;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import co.cask.cdap.format.RecordFormats;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import kafka.serializer.DefaultDecoder;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("Kafka")
@Description("Kafka streaming source.")
public class KafkaStreamingSource extends StreamingSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamingSource.class);
  private static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "kafka.record",
    Schema.Field.of("key", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
    Schema.Field.of("message", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));
  private final Conf conf;

  public KafkaStreamingSource(Conf conf) {
    this.conf = conf;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    // check the schema if there is one
    if (!Strings.isNullOrEmpty(conf.schema)) {
      if (Strings.isNullOrEmpty(conf.format)) {
        throw new IllegalArgumentException("You must specify a format to go along with a schema.");
      }
      FormatSpecification formatSpec = conf.getFormatSpec();

      try {
        RecordFormat recordFormat = RecordFormats.createInitializedFormat(formatSpec);
        pipelineConfigurer.getStageConfigurer().setOutputSchema(recordFormat.getSchema());
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid format and schema: " + e.getMessage(), e);
      }
    } else {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(DEFAULT_SCHEMA);
    }
  }

  @Override
  public JavaDStream<StructuredRecord> getStream(JavaStreamingContext javaStreamingContext) throws Exception {
    Map<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", conf.brokers);

    final FormatSpecification formatSpec = conf.getFormatSpec();

    // need this because Session will use the context classloader to instantiate an object.
    // the context classloader here is the etl application's classloader and not this class' classloader.
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    LOG.error("ashau - KafkaUtils' classloader = {}", KafkaUtils.class.getClassLoader());
    LOG.error("ashau - context classloader = {}", oldClassLoader);
    LOG.error("ashau - this classloader = {}", getClass().getClassLoader());

    URLClassLoader kafkaCL = (URLClassLoader) KafkaUtils.class.getClassLoader();
    for (URL url : kafkaCL.getURLs()) {
      LOG.error("ashau ---- url = {}", url);
    }

    JavaPairInputDStream<byte[], byte[]> kafkaStream = KafkaUtils.createDirectStream(
      javaStreamingContext, byte[].class, byte[].class, DefaultDecoder.class, DefaultDecoder.class,
      kafkaParams, conf.getTopics());

    if (formatSpec == null) {
      return kafkaStream.map(new Function<Tuple2<byte[], byte[]>, StructuredRecord>() {
        @Override
        public StructuredRecord call(Tuple2<byte[], byte[]> in) throws Exception {
          return StructuredRecord.builder(DEFAULT_SCHEMA)
            .set("key", in._1())
            .set("message", in._2())
            .build();
        }
      });
    }

    return kafkaStream.map(new FormatFunction(conf.schema, conf.format));
  }

  private static class FormatFunction implements Function<Tuple2<byte[], byte[]>, StructuredRecord> {
    private final String schemaStr;
    private final String format;
    private transient RecordFormat<StreamEvent, StructuredRecord> recordFormat;

    public FormatFunction(String schemaStr, String format) {
      this.schemaStr = schemaStr;
      this.format = format;
    }

    @Override
    public StructuredRecord call(Tuple2<byte[], byte[]> in) throws Exception {
      if (recordFormat == null) {
        Schema schema = Strings.isNullOrEmpty(schemaStr) ? null : Schema.parseJson(schemaStr);
        FormatSpecification spec = new FormatSpecification(format, schema, new HashMap<String, String>());
        recordFormat = RecordFormats.createInitializedFormat(spec);
      }
      return recordFormat.read(new StreamEvent(ByteBuffer.wrap(in._2())));
    }
  }

  /**
   * Conf for Kafka streaming source.
   */
  public static class Conf extends PluginConfig {
    @Description("Comma-separated list of Kafka brokers specified in host1:port1,host2:port2 form.")
    private String brokers;

    @Description("Comma-separated list of Kafka topics to read from.")
    private String topics;

    @Description("Optional schema for the body of Kafka events. The schema is used in conjunction with the format " +
      "to parse Kafka payloads. Some formats (such as the 'avro' format) require schema while others do not. " +
      "The schema given is for the body of the Kafka event.")
    @Nullable
    private String schema;

    @Description("Optional format of the Kafka event. Any format supported by CDAP is supported. " +
      "For example, a value of 'csv' will attempt to parse Kafka payloads as comma-separated values. " +
      "If no format is given, Kafka message payloads will be treated as bytes, resulting in a two-field schema: " +
      "'key' of type bytes and 'message' of type bytes.")
    @Nullable
    private String format;

    private Set<String> getTopics() {
      Set<String> topicSet = new HashSet<>();
      for (String topic : Splitter.on(',').trimResults().split(topics)) {
        topicSet.add(topic);
      }
      return topicSet;
    }

    @Nullable
    private FormatSpecification getFormatSpec() {
      if (Strings.isNullOrEmpty(format)) {
        return null;
      }

      Schema schema = getSchema();
      return new FormatSpecification(format, schema, new HashMap<String, String>());
    }

    private Schema getSchema() {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
      }
    }
  }
}
