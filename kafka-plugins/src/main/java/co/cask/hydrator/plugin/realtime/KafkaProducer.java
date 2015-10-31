/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.hydrator.plugin.realtime;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.realtime.DataWriter;
import co.cask.cdap.etl.api.realtime.RealtimeContext;
import co.cask.cdap.etl.api.realtime.RealtimeSink;
import co.cask.cdap.etl.common.StructuredRecordStringConverter;
import com.google.common.collect.Lists;
import kafka.producer.ProducerConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.List;
import java.util.Properties;

/**
 * Implementation of Kafka Realtime Producer Hydrator plugin. 
 * 
 * The producer has the capability to transform a {@link StructuredRecord}
 * into a CSV or JSON record and push it on to one or more Kafka topics. 
 * Producer can use one of the fields in the input records to partition the 
 * data. It can also be configured to operate in sync or async mode.  
 */
@Plugin(type = "realtimesink")
@Name("KafkaProducer")
@Description("Real-time Kafka producer")
public class KafkaProducer extends RealtimeSink<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);

  // Configuration for the plugin.
  private final Config sconfig;
  
  // Static constants for configuring Kafka producer. 
  private static final String BROKER_LIST = "bootstrap.servers";
  private static final String KEY_SERIALIZER = "key.serializer";
  private static final String VAL_SERIALIZER = "value.serializer";
  private static final String CLIENT_ID = "client.id";
  private static final String ACKS_REQUIRED = "request.required.acks";

  // Kafka properties
  private final Properties props = new Properties();
  
  // Kafka producer configuration
  private ProducerConfig config;
  
  // Kafka producer handle
  private org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
  
  // Plugin context
  private RealtimeContext context;
  
  // Optimization to collect fields extracted. This is required because the schema
  // is not available during initialization and configuration phase.
  private boolean fieldsExtracted = false;
  
  // If Async mode
  private boolean isAsync = false;
  
  // List of Kafka topics.
  private String[] topics;
  
  // required for testing.
  public KafkaProducer(Config config) {
    this.sconfig = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    
    if(!sconfig.isAsync.equalsIgnoreCase("true") && !sconfig.isAsync.equalsIgnoreCase("false")) {
      throw new IllegalArgumentException("Async flag has to be either TRUE or FALSE.");
    }
    
    // Validations to be added.
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);
    this.context = context;

    // Extract the topics
    topics = sconfig.topics.split(",");
    
    // Configure the properties for kafka.
    props.put(BROKER_LIST, sconfig.brokers);
    props.put(KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(VAL_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(CLIENT_ID, "kafka-producer-" + context.getInstanceId());
    if (sconfig.isAsync.equalsIgnoreCase("TRUE")) {
      props.put(ACKS_REQUIRED, "1");
      isAsync = true;
    }
    
    //config = new ProducerConfig(props);
    producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
    
  }
  
  @Override
  public int write(Iterable<StructuredRecord> objects, final DataWriter dataWriter) throws Exception {
    int count = 0;

    List<Schema.Field> fields = Lists.newArrayList();
    
    // For each object
    for (StructuredRecord object : objects) {
      
      // Extract the field names from the object passed in. This is required
      // because this information is not available in initialize or configuration phase.
      if(!fieldsExtracted) {
        fields = object.getSchema().getFields();
        fieldsExtracted = true;
      }
      
      // Depending on the configuration create a body that needs to be 
      // built and pushed to Kafka. 
      String body = "";
      if (sconfig.format.equalsIgnoreCase("JSON")) {
        body = StructuredRecordStringConverter.toJsonString(object);
      } else {
        // Extract all values from the structured record
        List<Object> objs = Lists.newArrayList();
        for(Schema.Field field : fields) {
          objs.add(object.get(field.getName()));
        }

        StringWriter writer = new StringWriter();
        CSVPrinter printer = null;
        CSVFormat csvFileFormat;
        switch(sconfig.format.toLowerCase()) {
          case "csv":
            csvFileFormat = CSVFormat.Predefined.Default.getFormat();
            printer = new CSVPrinter(writer, csvFileFormat);
            break;
          
          case "excel":
            csvFileFormat = CSVFormat.Predefined.Excel.getFormat();
            printer = new CSVPrinter(writer, csvFileFormat);
            break;
          
          case "mysql":
            csvFileFormat = CSVFormat.Predefined.MySQL.getFormat();
            printer = new CSVPrinter(writer, csvFileFormat);
            break;
          
          case "tdf":
            csvFileFormat = CSVFormat.Predefined.TDF.getFormat();
            printer = new CSVPrinter(writer, csvFileFormat);
            break;
          
          case "rfc4180":
            csvFileFormat = CSVFormat.Predefined.TDF.getFormat();
            printer = new CSVPrinter(writer, csvFileFormat);
            break;
        }
        
        if (printer != null) {
          printer.printRecord(objs);
          body = writer.toString();
        }
      }
      
      // Message key.
      String key = "no_key";
      if(sconfig.key != null) {
        key = object.get(sconfig.key);    
      }
      
      // Extract the partition key from the record. If the partition key is 
      // Integer then we use it as-is else
      Integer partitionKey = 0;
      if(sconfig.partitionField != null) {
        if(object.get(sconfig.partitionField).getClass().isInstance(Integer.class)) {
          partitionKey = object.get(sconfig.partitionField);  
        } else {
          partitionKey = object.get(sconfig.partitionField).hashCode();
        }
      }

      // Write to all the configured topics
      for(String topic : topics) {
        partitionKey = partitionKey % producer.partitionsFor(topic).size();
        if (isAsync) {
          producer.send(new ProducerRecord<String, String>(topic, partitionKey, key, body), new Callback() {
            @Override
            public void onCompletion(RecordMetadata meta, Exception e) {
              if (meta != null) {
                context.getMetrics().count("kafka.async.success", 1);
              }
              
              if (e != null) {
                context.getMetrics().count("kafka.async.error", 1);  
              }
            }
          });
        } else {
          // Waits infinitely to push the message through. 
          producer.send(new ProducerRecord<String, String>(topic, partitionKey, key, body)).get();
        }
        context.getMetrics().count("kafka.producer.count", 1);
      }
    }
    return count;
  }
  
  @Override
  public void destroy() {
    super.destroy();
    producer.close();
  }

  /**
   * Kafka Producer Configuration.
   */
  public static class Config extends PluginConfig {
    
    @Name("brokers")
    @Description("Specifies the connection string where Producer can find one or more brokers to " +
      "determine the leader for each topic")
    private String brokers;
    
    @Name("isasync")
    @Description("Specifies whether an acknowledgment is required from broker that message was received. " +
      "Default is FALSE")
    private String isAsync;
    
    @Name("partitionfield")
    @Description("Specify field that should be used as partition ID. Should be a int or long")
    private String partitionField;

    @Name("key")
    @Description("Specify the key field to be used in the message")
    private String key;
    
    @Name("topics")
    @Description("List of topics to which message needs to be published")
    private String topics;
    
    @Name("format")
    @Description("Format a structured record should be converted to")
    private String format;
    
    public Config(String brokers, String isAsync, String partitionField, String key, String topics,
                  String format) {
      this.brokers = brokers;
      this.isAsync = isAsync;
      this.partitionField = partitionField;
      this.key = key;
      this.topics = topics;
      this.format = format;
    }
  }
}
