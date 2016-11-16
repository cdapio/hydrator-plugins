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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("FieldAppender")
@Description("Appends fields to a record as it passes by.")
public final class FieldAppender extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(FieldAppender.class);

  // Number of 100ns intervals since 15 October 1582 00:00:000000000 till UNIX epoch
  private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
  // Multiplier to convert millisecond into 100ns
  private static final long HUNDRED_NANO_MULTIPLIER = 10000;

  private final Pattern datetimePattern = Pattern.compile("~datetime:(.+?)~");
  private final Pattern fieldPattern = Pattern.compile("~field:(.+?)~");

  // Output Schema associated with transform output.
  private Schema outSchema;
  private Map<String, String> toAppend;
  private static Random random;
  private static AtomicLong counter;
  private final Config config;

  // Required only for testing.
  public FieldAppender(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
    try {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    try {
      config.validate();
      outSchema = Schema.parseJson(config.schema);
      toAppend = config.parseFieldsToAppend();
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
    random = new Random();
    counter = new AtomicLong();
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    List<Schema.Field> fields = in.getSchema().getFields();
    StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
    for (Schema.Field field : fields) {
      String name = field.getName();
      if (outSchema.getField(name) != null) {
        builder.set(name, in.get(name));
      }
    }
    for (Map.Entry<String, String> fieldToAdd : toAppend.entrySet()) {
      String valueToAdd = fieldToAdd.getValue();
      if (valueToAdd.matches(".*~.+~.*")) {
        valueToAdd = miniMacroReplace(valueToAdd, in);
      }
      builder.set(fieldToAdd.getKey(), valueToAdd);
    }
    emitter.emit(builder.build());
  }

  private String miniMacroReplace(String valueToAdd, StructuredRecord in) {
    long ts = System.currentTimeMillis();
    Date date = new Date(ts);
    SimpleDateFormat dateFormat;

    // Add all the macro replacements here
    String retStr = valueToAdd.replaceAll("~uuid~", generateUUIDForTime(ts).toString());
    Matcher matcher = datetimePattern.matcher(retStr);
    while (matcher.find()) {
      dateFormat = new SimpleDateFormat(matcher.group(1));
      retStr = retStr.replaceAll(matcher.group(0), dateFormat.format(date));
    }
    matcher = fieldPattern.matcher(retStr);
    while (matcher.find()) {
      if (in.get(matcher.group(1)) != null) {
        retStr = retStr.replaceAll(matcher.group(0), String.valueOf(in.get(matcher.group(1))));
      }
    }
    return retStr;
  }

  /** Generates an unique ID for a running program using type 1 and variant 2 time-based {@link UUID}.
   *  Uses the same logic found in co.cask.cdap.common.app.RunIds
   */
  static UUID generateUUIDForTime(long timeInMillis) {
    // Use system time in milliseconds to generate time in 100ns.
    // Use counter to ensure unique time gets generated for the same millisecond (up to HUNDRED_NANO_MULTIPLIER)
    // Hence the time is valid only for millisecond precision, event though it represents 100ns precision.
    long ts = timeInMillis * HUNDRED_NANO_MULTIPLIER + counter.incrementAndGet() % HUNDRED_NANO_MULTIPLIER;
    long time = ts + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;

    long timeLow = time &       0xffffffffL;
    long timeMid = time &   0xffff00000000L;
    long timeHi = time & 0xfff000000000000L;
    long upperLong = (timeLow << 32) | (timeMid >> 16) | (1 << 12) | (timeHi >> 48);

    // Random clock ID
    int clockId = random.nextInt() & 0x3FFF;
    long nodeId;

    try {
      Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
      NetworkInterface networkInterface = null;
      while (interfaces.hasMoreElements()) {
        networkInterface = interfaces.nextElement();
        if (!networkInterface.isLoopback()) {
          break;
        }
      }
      byte[] mac = networkInterface == null ? null : networkInterface.getHardwareAddress();
      if (mac == null) {
        nodeId = (random.nextLong() & 0xFFFFFFL) | 0x100000L;
      } else {
        nodeId = Longs.fromBytes((byte) 0, (byte) 0, mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
      }

    } catch (SocketException e) {
      // Generate random node ID
      nodeId = random.nextLong() & 0xFFFFFFL | 0x100000L;
    }

    long lowerLong = ((long) clockId | 0x8000) << 48 | nodeId;

    return new java.util.UUID(upperLong, lowerLong);
  }

  /**
   * FieldAppender plugin configuration.
   */
  public static class Config extends PluginConfig {
    private static final String DELIMITER = "\n";
    private static final String KV_DELIMITER = "\t";

    @Name("fieldsToAppend")
    @Description("Specifies the fields and values to append.")
    @Macro
    private final String fieldsToAppend;

    @Name("schema")
    @Description("Specifies the schema that has to be output.")
    private final String schema;

    public Config(String fieldsToAppend, String schema) {
      this.fieldsToAppend = fieldsToAppend;
      this.schema = schema;
    }

    public Map<String, String> parseFieldsToAppend() {
      if (Strings.isNullOrEmpty(fieldsToAppend)) {
        return Maps.newHashMap();
      }
      try {
        List<String> fields = Arrays.asList(fieldsToAppend.split(DELIMITER));
        Map<String, String> toAppend = Maps.newHashMap();
        for (String field : fields) {
          String[] keyValue = field.split(KV_DELIMITER);
          toAppend.put(keyValue[0], keyValue[1]);
        }
        return toAppend;
      } catch (Exception e) {
        throw new IllegalArgumentException("Was not able to parse fieldsToAppend.");
      }
    }

    public void validate() throws IllegalArgumentException {
      // Check to make sure fields in fieldsToAppend are in output schema
      Map<String, String> toAppend = parseFieldsToAppend();
      try {
        Schema outSchema = Schema.parseJson(schema);
        for (String key : toAppend.keySet()) {
          if (outSchema.getField(key) == null) {
            throw new IllegalArgumentException("Output Schema must contain all fields to add.");
          }
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse output schema.");
      }
    }
  }
}
