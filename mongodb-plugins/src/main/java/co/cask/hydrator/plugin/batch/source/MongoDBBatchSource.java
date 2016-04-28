/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.plugin.BSONConverter;
import com.google.common.base.Strings;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.splitter.MongoSplitter;
import com.mongodb.hadoop.splitter.StandaloneMongoSplitter;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.bson.BSONObject;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * A {@link BatchSource} that reads data from MongoDB and converts each document into 
 * a {@link StructuredRecord} with the help of the specified Schema.
 */
@Plugin(type = "batchsource")
@Name("MongoDB")
@Description("MongoDB Batch Source will read documents from MongoDB and convert each document " +
  "into a StructuredRecord with the help of the specified Schema. ")
public class MongoDBBatchSource extends ReferenceBatchSource<Object, BSONObject, StructuredRecord> {

  private final MongoDBConfig config;
  private BSONConverter bsonConverter;

  public MongoDBBatchSource(MongoDBConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    try {
      BSONConverter.validateSchema(Schema.parseJson(config.schema));
      pipelineConfigurer.getStageConfigurer().setOutputSchema(Schema.parseJson(config.schema));
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid output schema : " + e.getMessage(), e);
    }
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Configuration conf = new Configuration();
    conf.clear();

    MongoConfigUtil.setInputFormat(conf, MongoInputFormat.class);
    MongoConfigUtil.setInputURI(conf, config.connectionString);
    if (!Strings.isNullOrEmpty(config.inputQuery)) {
      MongoConfigUtil.setQuery(conf, config.inputQuery);
    }
    if (!Strings.isNullOrEmpty(config.authConnectionString)) {
      MongoConfigUtil.setAuthURI(conf, config.authConnectionString);
    }
    if (!Strings.isNullOrEmpty(config.inputFields)) {
      MongoConfigUtil.setFields(conf, config.inputFields);
    }
    if (!Strings.isNullOrEmpty(config.splitterClass)) {
      String className = String.format("%s.%s", StandaloneMongoSplitter.class.getPackage().getName(),
                                       config.splitterClass);
      Class<? extends MongoSplitter> klass = getClass().getClassLoader().loadClass(
        className).asSubclass(MongoSplitter.class);
      MongoConfigUtil.setSplitterClass(conf, klass);
    }

    context.setInput(Input.of(config.referenceName,
                              new SourceInputFormatProvider(MongoConfigUtil.getInputFormat(conf), conf)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    bsonConverter = new BSONConverter(Schema.parseJson(config.schema));
  }

  @Override
  public void transform(KeyValue<Object, BSONObject> input, Emitter<StructuredRecord> emitter) throws Exception {
    BSONObject bsonObject = input.getValue();
    emitter.emit(bsonConverter.transform(bsonObject));
  }

  /**
   * Config class for {@link MongoDBBatchSource}.
   */
  public static class MongoDBConfig extends ReferencePluginConfig {

    @Name(Properties.CONNECTION_STRING)
    @Description("MongoDB Connection String (see http://docs.mongodb.org/manual/reference/connection-string); " +
      "Example: 'mongodb://localhost:27017/analytics.users'.")
    private String connectionString;

    @Name(Properties.AUTH_CONNECTION_STRING)
    @Nullable
    @Description("Auxiliary MongoDB connection string to authenticate against when constructing splits.")
    private String authConnectionString;

    @Name(Properties.SCHEMA)
    @Description("The schema for the data as it will be formatted in CDAP. Sample schema: {\n" +
      "    \"type\": \"record\",\n" +
      "    \"name\": \"schemaBody\",\n" +
      "    \"fields\": [\n" +
      "        {\n" +
      "            \"name\": \"name\",\n" +
      "            \"type\": \"string\"\n" +
      "        },\n" +
      "        {\n" +
      "            \"name\": \"age\",\n" +
      "            \"type\": \"int\"\n" +
      "        }" +
      "    ]\n" +
      "}")
    private String schema;

    @Name(Properties.INPUT_QUERY)
    @Description("Optionally filter the input collection with a query. This query must be represented in JSON " +
      "format, and use the MongoDB extended JSON format to represent non-native JSON data types.")
    @Nullable
    private String inputQuery;


    @Name(Properties.INPUT_FIELDS)
    @Nullable
    @Description("A projection document limiting the fields that appear in each document. " +
      "If no projection document is provided, all fields will be read.")
    private String inputFields;

    @Name(Properties.SPLITTER_CLASS)
    @Nullable
    @Description("The name of the Splitter class to use. If left empty, the MongoDB Hadoop Connector will attempt " +
      "to make a best guess as to what Splitter to use.")
    private String splitterClass;

    public MongoDBConfig(String referenceName, String connectionString, String authConnectionString,
                         String schema, String inputQuery, String inputFields, String splitterClass) {
      super(referenceName);
      this.connectionString = connectionString;
      this.authConnectionString = authConnectionString;
      this.schema = schema;
      this.inputQuery = inputQuery;
      this.inputFields = inputFields;
      this.splitterClass = splitterClass;
    }
  }

  /**
   * Property names for the config.
   */
  public static class Properties {
    public static final String AUTH_CONNECTION_STRING = "authConnectionString";
    public static final String CONNECTION_STRING = "connectionString";
    public static final String SCHEMA = "schema";
    public static final String INPUT_QUERY = "inputQuery";
    public static final String INPUT_FIELDS = "inputFields";
    public static final String SPLITTER_CLASS = "splitterClass";
  }
}
