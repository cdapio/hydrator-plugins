package co.cask.plugin.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.plugin.etl.batch.HiveConfig;
import co.cask.plugin.etl.batch.source.HiveBatchSource;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by rsinha on 10/28/15.
 */

@Plugin(type = "batchsink")
@Name("Hive")
@Description("Hive Batch Sink")
public class HiveBatchSink extends BatchSink<StructuredRecord, WritableComparable, HCatRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveBatchSink.class);
  private static final Gson GSON = new Gson();

  private HiveConfig config;
  private KeyValueTable table;
  private RecordToHCatRecordTransformer recordToHCatRecordTransformer;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(HiveBatchSource.HIVE_TABLE_SCHEMA_STORE, KeyValueTable.class, DatasetProperties.EMPTY);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    LOG.trace("Hadoop version: {}", VersionInfo.getVersion());
    Job job = context.getHadoopJob();
    Configuration configuration = job.getConfiguration();
    Credentials credentials = job.getCredentials();
    // Ignore the key for the reducer output; emitting an HCatalog record as value
    job.setOutputKeyClass(WritableComparable.class);
    job.setOutputValueClass(DefaultHCatRecord.class);
    job.setOutputFormatClass(HCatOutputFormat.class);

    LOG.info("### Sink table name is {}", config.tableName);


    configuration.set("hive.metastore.uris", config.metaStoreURI);
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    HCatSchema hiveSchema;
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      HCatOutputFormat.setOutput(configuration, credentials, OutputJobInfo.create(Objects.firstNonNull(config.dbName, HiveBatchSource.DEFAULT_HIVE_DATABASE), config.tableName, null));
      hiveSchema = HCatOutputFormat.getTableSchema(configuration);
      LOG.info("The hive schema in prepareRun is {}", hiveSchema);
      HCatOutputFormat.setSchema(configuration, hiveSchema);
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }

    List<HCatFieldSchema> fields = hiveSchema.getFields();
    table = context.getDataset(HiveBatchSource.HIVE_TABLE_SCHEMA_STORE);
    table.write(Joiner.on(":").join(Objects.firstNonNull(config.dbName, HiveBatchSource.DEFAULT_HIVE_DATABASE), config.tableName), GSON.toJson(fields));
  }

  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    table = context.getDataset(HiveBatchSource.HIVE_TABLE_SCHEMA_STORE);
    String hiveSchema = Bytes.toString(table.read(Joiner.on(":").join(Objects.firstNonNull(config.dbName, HiveBatchSource.DEFAULT_HIVE_DATABASE), config.tableName)));
    List<HCatFieldSchema> fields = GSON.fromJson(hiveSchema, new TypeToken<List<HCatFieldSchema>>() {
    }.getType());
    HCatSchema hCatSchema = new HCatSchema(fields);
    recordToHCatRecordTransformer = new RecordToHCatRecordTransformer(hCatSchema);
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<WritableComparable, HCatRecord>> emitter) throws Exception {
    LOG.info("#### The structured record name {}", input.get("name"));
    HCatRecord hCatRecord = recordToHCatRecordTransformer.toHCatRecord(input);
    LOG.info("The schema for hive is {}", recordToHCatRecordTransformer.gethCatSchema());
    LOG.info("#### The HcatRecord being written to sink: {} and name is {}", hCatRecord.toString(), hCatRecord.get("name", recordToHCatRecordTransformer.gethCatSchema()));

    HCatRecord record = new DefaultHCatRecord(2);
    record.set(0, hCatRecord.get("name", recordToHCatRecordTransformer.gethCatSchema()));
    record.set(1, hCatRecord.get("id", recordToHCatRecordTransformer.gethCatSchema()));
    emitter.emit(new KeyValue<WritableComparable, HCatRecord>(null, record));
  }
}