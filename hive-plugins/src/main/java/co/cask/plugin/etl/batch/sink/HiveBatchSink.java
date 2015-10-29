package co.cask.plugin.etl.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.plugin.etl.batch.HiveConfig;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by rsinha on 10/28/15.
 */

@Plugin(type = "batchsink")
@Name("Hive")
@Description("Hive Batch Sink")
public class HiveBatchSink extends BatchSink<StructuredRecord, NullWritable, HCatRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(HiveBatchSink.class);
  private static final Gson GSON = new Gson();

  private HiveConfig config;
  private KeyValueTable table;

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    pipelineConfigurer.createDataset(HiveConfig.HIVE_TABLE_SCHEMA_STORE, KeyValueTable.class, DatasetProperties.EMPTY);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    LOG.trace("Hadoop version: {}", VersionInfo.getVersion());
    Job job = context.getHadoopJob();
    Configuration configuration = job.getConfiguration();
    // Ignore the key for the reducer output; emitting an HCatalog record as value
    job.setOutputKeyClass(WritableComparable.class);
    job.setOutputValueClass(DefaultHCatRecord.class);
    job.setOutputFormatClass(HCatOutputFormat.class);


    configuration.set("hive.metastore.uris", config.metaStoreURI);
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      HCatInputFormat.setInput(job, config.dbName, config.tableName);
    } finally {
      Thread.currentThread().setContextClassLoader(classLoader);
    }
    HCatSchema hiveSchema = HCatInputFormat.getTableSchema(configuration);
    List<HCatFieldSchema> fields = hiveSchema.getFields();
    table = context.getDataset(HiveConfig.HIVE_TABLE_SCHEMA_STORE);
    table.write(Joiner.on(":").join(config.dbName, config.tableName), GSON.toJson(fields));
  }
}
