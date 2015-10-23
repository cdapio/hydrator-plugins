package co.cask.plugin.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.plugin.etl.batch.HBaseConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.KeyValueSerialization;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 */
@Plugin(type = "batchsource")
@Name("HBase")
@Description("Read from an HBase table in batch")
public class HBaseSource extends BatchSource<ImmutableBytesWritable, Result, StructuredRecord> {

  private HBaseConfig config;

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();
    job.setInputFormatClass(TableInputFormat.class);
    conf.set(TableInputFormat.INPUT_TABLE, config.tableName);
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, config.columnFamily);
    conf.set("hbase.zookeeper.quorum", config.zkQuorum);
    conf.set("hbase.zookeeper.property.clientPort", config.zkClientPort);
    conf.setStrings("io.serializations", conf.get("io.serializations"),
                    MutationSerialization.class.getName(), ResultSerialization.class.getName(),
                    KeyValueSerialization.class.getName());
    HBaseConfiguration.addHbaseResources(conf);
  }

  @Override
  public void transform(KeyValue<ImmutableBytesWritable, Result> input, Emitter<StructuredRecord> emitter)
    throws Exception {
    System.out.println(Bytes.toString(input.getKey().get()));
    // Use RowRecord Transformer to convert it to Structured Record
  }
}
