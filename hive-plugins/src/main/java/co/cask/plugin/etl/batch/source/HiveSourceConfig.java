package co.cask.plugin.etl.batch.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.plugin.etl.batch.HiveConfig;

import javax.annotation.Nullable;

/**
 * Configurations for Hive batch source
 */
public class HiveSourceConfig extends HiveConfig {
  @Name(Hive.FILTER)
  @Description("Hive expression filter for scan. This filter must reference only partition columns. " +
    "Values from other columns will cause the pipeline to fail.")
  @Nullable
  public String filter;

  @Name(Hive.SCHEMA)
  @Description("Optional schema to use while reading from Hive table. If no schema is provided then the " +
    "schema of the source table will be used. Note: If you want to use a hive table which has non-primitive types as " +
    "a source then you should provide a schema here with non-primitive fields dropped else your pipeline will fail.")
  @Nullable
  public String schema;
}
