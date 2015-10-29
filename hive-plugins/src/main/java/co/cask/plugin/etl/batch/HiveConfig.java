package co.cask.plugin.etl.batch;

import co.cask.cdap.api.plugin.PluginConfig;

/**
 * Created by rsinha on 10/26/15.
 */
public class HiveConfig extends PluginConfig {

  public static final String HIVE_TABLE_SCHEMA_STORE = "hiveTableSchemaStore";

  public String dbName;

  public String tableName;

  public String metaStoreURI;
}
