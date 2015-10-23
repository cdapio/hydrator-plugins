package co.cask.plugin.etl.batch;

import co.cask.cdap.api.plugin.PluginConfig;

/**
*
*/
public class HBaseConfig extends PluginConfig {

  public String tableName;

  public String columnFamily;

  public String schema;

  public String zkQuorum;

  public String zkClientPort;

  public String rowField;
}
