package co.cask.hydrator.plugin;


import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.EndpointPluginContext;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.collect.Lists;

import java.util.List;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Path;

@Plugin(type = "transform")
@Name("OmnitureParser")
@Description("Omniture Parser supports parsing of Click stream, Users and Products.")
public class OmnitureParser extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;

  // This is used only for tests, otherwise this is being injected by the ingestion framework.
  public OmnitureParser(Config config) {
    this.config = config;
  }

  /**
   * Request Object for the plugin to retrieve the schema based on the 
   * Omniture File type.  
   */
  class GetOmnitureFileSchemaRequest {
    public String type;
  }

  /**
   * Endpoint to get schema based on type of omniture file format
   *
   * @param request {@link GetOmnitureFileSchemaRequest} Omniture file type. 
   * @param pluginContext context to create plugins
   * @return schema of fields
   */
  @Path("getSchema")
  public Schema getSchema(GetOmnitureFileSchemaRequest request,
                          EndpointPluginContext pluginContext) throws InstantiationException, IllegalAccessException,
    BadRequestException {
    
    if (request.type == null || request.type.isEmpty()) {
      throw new BadRequestException("Empty Omniture file type specified.");
    }

    List<Schema.Field> fields = Lists.newArrayList();
    if (request.type.equalsIgnoreCase("clickstream")) {
      for(int i = 0; i < 178; ++i) {
        String name = String.format("col_%d", i);
        fields.add(Schema.Field.of(name, Schema.of(Schema.Type.STRING)));
      }
    }

    // https://github.com/msukmanowsky/OmnitureDataFileInputFormat/tree/master/test
    if (request.type.equalsIgnoreCase("hit_data")) {
      for(int i = 0; i < 178; ++i) {
        String name = String.format("col_%d", i);
        fields.add(Schema.Field.of(name, Schema.of(Schema.Type.STRING)));
      }
    }

    return Schema.recordOf(request.type, fields);
  }


  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    
    if(config.type == null || config.type.isEmpty()) {
      throw new IllegalArgumentException("")
    }
        
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
  }
  
  @Override
  public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {

  }

  /**
   * Configuration for the plugin.
   */
  public static class Config extends PluginConfig {
    @Name("format")
    @Description("Specifies the omniture file type to be parsed")
    private final String type;

    public Config(String type) {
      this.type = type;
    }
  }
  
}
