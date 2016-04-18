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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.collect.Maps;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;

/**
 * Parses XML Records using XPath.
 */
@Plugin(type = "transform")
@Name("XmlParser")
@Description("Parses XML using XPath")
public final class XmlParser extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  private final Map<String, String> xPathMapping = Maps.newHashMap();
  
  // Output Schema associated with transform output.
  private Schema outSchema;

  // Output Field name to type map
  private Map<String, Schema.Type> outSchemaMap = Maps.newHashMap();

  // Required only for testing.
  public XmlParser(Config config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    
    // Extract name:xpath[,name:xpath]* configuration
    String[] xpaths = config.pathMap.split(",");
    for (String xpath : xpaths) {
      String[] xpathmap = xpath.split(":"); //name:xpath[,name:xpath]*
      xPathMapping.put(xpathmap[0], xpathmap[1]);
    }
    
    try {
      outSchema = Schema.parseJson(config.schema);
      List<Schema.Field> outFields = outSchema.getFields();

      // Checks if all the fields in the XPath mapping are 
      // present in the output schema. If they are not a list 
      // of fields that are not present is included in the 
      // error message. 
      int matched = 0;
      StringBuilder notOutput = new StringBuilder();
      for (Map.Entry<String, String> xPathMap : xPathMapping.entrySet()) {
        for (Schema.Field field : outFields) {
          if (xPathMap.getKey().equalsIgnoreCase(field.getName())) {
            matched++;  
          }
        }
      }
      
      if (matched != xPathMapping.size()) {
        throw new IllegalArgumentException("Following fields are not present in output schema : " +
                                           notOutput.toString());
      }
      
      for (Schema.Field field : outFields) {
        outSchemaMap.put(field.getName(), field.getSchema().getType());
      }
      
    } catch (IOException e) {
      throw new IllegalArgumentException("Format of schema specified is invalid. Please check the format.");
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    InputSource source = new InputSource(new StringReader((String) in.get(config.field)));
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    Document document = db.parse(source);

    XPathFactory xpathFactory = XPathFactory.newInstance();
    XPath xpath = xpathFactory.newXPath();

    StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
    for (Map.Entry<String, Schema.Type> field : outSchemaMap.entrySet()) {
      String value = xpath.evaluate(xPathMapping.get(field.getKey()), document);
      builder.set(field.getKey(), value);
    }
    emitter.emit(builder.build());
  }

  /**
   * XML XPath to Field mapping.
   */
  public static class Config extends PluginConfig {
    
    @Name("field")
    @Description("Specifies the field in input that should be XML transformed.")
    private final String field;
    
    @Name("pathmap")
    @Description("Specifies a mapping from XPath of XML record to field name.")
    private final String pathMap;

    @Name("schema")
    @Description("Specifies the output schema")
    private final String schema;
    
    public Config(String field, String pathMap, String schema) {
      this.field = field;
      this.pathMap = pathMap;
      this.schema = schema;
    }
  }
}
