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
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

/**
 * Parses XML Event using XPath.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("XMLMultiParser")
@Description("Parse multiple records from an XML documents based on XPath")
public class XMLMultiParser extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(XMLMultiParser.class);

  private final Config config;
  private final DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
  private Schema schema;
  private XPathExpression xPathExpression;
  private Set<String> fieldNames;

  public XMLMultiParser(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    config.validate(pipelineConfigurer.getStageConfigurer().getInputSchema());
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    schema = config.getSchema();
    xPathExpression = config.getXPathExpression();
    fieldNames = new HashSet<>();
    for (Schema.Field field : schema.getFields()) {
      fieldNames.add(field.getName());
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws IOException {

    String xmlStr = input.get(config.field);
    if (xmlStr == null) {
      return;
    }

    try (Reader reader = new StringReader((String) input.get(config.field))) {
      InputSource source = new InputSource(reader);
      source.setEncoding(config.encoding);
      Document document;
      try {
        DocumentBuilder documentBuilder = builderFactory.newDocumentBuilder();
        document = documentBuilder.parse(source);
      } catch (ParserConfigurationException e) {
        // shouldn't happen
        throw new RuntimeException("Unable to create document builder.", e);
      } catch (SAXException e) {
        LOG.error("Unable to parse the xml document. This record will be dropped.", e);
        emitter.emitError(new InvalidEntry<>(31, "Unable to parse the xml document. This record will be dropped.",
                                             input));
        return;
      }

      NodeList nodeList;
      try {
        nodeList = (NodeList) xPathExpression.evaluate(document, XPathConstants.NODESET);
      } catch (XPathExpressionException e) {
        LOG.error("Unable to evaluate xpath for the xml document. This record will be dropped.", e);
        emitter.emitError(new InvalidEntry<>(31, "Unable to evaluate xpath for the xml document. This record will be " +
          "dropped.", input));
        return;
      }

      for (int i = 0; i < nodeList.getLength(); i++) {
        Node node = nodeList.item(i);

        NodeList children = node.getChildNodes();

        try {
          StructuredRecord.Builder builder = StructuredRecord.builder(schema);
          for (int j = 0; j < children.getLength(); j++) {
            Node childNode = children.item(j);
            String nodeName = childNode.getNodeName();

            if (fieldNames.contains(nodeName)) {
              builder.convertAndSet(nodeName, childNode.getTextContent());
            }
          }
          emitter.emit(builder.build());
        } catch (Exception e) {
          LOG.error("Unable to create a record from the xpath element. This record will be dropped.", e);
          emitter.emitError(new InvalidEntry<>(31, "Unable to create a record from the xpath element. This record " +
            "will be dropped.", input));
        }
      }
    }
  }

  /**
   * Configuration for the XMLParser transform..
   */
  public static class Config extends PluginConfig {
    private static final XPathFactory X_PATH_FACTORY = XPathFactory.newInstance();

    @Description("The field containing the XML document to parse.")
    @Macro
    private final String field;

    @Description("The character set encoding of the XML document. Defaults to UTF-8.")
    @Macro
    @Nullable
    private final String encoding;

    @Macro
    @Description("XPath for the output records, that points to an element in the xml document. " +
      "Every child of the xpath will be parsed into a field in the output record.")
    private final String xPath;

    @Description("Schema of the output records. Currently, only simple types are supported. " +
      "The field names must match the node names in the given xpath.")
    private final String schema;

    public Config() {
      this("", Charsets.UTF_8.name(), "", "");
    }

    public Config(String field, String encoding, String xPath, String schema) {
      this.field = field;
      this.encoding = encoding;
      this.xPath = xPath;
      this.schema = schema;
    }

    public void validate(@Nullable Schema inputSchema) {
      if (inputSchema != null && !containsMacro(field)) {
        Schema.Field parseField = inputSchema.getField(field);
        if (parseField == null) {
          throw new IllegalArgumentException(String.format("Field %s does not exist in the input schema.", field));
        }
      }

      if (!containsMacro(xPath)) {
        getXPathExpression();
      }

      for (Schema.Field field : getSchema().getFields()) {
        Schema fieldSchema = field.getSchema();
        Schema.Type type = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
        if (!type.isSimpleType()) {
          throw new IllegalArgumentException(String.format(
            "Unsupported schema. The schema can only contain simple types, but field %s is of type %s.",
            field.getName(), type));
        }
      }
    }

    public XPathExpression getXPathExpression() {
      try {
        return X_PATH_FACTORY.newXPath().compile(xPath);
      } catch (XPathExpressionException e) {
        throw new IllegalArgumentException("Could not compile xpath " + xPath);
      }
    }

    public Schema getSchema() {
      try {
        return Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Could not parse schema " + schema);
      }
    }
  }
}
