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
import com.google.common.base.Strings;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

/**
 * Parses XML Event using XPath.
 * This should generally be used in conjunction with the XML Reader Batch Source.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("XMLParser")
@Description("Parse XML events based on XPath")
public class XMLParser extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  private Schema outSchema;
  private Map<String, String> xPathMapping = new HashMap<>();
  private Map<String, Integer> mapErrorProcessing = new HashMap<>();

  // Required only for testing.
  public XMLParser(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getOutputSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    outSchema = config.getOutputSchema();
    validateXpathAndSchema();
    mapErrorProcessing.put("Ignore error and continue", 1);
    mapErrorProcessing.put("Exit on error", 2);
    mapErrorProcessing.put("Write to error dataset", 3);
  }

  /**
   * Valid if xpathMappings and schema contain the same field names.
   */
  private void validateXpathAndSchema() {
    String[] xpaths = config.xpathFieldMapping.split(",");
    String fieldName;
    for (String xpath : xpaths) {
      String[] xpathmap = xpath.split(":"); //name:xpath[,name:xpath]*
      fieldName = xpathmap[0].trim();
      if (Strings.isNullOrEmpty(fieldName)) {
        throw new IllegalArgumentException("Field name cannot be null or empty.");
      } else if (xpathmap.length < 2 || Strings.isNullOrEmpty(xpathmap[1])) {
        throw new IllegalArgumentException(String.format("Xpath for field name, %s cannot be null or empty.",
                                                         fieldName));
      }
      xPathMapping.put(fieldName, xpathmap[1].trim());
    }
    List<Schema.Field> outFields = outSchema.getFields();
    // Checks if all the fields in the XPath mapping are present in the output schema.
    // If they are not a list of fields that are not present is included in the error message.
    StringBuilder notOutput = new StringBuilder();
    for (Schema.Field field : outFields) {
      fieldName = field.getName();
      if (!xPathMapping.keySet().contains(field.getName())) {
        notOutput.append(fieldName + ";");
      }
    }
    if (notOutput.length() > 0) {
      throw new IllegalArgumentException("Following fields are not present in output schema :" +
                                           notOutput.toString());
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
    String fieldName;
    Node node;
    NodeList nodeList;
    try {
      InputSource source = new InputSource(new StringReader((String) input.get(config.inputField)));
      source.setEncoding(config.encoding);
      DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder documentBuilder = builderFactory.newDocumentBuilder();
      Document document = documentBuilder.parse(source);
      XPathFactory xpathFactory = XPathFactory.newInstance();
      XPath xpath = xpathFactory.newXPath();
      StructuredRecord.Builder builder = StructuredRecord.builder(outSchema);
      for (Schema.Field field : outSchema.getFields()) {
        fieldName = field.getName();
        //To evaluate a node, the type(Nodelist or Node) should be known before hand.
        //Since, the type is not specified from user inputs, taking everything as NodeList and then evaluating.
        nodeList = (NodeList) xpath.compile(xPathMapping.get(fieldName)).evaluate(document, XPathConstants.NODESET);
        if (nodeList.getLength() > 1) {
          throw new IllegalArgumentException("Cannot specify XPath that are arrays");
        }
        node = nodeList.item(0);
        //Since all columns have nullable schema extracting not nullable type.
        builder.set(fieldName, parseValues(getValue(node, field.getSchema().getNonNullable().getType(), fieldName),
                                           field));
      }
      emitter.emit(builder.build());
    } catch (Exception e) {
      switch (mapErrorProcessing.get(config.processOnError)) {
        case 2:
          throw new IllegalStateException("Terminating process in error : " + e.getMessage());
        case 3:
          emitter.emitError(new InvalidEntry<>(31, e.getStackTrace()[0].toString() + " : " + e.getMessage(), input));
          break;
        default:      //ignore on error (case 1)
          break;
      }
    }
  }

  /**
   * Get the node value to be parsed into the required format by parseValues().
   *
   * @param node      Node from which the text has to be extracted
   * @param type      schema type to check if it is a nullable string, in case the xpath evaluates to node with children
   * @param fieldName field name for which the type is to be evaluated
   * @return node value as string
   */
  private String getValue(Node node, Schema.Type type, String fieldName) {
    if (node != null) {
      Node firstChild = node.getFirstChild();
      //If the xpath evaluates to node which contains child element, the output will be an xml record
      if (firstChild != null && (firstChild.getNodeType() == Node.ELEMENT_NODE || (firstChild.getNextSibling()
        != null && (firstChild.getNextSibling().getNodeType() == Node.ELEMENT_NODE)))) {
        if (!type.equals(Schema.Type.STRING)) {
          throw new IllegalArgumentException(String.format("The xpath returned node which contains child nodes. " +
                                                             "Cannot convert %s  to type %s", fieldName, type));
        } else {
          return nodeToString(node.cloneNode(true));
        }
      } else {
        return node.getTextContent();
      }
    } else {
      return null;
    }
  }

  /**
   * Convert node to string to be returned in the output, for cases which contains child elements.
   *
   * @param node node to be converted to string
   * @return converted node as string
   */
  private String nodeToString(Node node) {
    StringWriter stringWriter = new StringWriter();
    try {
      Transformer transformer = TransformerFactory.newInstance().newTransformer();
      transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      transformer.setOutputProperty(OutputKeys.INDENT, "no");
      transformer.transform(new DOMSource(node), new StreamResult(stringWriter));
    } catch (TransformerException te) {
      System.out.println("nodeToString Transformer Exception");
    }
    return stringWriter.toString();
  }

  /**
   * Convert the node value retrieved in the type specified by user in schema.
   *
   * @param value node value to be parsed
   * @param field field type into which the node value is to be converted to
   * @return object to be passed to StructuredRecord builder
   * @throws IOException
   */
  private Object parseValues(String value, Schema.Field field) throws IOException {
    String fieldName = field.getName();
    Schema fieldSchema = field.getSchema();
    if (Strings.isNullOrEmpty(value)) {
      if (!fieldSchema.isNullable()) {
        throw new IllegalArgumentException("NULL value found for non-nullable field : " + fieldName);
      }
      return null;
    }
    Schema.Type fieldType = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
    switch (fieldType) {
      case NULL:
        return null;
      case INT:
        return Integer.parseInt(value);
      case DOUBLE:
        return Double.parseDouble(value);
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case LONG:
        return Long.parseLong(value);
      case FLOAT:
        return Float.parseFloat(value);
      case BYTES:
        return value.getBytes();
      case STRING:
        return value;
      default:
        throw new IllegalArgumentException(String.format("Unsupported schema: %s for field: \'%s\'", field.getSchema(),
                                                         field.getName()));
    }
  }

  /**
   * Configuration for the XMLParser transform..
   */
  public static class Config extends PluginConfig {

    @Name("input")
    @Description("Specifies the field in input that should be considered as source of XML event or record.")
    private final String inputField;

    @Description("Specifies  XML encoding type(default is UTF-8).")
    private final String encoding;

    @Name("xpathMappings")
    @Description("Specifies a mapping from XPath of XML record to field name.")
    private final String xpathFieldMapping;

    @Description("Specifies the field name as specified in xpathMappings and its corresponding data type." +
      "The data type can be of following types : - boolean, int, long, float, double, bytes, string.")
    private final String fieldTypeMapping;

    @Description("Specifies what happens in case of error.\n" +
      "1. Ignore the error record" +
      "2. Stop processing upon encoutering error" +
      "3. Write error record to different dataset")
    private final String processOnError;

    public Config(String inputField, String encoding, String xpathFieldMapping, String fieldTypeMapping,
                  String processOnError) {
      this.inputField = inputField;
      this.encoding = encoding;
      this.xpathFieldMapping = xpathFieldMapping;
      this.fieldTypeMapping = fieldTypeMapping;
      this.processOnError = processOnError;
    }

    /**
     * Create output schema from the field name and type value coming from keyvalue-dropdown widget.
     * Since the xpath acn evaluate to null(when no node is selected), creating nullable schema for all columns.
     *
     * @return output schema
     */
    private Schema getOutputSchema() {
      List<Schema.Field> fields = new ArrayList<>();
      String[] mappings = fieldTypeMapping.split(",");
      for (String mapping : mappings) {
        String[] params = mapping.split(":");
        String fieldName = params[0].trim();
        if (Strings.isNullOrEmpty(fieldName)) {
          throw new IllegalArgumentException("Field name cannot be null or empty.");
        } else if (params.length < 2 || Strings.isNullOrEmpty(params[1])) {
          throw new IllegalArgumentException("Type cannot be null. Please specify type for " + fieldName);
        }
        Schema.Field field = Schema.Field.of(fieldName, Schema.nullableOf(Schema.of(Schema.Type.valueOf(
          params[1].trim().toUpperCase()))));
        if (fields.contains(field)) {
          throw new IllegalArgumentException("Field " + fieldName + " already has type specified. Check the mapping.");
        } else {
          fields.add(field);
        }
      }
      return Schema.recordOf("record", fields);
    }
  }
}
