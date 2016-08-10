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

  private static final String EXIT_ON_ERROR = "Exit on error";
  private static final String WRITE_ERROR_DATASET = "Write to error dataset";
  private final Config config;
  private Schema outSchema;
  private Map<String, String> xPathMapping = new HashMap<>();

  // Required only for testing.
  public XMLParser(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    outSchema = config.getOutputSchema();
    validateXpathAndSchema();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(outSchema);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    outSchema = config.getOutputSchema();
    xPathMapping = getXPathMapping();
  }

  /**
   * Valid if xpathMappings and schema contain the same field names.
   */
  private void validateXpathAndSchema() {
    xPathMapping = getXPathMapping();
    List<Schema.Field> outFields = outSchema.getFields();
    // Checks if all the fields in the XPath mapping are present in the output schema.
    // If they are not a list of fields that are not present is included in the error message.
    StringBuilder notOutput = new StringBuilder();
    for (Schema.Field field : outFields) {
      String fieldName = field.getName();
      if (!xPathMapping.keySet().contains(field.getName())) {
        notOutput.append(fieldName + ";");
      }
    }
    if (notOutput.length() > 0) {
      throw new IllegalArgumentException("Following fields are not present in output schema :" +
                                           notOutput.toString());
    }
  }

  private Map<String, String> getXPathMapping() {
    Map<String, String> map = new HashMap<>();
    String[] xpaths = config.xPathFieldMapping.split(",");
    for (String xpath : xpaths) {
      String[] xpathmap = xpath.split(":"); //name:xpath[,name:xpath]*
      String fieldName = xpathmap[0].trim();
      if (Strings.isNullOrEmpty(fieldName)) {
        throw new IllegalArgumentException("Field name cannot be null or empty.");
      } else if (xpathmap.length < 2 || Strings.isNullOrEmpty(xpathmap[1])) {
        throw new IllegalArgumentException(String.format("XPath for field name %s cannot be null or empty.",
                                                         fieldName));
      }
      map.put(fieldName, xpathmap[1].trim());
    }
    return map;
  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) {
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
        String fieldName = field.getName();
        //To evaluate a node, the type(Nodelist or Node) should be known before hand.
        //Since, the type is not specified from user inputs, taking everything as NodeList and then evaluating.
        NodeList nodeList = (NodeList) xpath.compile(xPathMapping.get(fieldName)).evaluate(document,
                                                                                           XPathConstants.NODESET);
        if (nodeList.getLength() > 1) {
          throw new IllegalArgumentException("Cannot specify an XPath that is an array");
        }
        Node node = nodeList.item(0);
        //Since all columns have nullable schema extracting not nullable type.
        Schema.Type type = field.getSchema().getNonNullable().getType();
        String value = getValue(node, type, fieldName);
        if (value == null) {
          builder.set(fieldName, null);
        } else {
          builder.convertAndSet(fieldName, value);
        }
      }
      emitter.emit(builder.build());
    } catch (Exception e) {
      switch (config.processOnError) {
        case EXIT_ON_ERROR:
          throw new IllegalStateException("Terminating process on error: " + e.getMessage(), e);
        case WRITE_ERROR_DATASET:
          emitter.emitError(new InvalidEntry<>(31, e.getStackTrace()[0].toString() + " : " + e.getMessage(), input));
          break;
        default:
          //ignore on error(case "Ignore error and continue")
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
    } catch (TransformerException e) {
      throw new IllegalArgumentException("Cannot convert node to string. Transformer exception ", e);
    }
    return stringWriter.toString();
  }

  /**
   * Configuration for the XMLParser transform..
   */
  public static class Config extends PluginConfig {

    @Name("input")
    @Description("The field in the input record that is the source of the XML event or record.")
    private final String inputField;

    @Description("The source XML character set encoding (default UTF-8).")
    private final String encoding;

    @Name("xPathMappings")
    @Description("Mapping of the field names to the XPaths of the XML record. A comma-separated list, each element " +
      "of which is a field name, followed by a colon, followed by an XPath expression. XPath location paths can " +
      "include predicates and supports XPath 1.0. Example : <field-name>:<XPath expression>")
    private final String xPathFieldMapping;

    @Description("Mapping of field names in the output schema to data types. Consists of a comma-separated list, " +
      "each element of which is a field name followed by a colon and a type, where the field names are the same as " +
      "used in the xPathMappings, and the type is one of: boolean, int, long, float, double, bytes, or string. " +
      "Example : <field-name>:<data-type>")
    private final String fieldTypeMapping;

    @Description("The action to take in case of an error.\n" +
      "                     - \"Ignore error and continue\"\n" +
      "                     - \"Exit on error\" : Stops processing upon encountering an error\n" +
      "                     - \"Write to error dataset\" :  Writes the error record to an error dataset and continues")
    private final String processOnError;

    public Config(String inputField, String encoding, String xPathFieldMapping, String fieldTypeMapping,
                  String processOnError) {
      this.inputField = inputField;
      this.encoding = encoding;
      this.xPathFieldMapping = xPathFieldMapping;
      this.fieldTypeMapping = fieldTypeMapping;
      this.processOnError = processOnError;
    }

    /**
     * Create output schema from the field name and type value coming from keyvalue-dropdown widget.
     * Since the xpath can evaluate to null(when no node is selected), creating nullable schema for all columns.
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
          throw new IllegalArgumentException(String.format("Field %s already has type specified. Duplicate field %s",
                                                           fieldName, fieldName));
        } else {
          fields.add(field);
        }
      }
      return Schema.recordOf("record", fields);
    }
  }
}
