/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin;

import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathFactoryConfigurationException;

/**
 * Utility methods for secure XML parsing and validation of legacy XML security properties.
 */
final class XmlSecurityUtils {
  private static final Logger LOG = LoggerFactory.getLogger(XmlSecurityUtils.class);

  static final String ENABLE_EXTERNAL_GENERAL_ENTITIES = "enableExternalGeneralEntities";
  static final String ENABLE_EXTERNAL_PARAMETER_ENTITIES = "enableExternalParameterEntities";
  static final String LOAD_EXTERNAL_DTD = "loadExternalDTD";
  static final String DISALLOW_DOCTYPE_DTD = "disallowDocTypeDTD";

  private static final List<String> DISALLOWED_TRUE_PROPERTIES = Arrays.asList(
    ENABLE_EXTERNAL_GENERAL_ENTITIES,
    ENABLE_EXTERNAL_PARAMETER_ENTITIES,
    LOAD_EXTERNAL_DTD
  );

  private XmlSecurityUtils() {
  }

  /**
   * Creates a {@link DocumentBuilderFactory} hardened against XXE, DTD, and XInclude attacks.
   */
  static DocumentBuilderFactory createSecureDocumentBuilderFactory() throws ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
    factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
    factory.setXIncludeAware(false);
    factory.setExpandEntityReferences(false);
    setAttributeIfSupported(factory, XMLConstants.ACCESS_EXTERNAL_DTD, "");
    setAttributeIfSupported(factory, XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
    return factory;
  }

  /**
   * Creates a {@link TransformerFactory} hardened against external DTD and stylesheet access.
   */
  static TransformerFactory createSecureTransformerFactory() throws TransformerConfigurationException {
    TransformerFactory factory = TransformerFactory.newInstance();
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    setAttributeIfSupported(factory, XMLConstants.ACCESS_EXTERNAL_DTD, "");
    setAttributeIfSupported(factory, XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
    return factory;
  }

  /**
   * Creates an {@link XPathFactory} with secure processing enabled.
   */
  static XPathFactory createSecureXPathFactory() throws XPathFactoryConfigurationException {
    XPathFactory factory = XPathFactory.newInstance();
    factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
    return factory;
  }

  /**
   * Rejects legacy XML parser properties that attempt to enable DTDs or external entity resolution.
   * Safe values (or omitted properties) remain accepted for backward compatibility with existing pipelines.
   */
  static void validateLegacyXmlSecurityProperties(PluginConfig config, FailureCollector collector) {
    for (String propertyName : DISALLOWED_TRUE_PROPERTIES) {
      if (hasUnsafeBooleanProperty(config, propertyName, false)) {
        collector.addFailure(
          String.format("Property '%s' cannot be enabled because external entities and DTD processing are disabled.",
                        propertyName),
          String.format("Remove '%s' or set it to 'false'.", propertyName))
          .withConfigProperty(propertyName);
      }
    }

    if (hasUnsafeBooleanProperty(config, DISALLOW_DOCTYPE_DTD, true)) {
      collector.addFailure(
        String.format("Property '%s' cannot be disabled because DTD processing is disallowed.",
                      DISALLOW_DOCTYPE_DTD),
        String.format("Remove '%s' or set it to 'true'.", DISALLOW_DOCTYPE_DTD))
        .withConfigProperty(DISALLOW_DOCTYPE_DTD);
    }
  }

  private static boolean hasUnsafeBooleanProperty(PluginConfig config, String propertyName,
                                                  boolean requiredSafeValue) {
    String rawValue = getFromPluginProperties(config.getRawProperties(), propertyName);
    String resolvedValue = getFromPluginProperties(config.getProperties(), propertyName);
    return isUnsafeBooleanProperty(rawValue, requiredSafeValue)
      || isUnsafeBooleanProperty(resolvedValue, requiredSafeValue);
  }

  @Nullable
  private static String getFromPluginProperties(@Nullable PluginProperties pluginProperties, String propertyName) {
    if (pluginProperties == null) {
      return null;
    }
    Map<String, String> properties = pluginProperties.getProperties();
    return properties == null ? null : properties.get(propertyName);
  }

  private static boolean isUnsafeBooleanProperty(@Nullable String value, boolean requiredSafeValue) {
    if (value == null) {
      return false;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty()) {
      return false;
    }
    return !Boolean.toString(requiredSafeValue).equalsIgnoreCase(trimmed);
  }

  private static void setAttributeIfSupported(DocumentBuilderFactory factory, String attribute, Object value) {
    try {
      factory.setAttribute(attribute, value);
    } catch (IllegalArgumentException e) {
      LOG.debug("DocumentBuilderFactory {} does not recognize attribute {}", factory, attribute, e);
    }
  }

  private static void setAttributeIfSupported(TransformerFactory factory, String attribute, Object value) {
    try {
      factory.setAttribute(attribute, value);
    } catch (IllegalArgumentException e) {
      LOG.debug("TransformerFactory {} does not recognize attribute {}", factory, attribute, e);
    }
  }
}
