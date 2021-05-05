/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.batch.aggregator.function;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.MapContext;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class used for evaluating Jexl condition
 */
public class JexlCondition implements Condition, Serializable {

  private final String condition;
  private transient JexlEngine engine;
  private transient JexlScript script;

  private JexlCondition(String condition) {
    this.condition = condition;
  }

  @Override
  public boolean apply(StructuredRecord record) {
    JexlScript script = getScript();
    Set<List<String>> variables = script.getVariables();
    JexlContext context = new MapContext();

    for (List<String> variable : variables) {
      Object value = getValue(record, variable);
      context.set(variable.stream().collect(Collectors.joining(".")), value);
    }

    Object result = this.script.execute(context);

    if (result instanceof Boolean) {
      return (boolean) result;
    } else {
      throw new IllegalArgumentException("incorrect condition");
    }
  }

  /**
   * Return value of a field from a record based on provided path
   *
   * @param record {@link StructuredRecord}
   * @param path   path to a field in record
   * @return {@link Object} value of a field in record
   */
  private Object getValue(StructuredRecord record, List<String> path) {
    StructuredRecord structuredRecord = record;
    Iterator<String> iterator = path.iterator();
    Object value = null;
    Schema.Field field = null;
    while (iterator.hasNext()) {
      field = structuredRecord.getSchema().getField(iterator.next());
      if (field == null) {
        throw new IllegalArgumentException("Field provided in condition is not in input schema.");
      }
      if (field.getSchema().getType().equals(Schema.Type.RECORD)) {
        structuredRecord = structuredRecord.get(field.getName());
      } else {
        value = structuredRecord.get(field.getName());
      }
    }
    return value;
  }

  /**
   * Generates JexlScript containing condition
   *
   * @return {@link JexlScript}
   */
  private synchronized JexlScript getScript() {
    if (engine == null) {
      engine = new JexlBuilder().cache(1024).strict(true).silent(false).create();
    }
    if (script == null) {
      script = engine.createScript(condition);
    }
    return script;
  }

  /**
   * Generates the actual condition based on the given string
   *
   * @param condition string representation of Jexl condition
   * @return {@link JexlCondition}
   */
  public static JexlCondition of(String condition) {
    return new JexlCondition(condition);
  }

  /**
   * This method extracts variables from a given condition
   *
   * @param condition string representation of Jexl condition
   * @return set of lists representing full path of each variable
   */
  public static Set<List<String>> getVariables(String condition) {
    JexlEngine engine = new JexlBuilder().cache(1024).strict(true).silent(false).create();
    JexlScript script = engine.createScript(condition);

    return script.getVariables();
  }
}
