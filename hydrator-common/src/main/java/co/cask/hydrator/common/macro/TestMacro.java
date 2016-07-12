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

package co.cask.hydrator.common.macro;


import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.avro.reflect.Nullable;

import java.util.HashMap;
import java.util.Map;

public class TestMacro implements Macro {

  private final Map<String, String> substitutions;

  @VisibleForTesting
  public TestMacro() {
    substitutions = ImmutableMap.<String, String>builder()
      // circular key
      .put("key", "${test(key)}")
      // simple macro escaping
      .put("simpleEscape", "\\${test(\\${test(expansiveHostnameTree)})}")
      // advanced macro escaping
      .put("advancedEscape", "${test(lotsOfEscaping)}")
      .put("lotsOfEscaping", "\\${test(simpleHostnameTree)\\${test(first)}\\${test(filename\\${test(fileTypeMacro))}")
      // expansive macro escaping
      .put("expansiveEscape", "${test(${test(\\${test(macroLiteral\\)\\})})}\\${test(nothing)${test(simplePath)}")
      .put("\\${test(macroLiteral\\)\\}", "match")
      .put("match", "\\{test(dontEvaluate):${test(firstPortDigit)}0\\${test-\\${test(null)}\\${\\${\\${nil")
      // simple hostname tree
      .put("simpleHostnameTree", "${test(simpleHostname)}/${test(simplePath)}:${test(simplePort)}")
      .put("simpleHostname", "localhost")
      .put("simplePath", "index.html")
      .put("simplePort", "80")
      // advanced hostname tree
      .put("advancedHostnameTree", "${test(first)}/${test(second)}")
      .put("first", "localhost")
      .put("second", "${test(third)}:${test(sixth)}")
      .put("third", "${test(fourth)}${test(fifth)}")
      .put("fourth", "index")
      .put("fifth", ".html")
      .put("sixth", "80")
      // expansive hostname tree
      .put("expansiveHostnameTree", "${test(hostname)}/${test(path)}:${test(port)}")
      .put("hostname", "${test(one)}")
      .put("path", "${test(two)}")
      .put("port", "${test(three)}")
      .put("one", "${test(host${test(hostScopeMacro)})}")
      .put("hostScopeMacro", "-local")
      .put("host-local", "${test(l)}${test(o)}${test(c)}${test(a)}${test(l)}${test(hostSuffix)}")
      .put("l", "l")
      .put("o", "o")
      .put("c", "c")
      .put("a", "a")
      .put("hostSuffix", "host")
      .put("two", "${test(filename${test(fileTypeMacro)})}")
      .put("three", "${test(firstPortDigit)}${test(secondPortDigit)}")
      .put("filename", "index")
      .put("fileTypeMacro", "-html")
      .put("filename-html", "index.html")
      .put("filename-php", "index.php")
      .put("firstPortDigit", "8")
      .put("secondPortDigit", "0")
      .build();
  }

  @VisibleForTesting
  public TestMacro(Map<String, String> substitutions) {
    this.substitutions = substitutions;
  }

  @Override
  public String getValue(@Nullable String argument, MacroContext macroContext) {
    if (argument == null || argument.isEmpty()) {
      throw new IllegalArgumentException("default macros must have an argument provided.");
    }
    String substitution = substitutions.get(argument);
    if (substitution == null) {
      throw new InvalidMacroException(String.format("Macro '%s' not specified.", argument));
    }
    return substitutions.get(argument);
  }

}
