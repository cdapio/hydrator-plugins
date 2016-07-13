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
import org.apache.avro.reflect.Nullable;

import java.util.Map;

public class PropertyMacro implements Macro {
  private final Map<String, String> substitutions;

  @VisibleForTesting
  public PropertyMacro() {
    substitutions = ImmutableMap.<String, String>builder()
      // property-specific tests
      .put("notype", "Property Macro")
      .put("{}", "brackets")
      // circular key
      .put("key", "${key}")
      // simple macro escaping
      .put("simpleEscape", "\\${\\${expansiveHostnameTree}}")
      // advanced macro escaping
      .put("advancedEscape", "${lotsOfEscaping}")
      .put("lotsOfEscaping", "\\${simpleHostnameTree\\${first}\\${filename\\${fileTypeMacro}")
      // expansive macro escaping
      .put("expansiveEscape", "${${\\${macroLiteral\\}}}\\${nothing${simplePath}")
      .put("\\${macroLiteral\\}", "match")
      .put("match", "\\{dontEvaluate:${firstPortDigit}0\\${NaN-\\${null}\\${\\${\\${nil")
      // simple hostname tree
      .put("simpleHostnameTree", "${simpleHostname}/${simplePath}:${simplePort}")
      .put("simpleHostname", "localhost")
      .put("simplePath", "index.html")
      .put("simplePort", "80")
      // advanced hostname tree
      .put("advancedHostnameTree", "${first}/${second}")
      .put("first", "localhost")
      .put("second", "${third}:${sixth}")
      .put("third", "${fourth}${fifth}")
      .put("fourth", "index")
      .put("fifth", ".html")
      .put("sixth", "80")
      // expansive hostname tree
      .put("expansiveHostnameTree", "${hostname}/${path}:${port}")
      .put("hostname", "${one}")
      .put("path", "${two}")
      .put("port", "${three}")
      .put("one", "${host${hostScopeMacro}}")
      .put("hostScopeMacro", "-local")
      .put("host-local", "${l}${o}${c}${a}${l}${hostSuffix}")
      .put("l", "l")
      .put("o", "o")
      .put("c", "c")
      .put("a", "a")
      .put("hostSuffix", "host")
      .put("two", "${filename${fileTypeMacro}}")
      .put("three", "${firstPortDigit}${secondPortDigit}")
      .put("filename", "index")
      .put("fileTypeMacro", "-html")
      .put("filename-html", "index.html")
      .put("filename-php", "index.php")
      .put("firstPortDigit", "8")
      .put("secondPortDigit", "0")
      .build();
  }

  @VisibleForTesting
  public PropertyMacro(Map<String, String> substitutions) {
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

