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

import org.junit.Assert;
import org.junit.Test;

/**
 */
public class MacroConfigTest {
  private static final String CONSTANT = "${runtime:yyyy}";

  @Test
  public void testNoOp() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);
    String value = "abc123";
    TestConfig testConfig = new TestConfig(value);
    testConfig.substituteMacros(macroContext);

    Assert.assertEquals(value, testConfig.stringField);
    Assert.assertEquals(CONSTANT, TestConfig.CONSTANT);
  }

  @Test
  public void testSubstitution() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("${runtime(yyyy-MM-dd'T'HH:mm:ss,0s,UTC)}");
    testConfig.substituteMacros(macroContext);
    Assert.assertEquals("1970-01-01T00:00:00", testConfig.stringField);

    testConfig = new TestConfig("${runtime(yyyy,0s,UTC)}${runtime(MM,0s,UTC)}");
    testConfig.substituteMacros(macroContext);
    Assert.assertEquals("197001", testConfig.stringField);

    testConfig = new TestConfig("abc-${runtime(yyyy,0s,UTC)}-123");
    testConfig.substituteMacros(macroContext);
    Assert.assertEquals("abc-1970-123", testConfig.stringField);

    testConfig = new TestConfig("{${runtime(yyyy,0s,UTC)}}}{}$$");
    testConfig.substituteMacros(macroContext);
    Assert.assertEquals("{1970}}{}$$", testConfig.stringField);
  }

  @Test(expected = InvalidMacroException.class)
  public void testUnenclosedMacro() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("${runtime(yyyy-MM-dd'T'HH:mm:ss)");
    testConfig.substituteMacros(macroContext);
    Assert.assertEquals("${runtime(yyyy-MM-dd'T'HH:mm:ss)", testConfig.stringField);
  }

  @Test(expected = InvalidMacroException.class)
  public void testInvalidPattern() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("${runtime(asdf)}");
    testConfig.substituteMacros(macroContext);
  }

  @Test
  public void testNullValueOK() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig(null);
    testConfig.substituteMacros(macroContext);
    Assert.assertNull(testConfig.stringField);
  }

  @Test
  public void testFieldSelection() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("${runtime(yyyy-MM-dd'T'HH:mm:ss,0m,UTC)}");
    testConfig.substituteMacros(macroContext, "nonexistant");
    Assert.assertEquals("${runtime(yyyy-MM-dd'T'HH:mm:ss,0m,UTC)}", testConfig.stringField);
    testConfig.substituteMacros(macroContext, "stringField");
    Assert.assertEquals("1970-01-01T00:00:00", testConfig.stringField);
  }

  // 'test' macroFunction tests

  @Test
  public void testNoMacroType() {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("${notype}");
    testConfig.substituteMacros(macroContext, "stringField");
  }

  @Test(expected = InvalidMacroException.class)
  public void testNonexistentMacro() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("${test(invalid)}");
    testConfig.substituteMacros(macroContext, "stringField");
  }

  @Test(expected = InvalidMacroException.class)
  public void testCircularMacro() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("${test(key)}");
    testConfig.substituteMacros(macroContext, "stringField");
  }

  @Test
  public void testSimpleMacroSyntaxEscaping() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("${test(simpleEscape)}");
    testConfig.substituteMacros(macroContext, "stringField");
    Assert.assertEquals("${test(${test(expansiveHostnameTree)})}", testConfig.stringField);
  }

  @Test
  public void testAdvancedMacroSyntaxEscaping() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("${test(advancedEscape)}");
    testConfig.substituteMacros(macroContext, "stringField");
    Assert.assertEquals("${test(simpleHostnameTree)${test(first)}${test(filename${test(fileTypeMacro))}",
                        testConfig.stringField);
  }

  @Test
  public void testExpansiveSyntaxEscaping() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("${test(expansiveEscape)}");
    testConfig.substituteMacros(macroContext, "stringField");
    Assert.assertEquals("{test(dontEvaluate):80${test-${test(null)}${${${nil${test(nothing)index.html",
                        testConfig.stringField);
  }

  @Test
  public void testSimpleMacroTree() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("${test(simpleHostnameTree)}");
    testConfig.substituteMacros(macroContext, "stringField");
    Assert.assertEquals("localhost/index.html:80", testConfig.stringField);
  }

  @Test
  public void testAdvancedMacroTree() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("${test(advancedHostnameTree)}");
    testConfig.substituteMacros(macroContext, "stringField");
    Assert.assertEquals("localhost/index.html:80", testConfig.stringField);
  }

  @Test
  public void testExpansiveMacroTree() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("${test(expansiveHostnameTree)}");
    testConfig.substituteMacros(macroContext, "stringField");
    Assert.assertEquals("localhost/index.html:80", testConfig.stringField);
  }

  // 'property' macroFunction tests

  @Test
  public void propertyBracketEscapingTest() throws InvalidMacroException {
    MacroContext macroContext = new DefaultMacroContext(0);

    TestConfig testConfig = new TestConfig("$${\\{\\}}");
    testConfig.substituteMacros(macroContext, "stringField");
    Assert.assertEquals("$brackets", testConfig.stringField);
  }



  // unused fields are ok, they are there just to make sure we don't choke on them while reflecting.
  @SuppressWarnings("unused")
  private static class TestConfig extends MacroConfig {
    // shouldn't get substituted
    private static final String CONSTANT = MacroConfigTest.CONSTANT;

    // shouldn't break substitution
    private Integer intField;
    private Long longField;
    private Double doubleField;
    private Float floatField;
    private Boolean boolField;

    // should get substituted
    private String stringField;

    public TestConfig(String stringField) {
      super();
      this.stringField = stringField;
    }
  }
}
