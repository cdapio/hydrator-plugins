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

import java.util.TimeZone;

/**
 *
 */
public class RuntimeMacroTest {

  @Test
  public void testSubstitution() {
    RuntimeMacro runtimeMacro = new RuntimeMacro(TimeZone.getTimeZone("UTC"));
    MacroContext macroContext = new DefaultMacroContext(0);

    Assert.assertEquals("1970-01-01", runtimeMacro.getValue("yyyy-MM-dd", macroContext));
    Assert.assertEquals("1969-12-31", runtimeMacro.getValue("yyyy-MM-dd,1d", macroContext));
    Assert.assertEquals("1969-12-31", runtimeMacro.getValue("yyyy-MM-dd,+1d", macroContext));
    Assert.assertEquals("1970-01-02", runtimeMacro.getValue("yyyy-MM-dd,-1d", macroContext));

    macroContext = new DefaultMacroContext(1451606400000L);
    Assert.assertEquals("2016-01-01", runtimeMacro.getValue("yyyy-MM-dd", macroContext));
    Assert.assertEquals("2015-12-31", runtimeMacro.getValue("yyyy-MM-dd,1d", macroContext));
    Assert.assertEquals("2015-12-31T00:00:00", runtimeMacro.getValue("yyyy-MM-dd'T'HH:mm:ss,1d", macroContext));
    Assert.assertEquals("2015-12-31T12:00:00", runtimeMacro.getValue("yyyy-MM-dd'T'HH:mm:ss,1d-12h", macroContext));
    Assert.assertEquals("2015-12-31T11:29:45",
                        runtimeMacro.getValue("yyyy-MM-dd'T'HH:mm:ss,1d-12h+30m+15s", macroContext));
  }

  @Test
  public void testOffset() {
    RuntimeMacro runtimeMacro = new RuntimeMacro(TimeZone.getTimeZone("UTC"));
    MacroContext macroContext = new DefaultMacroContext(0);
    Assert.assertEquals("1969-12-31T23:30:00", runtimeMacro.getValue("yyyy-MM-dd'T'HH:mm:ss,30m", macroContext));
  }

  @Test
  public void testTimeZone() {
    RuntimeMacro runtimeMacro = new RuntimeMacro(TimeZone.getTimeZone("PST"));
    MacroContext macroContext = new DefaultMacroContext(0);
    Assert.assertEquals("1969-12-31T15:30:00", runtimeMacro.getValue("yyyy-MM-dd'T'HH:mm:ss,30m, PST ", macroContext));
  }

}
