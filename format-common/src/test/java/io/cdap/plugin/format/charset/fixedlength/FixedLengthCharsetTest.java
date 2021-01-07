/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.format.charset.fixedlength;

import io.cdap.plugin.format.plugin.AbstractFileSourceConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FixedLengthCharsetTest {

  @Test
  public void testEncodingsAreValid() {
    List<String> encodings = Arrays.asList(
      "UTF-32",
      "ISO-8859-1 (Latin-1 Western European)",
      "ISO-8859-2 (Latin-2 Central European)",
      "ISO-8859-3 (Latin-3 South European)",
      "ISO-8859-4 (Latin-4 North European)",
      "ISO-8859-5 (Latin/Cyrillic)",
      "ISO-8859-6 (Latin/Arabic)",
      "ISO-8859-7 (Latin/Greek)",
      "ISO-8859-8 (Latin/Hebrew)",
      "ISO-8859-9 (Latin-5 Turkish)",
      "ISO-8859-11 (Latin/Thai)",
      "ISO-8859-13 (Latin-7 Baltic Rim)",
      "ISO-8859-15 (Latin-9)",
      "Windows-1250",
      "Windows-1251",
      "Windows-1252",
      "Windows-1253",
      "Windows-1254",
      "Windows-1255",
      "Windows-1256",
      "Windows-1257",
      "Windows-1258",
      "IBM00858",
      "IBM01140",
      "IBM01141",
      "IBM01142",
      "IBM01143",
      "IBM01144",
      "IBM01145",
      "IBM01146",
      "IBM01147",
      "IBM01148",
      "IBM01149",
      "IBM037",
      "IBM1026",
      "IBM1047",
      "IBM273",
      "IBM277",
      "IBM278",
      "IBM280",
      "IBM284",
      "IBM285",
      "IBM290",
      "IBM297",
      "IBM420",
      "IBM424",
      "IBM437",
      "IBM500",
      "IBM775",
      "IBM850",
      "IBM852",
      "IBM855",
      "IBM857",
      "IBM860",
      "IBM861",
      "IBM862",
      "IBM863",
      "IBM864",
      "IBM865",
      "IBM866",
      "IBM868",
      "IBM869",
      "IBM870",
      "IBM871",
      "IBM918"
    );

    for (String encoding : encodings) {
      Assert.assertTrue(FixedLengthCharset.isValidEncoding(FixedLengthCharset.cleanFileEncodingName(encoding)));
    }

    Assert.assertFalse(FixedLengthCharset.isValidEncoding("utf-8"));
  }
}
