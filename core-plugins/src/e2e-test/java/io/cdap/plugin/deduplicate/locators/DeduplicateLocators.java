/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.plugin.deduplicate.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * Deduplicate Related Locators.
 */
public class DeduplicateLocators {

  @FindBy(how = How.XPATH, using = "//*[@data-cy='filterOperation']//*[@data-cy='key']/input")
  public static WebElement deduplicateFieldName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='filterOperation']//*[@data-cy='value']")
  public static WebElement deduplicateFunctionName;

  @FindBy(how = How.XPATH, using = "(//*[contains(@class, 'metric-records-out-label')])[3]/following-sibling::span")
  public static WebElement targetRecordsCount;
}
