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

package io.cdap.plugin.file.actions;

import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.file.locators.CdfFileLocators;
import org.openqa.selenium.By;

import java.io.IOException;
import java.util.UUID;

/**
 * File connector related Step Actions.
 */

public class CdfFileActions {

  static {
    SeleniumHelper.getPropertiesLocators(CdfFileLocators.class);
    SeleniumHelper.getPropertiesLocators(CdfBigQueryPropertiesLocators.class);
  }

  public static void enterFileBucket(String filepath) throws IOException {
    CdfFileLocators.filePath.sendKeys(filepath);
  }
  public static void selectFile() throws InterruptedException {
    SeleniumHelper.waitElementIsVisible(CdfFileLocators.fileBucket);
    CdfFileLocators.fileBucket.click();
  }
  public static void enterSampleSize(String sampleSize) {
    CdfFileLocators.sampleSize.sendKeys(sampleSize);
  }

  public static void closeButton() {
    CdfFileLocators.closeButton.click();
  }

  public static void fileProperties() {
    CdfFileLocators.fileProperties.click();
  }

  public static void skipHeader() {
    CdfFileLocators.skipHeader.click();
  }

  public static void getSchema() {
    CdfFileLocators.getSchemaButton.click();
  }

  public static void selectFormat(String formatType) throws InterruptedException {
    CdfFileLocators.format.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(By.xpath(
      "//li[@data-value='" + formatType + "']")));
  }
  public static void enterReferenceName() {
    CdfFileLocators.referenceName.sendKeys(UUID.randomUUID().toString());
  }
  public static void enterPathField(String pathField) {
    CdfFileLocators.pathField.sendKeys(pathField);
  }

  public static void enterOverride(String override) {
    CdfFileLocators.override.sendKeys(override);
  }

  public static void clickOverrideDataType(String dataType) {
    CdfFileLocators.overrideDataType.click();
    SeleniumHelper.waitAndClick(
      SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-value='" + dataType + "']")));
  }

  public static void clickPreviewData() {
    SeleniumHelper.waitAndClick(CdfFileLocators.filePreviewData);
  }

  public static void enterDelimiterField(String delimiter) {
    CdfFileLocators.delimiterField.sendKeys(delimiter);
  }

  public static void enterRegexPath(String regexPath) {
    CdfFileLocators.regexPath.sendKeys(regexPath);
  }

  public static void enterMaxSplitSize(String maxSplitSize) {
    CdfFileLocators.minSplitSize.sendKeys(maxSplitSize);
  }

  public static void clickValidateButton() {
    CdfFileLocators.validateButton.click();
  }
}
