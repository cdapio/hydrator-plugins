package io.cdap.plugin.odp.utils;

import io.cdap.e2e.utils.SeleniumDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * CDAPUtils.
 */
public class CDAPUtils {

  private static Properties errorProp = new Properties();
  public static Properties pluginProp = new Properties();

  static {
    try {
      errorProp.load(new FileInputStream("src/e2e-test/resources/error_message.properties"));
      pluginProp.load(new FileInputStream("src/e2e-test/resources/PluginParameters.properties"));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static String getPluginProp(String property) throws IOException {
      return pluginProp.getProperty(property);
  }

  public static boolean schemaValidation() {
    boolean schemaCreated = false;
    List<WebElement> elements = SeleniumDriver.getDriver().findElements(By.xpath("//*[@placeholder='Field name']"));
    if (elements.size() > 2) {
      schemaCreated = true;
      return schemaCreated;
    }
    return false;
  }

  public static String getErrorProp(String errorMessage) {
    return errorProp.getProperty(errorMessage);
  }
}
