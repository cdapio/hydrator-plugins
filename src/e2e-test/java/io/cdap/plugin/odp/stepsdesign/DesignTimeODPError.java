package io.cdap.plugin.odp.stepsdesign;

import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.plugin.odp.locators.ODPLocators;
import io.cdap.plugin.odp.utils.CDAPUtils;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import stepsdesign.BeforeActions;

/**
 * DesignTimeODPError.
 */
public class DesignTimeODPError {
  static boolean errorExist = false;
  static String color;

  @Then("{string} as {string} and getting {string}")
  public void userIsAbleToSetParameterAsAndGetting(String arg0, String input, String errorMessage) {
    for (int i = 0; i < 40; i++) {
      SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-cy='" + arg0 + "' and @class='MuiInputBase-input']"))
        .sendKeys(Keys.BACK_SPACE);
    }
    ODPLocators.validateButton.click();
    color = ODPLocators.rowError.getCssValue("border-color");
    errorExist = CDAPUtils.getErrorProp(errorMessage).toLowerCase().contains(ODPLocators.rowError.getText()
                                                                               .toLowerCase());
  }

  @Then("User is able to validate the validate the error")
  public void userIsAbleToValidateTheValidateTheError() {
    Assert.assertTrue(errorExist);
  }

  @Then("User is able to set parameter {string} as {string} and getting {string} for wrong input")
  public void userIsAbleToSetParameterAsAndGettingForWrongInput(String arg0, String input, String errorMessage) {
    errorExist = false;
    for (int i = 0; i < 40; i++) {
      SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-cy='" + arg0 + "' and @class='MuiInputBase-input']"))
        .sendKeys(Keys.BACK_SPACE);
    }
    SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-cy='" + arg0 + "' and @class='MuiInputBase-input']"))
      .sendKeys(input);
    ODPLocators.validateButton.click();
    errorExist = CDAPUtils.getErrorProp(errorMessage).toLowerCase().contains(ODPLocators.jcoError.getText()
                                                                               .toLowerCase());
  }

  @Then("User is able to set parameter {string} as {string} and getting {string} for wrong input of password")
  public void userIsAbleToSetParameterAsAndGettingForWrongInputOfPassword(String arg0, String input1,
                                                                          String errorMessage) {
    errorExist = false;
    for (int i = 0; i < 40; i++) {
      SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-cy='" + arg0 + "' and @type='password']"))
        .sendKeys(Keys.BACK_SPACE);
    }
    ODPLocators.validateButton.click();
    errorExist = CDAPUtils.getErrorProp(errorMessage).toLowerCase().contains(ODPLocators.jcoError.getText()
                                                                               .toLowerCase());
    color = ODPLocators.rowError.getCssValue("border-color");
  }

  @Then("^User is able to set parameter (.+) as (.+) and getting row (.+) for wrong input$")
  public void user_is_able_to_set_parameter_as_and_getting_row_for_wrong_input(
    String option, String input, String errorMessage) {
    errorExist = false;
    for (int i = 0; i < 40; i++) {
      SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-cy='" + option + "' and @class='MuiInputBase-input']"
      )).sendKeys(Keys.BACK_SPACE);
    }
    SeleniumDriver.getDriver().findElement(By.xpath("//*[@data-cy='" + option + "' and @class='MuiInputBase-input']")).
      sendKeys(input);
    ODPLocators.validateButton.click();
    errorExist = CDAPUtils.getErrorProp(errorMessage).toLowerCase().contains(ODPLocators.rowError.getText()
                                                                               .toLowerCase());
    color = ODPLocators.rowError.getCssValue("border-color");
  }

  @Then("^User is able to set parameters filterEqualKey as (.+) and its filterEqualVal " +
    "as (.+) and getting row (.+) for wrong input$")
  public void user_is_able_to_set_parameters_filterequalkey_as_and_its_filterequalval_as_and_getting
    (String filteroption, String query, String errorMessage) throws Throwable {
    errorExist = false;
    ODPLocators.filterEqualKey.sendKeys(filteroption);
    ODPLocators.filterEqualVal.sendKeys(query);
    ODPLocators.validateButton.click();
    errorExist = ODPLocators.rowError.getText().toLowerCase().contains(CDAPUtils.getErrorProp(errorMessage)
                                                                         .toLowerCase());
    color = ODPLocators.rowError.getCssValue("border-color");
  }

  @Then("User is able to validate the text box is highlighted")
  public void userIsAbleToValidateTheTextBoxIsHighlighted() {
    BeforeActions.scenario.write("Color of the text box" + color);
    Assert.assertTrue(color.toLowerCase().contains("rgb(164, 4, 3)"));

  }


  @Then("^User is able to set parameters filterRangeKey as (.+) and its filterRangeVal " +
    "as (.+) and getting row (.+) for wrong input$")
  public void filterRangeKeyAsFilterOptionAndItsFilterRangeValAsQueryAndGettingRowErrorMessageForWrongInput
    (String filteroption, String query, String errorMessage) throws Throwable {
    ODPLocators.filterRangeKey.sendKeys(filteroption);
    ODPLocators.filterRangeVal.sendKeys(query);
    ODPLocators.validateButton.click();

  }

}
