package io.cdap.plugin.odp.tests.runneroptional.optional;

import cucumber.api.event.EventListener;
import cucumber.api.event.EventPublisher;
import io.cdap.plugin.odp.utils.CDAPUtils;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Event listener to update properties on the run, called as a plugin by TestRunner.
 */
public class PropModifier implements EventListener {
  public PropModifier(String fileRelativePath) {
    //called in the beginning, before any scenarios are executed
    appendToProps(fileRelativePath);
  }

  private void appendToProps(String fileRelativePath) {
    System.out.println("=========================================================" + fileRelativePath);
    System.out.println(CDAPUtils.pluginProp.entrySet().size());
    try {
      CDAPUtils.pluginProp.load(new FileInputStream(fileRelativePath));
    } catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println(CDAPUtils.pluginProp.entrySet().size());
  }

  @Override
  public void setEventPublisher(EventPublisher eventPublisher) {
    //called after all tagged scenarios are completed, for eg: reporting. As of now do nothing
  }
}
