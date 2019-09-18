/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.batch.action;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchActionContext;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.plugin.common.batch.action.ConditionConfig;

import java.util.Properties;
import javax.annotation.Nullable;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 * Sends an email to the specified email address after an ETL Batch Application run is completed.
 * The user must specify a subject, the recipient's email address, and the sender's email address.
 * Optional properties are the host and port (defaults to localhost:25),
 * a protocol (defaults to SMTP), and a username and password.
 */
@Plugin(type = PostAction.PLUGIN_TYPE)
@Name("Email")
@Description("Sends an email after a pipeline run.")
public class EmailAction extends PostAction {
  private final Config config;

  public EmailAction(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
  }

  // some config fields are not actually nullable even though they are annotated as such
  // the annotation is only used to tell CDAP that the field is optional, but there is always a default value for it.
  @SuppressWarnings("ConstantConditions")
  @Override
  public void run(BatchActionContext context) throws Exception {
    if (!config.shouldRun(context)) {
      return;
    }
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    Authenticator authenticator = null;

    Properties javaMailProperties = new Properties();
    javaMailProperties.put("mail.smtp.host", config.host);
    javaMailProperties.put("mail.smtp.port", config.port);
    if (!(Strings.isNullOrEmpty(config.username))) {
      javaMailProperties.put("mail.smtp.auth", true);
      authenticator = new Authenticator() {
        @Override
        public PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication(config.username, config.password);
        }
      };
    }
    if ("SMTPS".equalsIgnoreCase(config.protocol)) {
      javaMailProperties.put("mail.smtp.ssl.enable", true);
    }
    if ("TLS".equalsIgnoreCase(config.protocol)) {
      javaMailProperties.put("mail.smtp.starttls.enable", true);
    }

    Session session = Session.getInstance(javaMailProperties, authenticator);
    session.setDebug(true);

    try {
      Message msg = new MimeMessage(session);
      msg.setFrom(new InternetAddress(config.sender));
      for (InternetAddress internetAddress : InternetAddress.parse(config.recipients)) {
        msg.addRecipient(Message.RecipientType.TO, internetAddress);
      }
      msg.setSubject(config.subject);
      WorkflowToken token = context.getToken();
      String message = config.includeWorkflowToken ?
        config.message + "\nUSER Workflow Tokens:\n" + token.getAll(WorkflowToken.Scope.USER)
          + "\nSYSTEM Workflow Tokens:\n" + token.getAll(WorkflowToken.Scope.SYSTEM) :
        config.message;
      msg.setText(message);

      // need this because Session will use the context classloader to instantiate an object.
      // the context classloader here is the etl application's classloader and not this class' classloader.
      ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      try {
        Transport transport = session.getTransport(config.protocol);
        transport.connect(config.host, config.port, config.username, config.password);
        try {
          transport.sendMessage(msg, msg.getAllRecipients());
        } finally {
          transport.close();
        }
      } finally {
        Thread.currentThread().setContextClassLoader(oldClassLoader);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error sending email: ", e);
    }
  }

  /**
   * Config for the email action plugin.
   */
  public static class Config extends ConditionConfig {

    // Constants for property names
    private static final String SENDER = "sender";
    private static final String RECIPIENTS = "recipients";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    @Description("Comma separated list of addresses to send the email to.")
    @Macro
    private String recipients;

    @Description("The address to send the email from.")
    @Macro
    private String sender;

    @Description("The message of the email.")
    @Macro
    private String message;

    @Description("The subject of the email.")
    @Macro
    private String subject;

    @Nullable
    @Description("The username to use for authentication if the protocol requires it.")
    @Macro
    private String username;

    @Nullable
    @Description("The password to use for authentication if the protocol requires it.")
    @Macro
    private String password;

    @Nullable
    @Description("The email protocol to use. smtp, smtps, and tls are supported. Defaults to smtp.")
    @Macro
    private String protocol;

    @Nullable
    @Description("The SMTP host to use. Defaults to localhost.")
    @Macro
    private String host;

    @Nullable
    @Description("The SMTP port to use. Defaults to 587.")
    @Macro
    private Integer port;

    @Nullable
    @Description("Whether to include the contents of the workflow token in the email message. Defaults to false.")
    @Macro
    private Boolean includeWorkflowToken;

    public Config() {
      host = "localhost";
      port = 587;
      protocol = "smtp";
      includeWorkflowToken = false;
    }

    public void validate(FailureCollector collector) {
      super.validate(collector);

      if (!containsMacro(USERNAME) && (Strings.isNullOrEmpty(username) ^ Strings.isNullOrEmpty(password))) {
        collector.addFailure("Both username and password must be given, or neither of them must be given.",
                             "Leave username and password fields empty or provide values for both fields.")
          .withConfigProperty(USERNAME).withConfigProperty(PASSWORD);
      }

      if (!containsMacro(SENDER)) {
        try {
          InternetAddress[] addresses = InternetAddress.parse(sender);
          if (addresses.length == 0) {
            collector.addFailure("Sender email was not specified.", null).withConfigProperty(SENDER);
          }
          if (addresses.length > 1) {
            collector.addFailure(
              String.format("%s is an invalid sender email address. Only one sender is supported.", sender),
              "Only specify one sender email address.").withConfigProperty(SENDER);
          }
        } catch (AddressException e) {
          collector.addFailure(String.format("%s is an invalid sender email address. Reason: %s", sender,
                                             e.getMessage()), null).withConfigProperty(SENDER);
        }
      }

      if (!containsMacro(RECIPIENTS)) {
        try {
          InternetAddress.parse(recipients);
        } catch (AddressException e) {
          collector.addFailure(String.format("%s is an invalid list of recipient email addresses. Reason: %s",
                                             recipients, e.getMessage()), null)
            .withConfigProperty(RECIPIENTS);
        }
      }
    }
  }
}
