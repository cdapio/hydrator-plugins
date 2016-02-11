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

package co.cask.hydrator.plugin.batch.action;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.api.batch.PostAction;
import co.cask.hydrator.common.MacroConfig;
import com.google.common.base.Strings;

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
@Description("Sends an email to a specific email address after the pipeline run.")
public class EmailAction extends PostAction {
  private final Config config;

  /**
   * Config for the email action plugin.
   */
  public static class Config extends MacroConfig {
    private String recipientEmailAddress;

    private String senderEmailAddress;

    private String subject;

    private String message;

    @Nullable
    private String username;

    @Nullable
    private String password;

    @Nullable
    private String protocol;

    @Nullable
    private String host;

    @Nullable
    private Integer port;

    @Nullable
    private Boolean runOnFailure;

    @Nullable
    private Boolean runOnSuccess;

    public Config() {
      host = "localhost";
      port = 25;
      protocol = "smtp";
      runOnFailure = true;
      runOnSuccess = false;
    }
  }

  public EmailAction(Config config) {
    this.config = config;
  }

  @Override
  public void run(boolean succeeded, BatchContext context) throws Exception {

    if (config.runOnSuccess != succeeded && config.runOnFailure == succeeded) {
      return;
    }
    config.substituteMacros(context.getLogicalStartTime());

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
    Message msg = new MimeMessage(session);
    msg.setFrom(new InternetAddress(config.senderEmailAddress));
    msg.addRecipient(Message.RecipientType.TO, new InternetAddress(config.recipientEmailAddress));
    msg.setSubject(config.subject);
    msg.setText(config.message);

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
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    if (Strings.isNullOrEmpty(config.username) ^ Strings.isNullOrEmpty(config.password)) {
      throw new IllegalArgumentException(
        String.format("You must either set both username and password " +
                        "or neither username nor password. " +
                        "Currently, they are username: \'%s\', and password: \'%s\'",
                      config.username, config.password));
    }

    try {
      new InternetAddress(config.senderEmailAddress);
    } catch (AddressException e) {
      throw new IllegalArgumentException(
        String.format("%s is an invalid sender email address. Reason: %s",
                      config.senderEmailAddress, e.getMessage()));
    }

    try {
      new InternetAddress(config.recipientEmailAddress);
    } catch (AddressException e) {
      throw new IllegalArgumentException(
        String.format("%s is an invalid recipient email address. Reason: %s",
                      config.recipientEmailAddress, e.getMessage()));
    }
  }
}
