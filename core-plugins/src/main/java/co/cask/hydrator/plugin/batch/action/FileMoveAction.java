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
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;

import javax.annotation.Nullable;

/**
 * SSH into a remote machine and execute a command to pull files/data from a different machine
 * A user must specify source username and keypair authentication credentials.
 * 
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("FileMoveAction")
@Description("Action to move files between remote machines")
public class FileMoveAction extends SSHAction {
  private FileMoveActionConfig config;

  public FileMoveAction(FileMoveActionConfig config) {
    super(new SSHActionConfig(config.destHost, config.destUser, config.destPrivateKeyFile, config.destPort,
                              config.destPassword, config.createCMD(config.sourceDestinationPair, config.sourceHost,
                                                                    config.sourceUser, config.sourcePrivateKeyFile,
                                                                    config.sourcePort, config.sourcePassword,
                                                                    config.sourceFile, config.destFile), null));
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    super.run(context);
  }

  /**
   * Config class that contains all the properties needed to SSH into the destination machine, the properties needed to
   * remotely pull the file of interest, and the type of machine the action is pulling the file from and placing in
   */
  public static class FileMoveActionConfig extends PluginConfig {
    private String sourceDestinationPair;

    @Description("Host name of the remote machine where the command needs to be executed.")
    private String sourceHost;

    @Nullable
    @Description("Port to connect to. Defaults to 22")
    private Integer sourcePort;

    @Description("User name used to connect to host")
    private String sourceUser;

    @Description("File path to Private key")
    private String sourcePrivateKeyFile;

    @Nullable
    @Description("Password associated with private key")
    private String sourcePassword;

    @Description("Path of file in source machine")
    private String sourceFile;

    @Description("Host name of the remote machine where the command needs to be executed.")
    private String destHost;

    @Nullable
    @Description("Port to connect to. Defaults to 22")
    private Integer destPort;

    @Description("User name used to connect to host")
    private String destUser;

    @Description("File path to Private key")
    private String destPrivateKeyFile;

    @Nullable
    @Description("Password associated with private key")
    private String destPassword;

    private String destFile;



    FileMoveActionConfig(String destHost, String destUser, String destPrivateKeyFile, int destPort, String destPassword,
                         String sourceHost, String sourceUser, String sourcePrivateKeyFile, int sourcePort,
                         String sourcePassword, String sourceDestinationPair, String sourceFile, String destFile) {

      //always will SSH into destination machine and pull file from source machine
      this.sourceDestinationPair = sourceDestinationPair;
      this.sourceHost = sourceHost;
      this.sourceUser = sourceUser;
      this.sourcePrivateKeyFile = sourcePrivateKeyFile;
      this.sourcePassword = sourcePassword;
      this.sourceFile = sourceFile;
      this.destHost = destHost;
      this.destPort = destPort;
      this.destUser = destUser;
      this.destPrivateKeyFile = destPrivateKeyFile;
      this.destPassword = destPassword;
      this.destFile = destFile;

//      super(destHost, destUser, destPrivateKeyFile, destPort, destPassword, null, null);
//      String cmd =  createCMD(sourceDestinationPair, sourceHost, sourceUser, sourcePrivateKeyFile,
//                              sourcePort, sourcePassword, sourceFile, destFile);
//      super.setCMD(cmd);

    }

    private String createFTPGetCMD(String sourceHost, String sourceUser,
                                   String sourcePrivateKeyFile, int sourcePort,
                                   String sourcePassword, String sourceFile) {
      StringBuilder ftpCMD = new StringBuilder(50);
      ftpCMD.append("wgets ftp://");
      ftpCMD.append(sourceUser);
      ftpCMD.append(":");
      ftpCMD.append(sourcePassword);
      ftpCMD.append("@");
      ftpCMD.append(sourceHost);
      ftpCMD.append(sourceFile); //must had slash in front of it
      return ftpCMD.toString();

    }

    private String createCMD(String sourceDestinationPair, String sourceHost, String sourceUser,
                             String sourcePrivateKeyFile, int sourcePort,
                             String sourcePassword, String sourceFile, String destFile) {
      /**
       * Source-Destination pairing could be: FTP->HDFS  FTP->Unix HDFS->HDFS  Unix->Unix  Unix-HDFS
      */
      switch (sourceDestinationPair) {
        case "FTP->HDFS":
            //SSH into Hadoop machine, call FTP get command, move file to hdfs, and then delete local file
            //Template command:
            //echo wgets ftp://user:pass@hostname/source/File | hadoop fs -put - /hdfs/destination/File
          // Default size if 16 characters, which would require multiple re-allocations
          String ftpCMD = createFTPGetCMD(sourceHost, sourceUser, sourcePrivateKeyFile,
                                             sourcePort, sourcePassword, sourceFile);
          
          StringBuilder hdfsCMD = new StringBuilder(50);
          hdfsCMD.append("hadoop fs -put - "); //piping it through stdin
          hdfsCMD.append(destFile);

          StringBuilder ftpHdfsCMD = new StringBuilder(100);
          ftpHdfsCMD.append("echo ");
          ftpHdfsCMD.append(ftpCMD);
          ftpHdfsCMD.append(" | ");
          ftpHdfsCMD.append(hdfsCMD);
          
          return ftpHdfsCMD.toString();
        case "FTP->Unix":
            //SSH into Unix machine, call FTP get command, and pipe the file to the proper location
            //Template Command:
            //echo wgets ftp://user:pass@hostname/source/File | mv - /destination/File
          String ftpSourceCMD = createFTPGetCMD(sourceHost, sourceUser, sourcePrivateKeyFile,
                                             sourcePort, sourcePassword, sourceFile);
          StringBuilder ftpUnixCMD = new StringBuilder(45);
          ftpUnixCMD.append("echo ");
          ftpUnixCMD.append(ftpSourceCMD);
          ftpUnixCMD.append(" | ");
          ftpUnixCMD.append("mv - ");
          ftpUnixCMD.append(destFile);

          return ftpUnixCMD.toString();
        case "HDFS->HDFS":
            //SSH into hadoop machine, do hdfs move command
            //Template Command:
            //hadoop fs -mv /source/File /destination/File
          StringBuilder hdfsMoveCMD = new StringBuilder(50);
          hdfsMoveCMD.append("hadoop fs -mv ");
          hdfsMoveCMD.append(sourceFile);
          hdfsMoveCMD.append(" ");
          hdfsMoveCMD.append(destFile);

          return hdfsMoveCMD.toString();
        case "Unix->Unix":
            //SSH into destination Unix, scp call to retrieve file from source machine
            //Template Command:
            //scp sourceUsr@sourceHost:/source/File /destination/file
          StringBuilder scpCMD = new StringBuilder(50);
          scpCMD.append("scp ");
          scpCMD.append(sourceUser);
          scpCMD.append("@");
          scpCMD.append(sourceHost);
          scpCMD.append(":");
          scpCMD.append(sourceFile);
          scpCMD.append(" ");
          scpCMD.append(destFile);

          return scpCMD.toString();
        case "Unix-HDFS":
            //SSH into hadoop machine, do scp call to retrieve file from source machine, and then move file to hdfs
            //Template Command:
            //scp sourceUsr@sourceHost:/ && hadoop fs -put / /hdfs/destination/File
          StringBuilder scpHdfsCMD = new StringBuilder(50);
          scpHdfsCMD.append("scp ");
          scpHdfsCMD.append(sourceUser);
          scpHdfsCMD.append("@");
          scpHdfsCMD.append(sourceHost);
          scpHdfsCMD.append(":");
          scpHdfsCMD.append(sourceFile);
          scpHdfsCMD.append(" /");
          scpHdfsCMD.append(" && hadoop fs -put / ");
          scpHdfsCMD.append(destFile);
          return scpHdfsCMD.toString();
        default:
          return "";
      }
    }
  }
}
