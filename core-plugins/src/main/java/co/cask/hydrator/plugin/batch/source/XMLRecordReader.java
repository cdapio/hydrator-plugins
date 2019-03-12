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

package co.cask.hydrator.plugin.batch.source;

import com.google.common.base.Strings;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * XMLRecordReader class to read through a given xml document and to output xml blocks as per node path specified.
 */
public class XMLRecordReader extends RecordReader<LongWritable, Map<String, String>> {
  private static final Logger LOG = LoggerFactory.getLogger(XMLRecordReader.class);

  public static final String CLOSING_END_TAG_DELIMITER = ">";
  public static final String OPENING_END_TAG_DELIMITER = "</";
  public static final String CLOSING_START_TAG_DELIMITER = ">";
  public static final String OPENING_START_TAG_DELIMITER = "<";

  private String fileName;
  private XMLStreamReader reader;
  private String[] nodes;
  private Map<Integer, String> currentNodeLevelMap;
  private String tempFilePath;
  private Path file;
  private String fileAction;
  private FileSystem fs;
  private String targetFolder;
  private long availableBytes;
  private FSDataInputStream fdDataInputStream;
  private final DecimalFormat df = new DecimalFormat("#.##");

  private LongWritable currentKey;
  private Map<String, String> currentValue;
  private int nodeLevel = 0;

  @Override
  public void close() throws IOException {
    if (reader != null) {
      try {
        reader.close();
        fdDataInputStream.close();
      } catch (XMLStreamException exception) {
        LOG.error("Error occurred while closing reader : " +  exception.getMessage());
      }
    }
  }

  @Override
  public float getProgress() throws IOException {
    float progress = (float) fdDataInputStream.getPos() /  availableBytes;
    return Float.valueOf(df.format(progress));
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return currentKey;
  }

  @Override
  public Map<String, String> getCurrentValue() throws IOException, InterruptedException {
    return currentValue;
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    FileSplit fileSplit  = (FileSplit) split;
    file = fileSplit.getPath();
    fileName = file.toUri().toString();
    Configuration conf = context.getConfiguration();
    fs = file.getFileSystem(conf);
    XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.FALSE);
    fdDataInputStream = fs.open(file);
    availableBytes = split.getLength();
    try {
      reader = factory.createXMLStreamReader(fdDataInputStream);
    } catch (XMLStreamException exception) {
      throw new RuntimeException("XMLStreamException exception : ", exception);
    }
    //Set required node path details.
    String nodePath = conf.get(XMLInputFormat.XML_INPUTFORMAT_NODE_PATH);
    //Remove preceding '/' in node path to avoid first unwanted element after split('/')
    if (nodePath.indexOf("/") == 0) {
      nodePath = nodePath.substring(1, nodePath.length());
    }
    nodes = nodePath.split("/");

    currentNodeLevelMap = new HashMap<>();
    tempFilePath = conf.get(XMLInputFormat.XML_INPUTFORMAT_PROCESSED_DATA_TEMP_FOLDER);
    fileAction = conf.get(XMLInputFormat.XML_INPUTFORMAT_FILE_ACTION);
    targetFolder = conf.get(XMLInputFormat.XML_INPUTFORMAT_TARGET_FOLDER);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    currentKey = new LongWritable();
    currentValue = new HashMap<>();
    String lastNode = nodes[nodes.length - 1];
    StringBuilder xmlRecord = new StringBuilder();

    //Flag to know if xml record matching to node path has been read and ready to use.
    boolean xmlRecordReady = false;
    //Flag to know if matching node found as per node path
    boolean nodeFound = false;
    try {
      while (reader.hasNext()) {
        int event = reader.next();
        switch (event) {
          case XMLStreamConstants.START_ELEMENT:
            String nodeNameStart = reader.getLocalName();
            boolean validHierarchy = true;
            currentNodeLevelMap.put(nodeLevel, nodeNameStart);

            //Check if node hierarchy matches with expected one.
            for (int j = nodeLevel; j >= 0; j--) {
              if (j < nodes.length && !currentNodeLevelMap.get(j).equals(nodes[j])) {
                validHierarchy = false;
              }
            }
            //Check if node hierarchy is valid and it matches with last node in node path
            //then start appending tag information to create valid XML Record.
            if (validHierarchy && nodeNameStart.equals(lastNode)) {
              appendStartTagInformation(nodeNameStart, xmlRecord);
              //Set flag for valid node path found
              nodeFound = true;
              //Set file offset
              currentKey.set(reader.getLocation().getLineNumber());
            } else if (nodeFound) {
              //Append all child nodes inside valid node path
              appendStartTagInformation(nodeNameStart, xmlRecord);
            }
            nodeLevel++;
            break;
          case XMLStreamConstants.CHARACTERS:
            if (nodeFound) {
              xmlRecord.append(StringEscapeUtils.escapeXml(reader.getText()));
            }
            break;
          case XMLStreamConstants.END_ELEMENT:
            String nodeNameEnd = reader.getLocalName();
            if (nodeFound) {
              //Add closing tag
              xmlRecord.append(OPENING_END_TAG_DELIMITER).append(nodeNameEnd).append(CLOSING_END_TAG_DELIMITER);
              if (nodeNameEnd.equals(lastNode)) {
                nodeFound = false;
                //Set flag for XML record is ready to emit.
                xmlRecordReady = true;
              }
            }
            nodeLevel--;
            break;
          case XMLStreamConstants.START_DOCUMENT:
            break;
        }
        if (xmlRecordReady) {
          currentValue.put(fileName, xmlRecord.toString());
          return true;
        }
      }
    } catch (XMLStreamException exception) {
      throw new IllegalArgumentException(exception);
    }
    if (!Strings.isNullOrEmpty(tempFilePath)) {
      updateFileTrackingInfo();
    }
    processFileAction();
    return false;
  }

  /**
   * Method to append start tag information along with attributes if any
   */
  private void appendStartTagInformation(String nodeNameStart, StringBuilder xmlRecord) {
    xmlRecord.append(OPENING_START_TAG_DELIMITER).append(nodeNameStart);
    int count = reader.getAttributeCount();
    for (int i = 0; i < count; i++) {
      xmlRecord.append(" ")
        .append(StringEscapeUtils.escapeXml(reader.getAttributeLocalName(i)))
        .append("=\"")
        .append(StringEscapeUtils.escapeXml(reader.getAttributeValue(i)))
        .append("\"");
    }
    xmlRecord.append(CLOSING_START_TAG_DELIMITER);
  }

  /**
   * Method to process file with actions (Delete, Move, Archive ) specified.
   * @throws IOException - IO Exception occurred while deleting, moving or archiving file.
   */
  private void processFileAction() throws IOException {
    switch (fileAction.toLowerCase()) {
      case "delete":
        fs.delete(file, true);
        break;
      case "move":
        Path targetFileMovePath = new Path(targetFolder, file.getName());
        fs.rename(file, targetFileMovePath);
        break;
      case "archive":
        try (FSDataOutputStream archivedStream = fs.create(new Path(targetFolder, file.getName() + ".zip"));
             ZipOutputStream zipArchivedStream = new ZipOutputStream(archivedStream);
             FSDataInputStream fdDataInputStream = fs.open(file)) {
          zipArchivedStream.putNextEntry(new ZipEntry(file.getName()));
          int length;
          byte[] buffer = new byte[1024];
          while ((length = fdDataInputStream.read(buffer)) > 0) {
            zipArchivedStream.write(buffer, 0, length);
          }
          zipArchivedStream.closeEntry();
        }
        fs.delete(file, true);
        break;
      default:
        break;
    }
  }

  /**
   * Method to update temporary file with latest XML processed information.
   * @throws IOException - IO Exception occurred while writing data to temp file.
   */
  private void updateFileTrackingInfo() throws IOException {
    try (FSDataOutputStream outputStream = fs.create(new Path(tempFilePath, file.getName() + ".txt"))) {
      outputStream.writeUTF(fileName);
    }
  }
}
