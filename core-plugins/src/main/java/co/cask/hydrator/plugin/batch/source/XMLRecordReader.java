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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
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

  private final String fileName;
  private final XMLStreamReader reader;
  private final String[] nodes;
  private final Map<Integer, String> currentNodeLevelMap;
  private final String tempFilePath;
  private final Path file;
  private final FileSystem fs;
  private final long availableBytes;
  private final TrackingInputStream inputStream;
  private final DecimalFormat df = new DecimalFormat("#.##");

  private LongWritable currentKey;
  private Map<String, String> currentValue;
  private int nodeLevel = 0;

  public XMLRecordReader(FileSplit split, Configuration conf) throws IOException {
    file = split.getPath();
    fileName = file.toUri().toString();
    fs = file.getFileSystem(conf);
    XMLInputFactory factory = XMLInputFactory.newInstance();
    FSDataInputStream fdDataInputStream = fs.open(file);
    inputStream = new TrackingInputStream(fdDataInputStream);
    availableBytes = inputStream.available();
    try {
      reader = factory.createXMLStreamReader(inputStream);
    } catch (XMLStreamException exception) {
      throw new RuntimeException("XMLStreamException exception : ", exception);
    }
    //set required node path details.
    String nodePath = conf.get(XMLInputFormat.XML_INPUTFORMAT_NODE_PATH);
    //remove preceding '/' in node path to avoid first unwanted element after split('/')
    if (nodePath.indexOf("/") == 0) {
      nodePath = nodePath.substring(1, nodePath.length());
    }
    nodes = nodePath.split("/");

    currentNodeLevelMap = new HashMap<Integer, String>();

    tempFilePath = conf.get(XMLInputFormat.XML_INPUTFORMAT_PROCESSED_DATA_TEMP_FOLDER);
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      try {
        reader.close();
      } catch (XMLStreamException exception) {
        LOG.error("Error occurred while closing reader : " +  exception.getMessage());
      }
    }
  }

  @Override
  public float getProgress() throws IOException {
    float progress = (float) inputStream.getTotalBytes() /  availableBytes;
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
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    currentKey = new LongWritable();
    currentValue = new HashMap<String, String>();
    String lastNode = nodes[nodes.length - 1];
    StringBuilder xmlRecord = new StringBuilder();

    //flag to know if xml record matching to node path has been read and ready to use.
    boolean xmlRecordReady = false;
    //flag to know if matching node found as per node path
    boolean nodeFound = false;

    try {
      while (reader.hasNext()) {
        int event = reader.next();
        switch (event) {
          case XMLStreamConstants.START_ELEMENT:
            String nodeNameStart = reader.getLocalName();
            boolean validHierarchy = true;
            currentNodeLevelMap.put(nodeLevel, nodeNameStart);

            //check if node hierarchy matches with expected one.
            for (int j = nodeLevel; j >= 0; j--) {
              if (j < nodes.length && !currentNodeLevelMap.get(j).equals(nodes[j])) {
                validHierarchy = false;
              }
            }
            //check if node hierarchy is valid and it matches with last node in node path
            //then start appending tag information to create valid XML Record.
            if (validHierarchy && nodeNameStart.equals(lastNode)) {
              appendStartTagInformation(nodeNameStart, xmlRecord);
              //Set flag for valid node path found
              nodeFound = true;
              //set file offset
              currentKey.set(reader.getLocation().getLineNumber());
            } else if (nodeFound) {
              //append all child nodes inside valid node path
              appendStartTagInformation(nodeNameStart, xmlRecord);
            }
            nodeLevel++;
            break;
          case XMLStreamConstants.CHARACTERS:
            if (nodeFound) {
              xmlRecord.append(reader.getText());
            }
            break;
          case XMLStreamConstants.END_ELEMENT:
            String nodeNameEnd = reader.getLocalName();
            if (nodeFound) {
              //add closing tag
              xmlRecord.append(OPENING_END_TAG_DELIMITER).append(nodeNameEnd).append(CLOSING_END_TAG_DELIMITER);
              if (nodeNameEnd.equals(lastNode)) {
                nodeFound = false;
                //set flag for XML record is ready to emit.
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
    updateFileTrackingInfo();
    return false;
  }

  /**
   * Method to append start tag information along with attributes if any
   */
  private void appendStartTagInformation(String nodeNameStart, StringBuilder xmlRecord) {
    xmlRecord.append(OPENING_START_TAG_DELIMITER).append(nodeNameStart);
    int count = reader.getAttributeCount();
    for (int i = 0; i < count; i++) {
      xmlRecord.append(" ").append(reader.getAttributeLocalName(i)).append("=\"").append(reader.getAttributeValue(i))
        .append("\"");
    }
    xmlRecord.append(CLOSING_START_TAG_DELIMITER);
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

  /**
   * Class to track input stream and get total bytes read.
   */
  public class TrackingInputStream extends FilterInputStream {
    private long totalBytes = 0;

    public TrackingInputStream(InputStream in) {
      super(in);
    }

    @Override
    public int read(byte[] b) throws IOException {
      int count = super.read(b);
      this.totalBytes += count;
      return count;
    }

    @Override
    public int read() throws IOException {
      int count = super.read();
      this.totalBytes += count;
      return count;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int count = super.read(b, off, len);
      this.totalBytes += count;
      return count;
    }

    public long getTotalBytes() {
      return totalBytes;
    }
  }
}
