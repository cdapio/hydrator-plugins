/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.hydrator.plugin.batch.file;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class that contains file metadata fields.
 * Extend from this class to add credentials specific to different filesystems.
 */
public abstract class AbstractFileMetadata implements Comparable {

  public static final String FILE_NAME = "fileName";
  public static final String FILE_SIZE = "fileSize";
  public static final String MODIFICATION_TIME = "modificationTime";
  public static final String OWNER = "owner";
  public static final String GROUP = "group";
  public static final String FULL_PATH = "fullPath";
  public static final String IS_FOLDER = "isFolder";
  public static final String RELATIVE_PATH = "relativePath";
  public static final String PERMISSION = "permission";
  public static final String HOST_URI = "hostURI";

  public static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "metadata",
    Schema.Field.of(FILE_NAME, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(FULL_PATH, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(FILE_SIZE, Schema.of(Schema.Type.LONG)),
    Schema.Field.of(MODIFICATION_TIME, Schema.of(Schema.Type.LONG)),
    Schema.Field.of(GROUP, Schema.of(Schema.Type.LONG)),
    Schema.Field.of(OWNER, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(IS_FOLDER, Schema.of(Schema.Type.BOOLEAN)),
    Schema.Field.of(RELATIVE_PATH, Schema.of(Schema.Type.STRING)),
    Schema.Field.of(PERMISSION, Schema.of(Schema.Type.INT)),
    Schema.Field.of(HOST_URI, Schema.of(Schema.Type.STRING))
  );


  // contains only the name of the file
  private final String fileName;

  // full path of the file in the source filesystem
  private final String fullPath;

  // file size
  private final long fileSize;

  // modification time of file
  private final long modificationTime;

  // file owner's group
  private final String group;

  // file owner
  private final String owner;

  // whether or not the file is a folder
  private final boolean isFolder;

  /*
   * The relavite path is constructed by deleting the portion of the source
   * path that comes before the last path separator ("/") from the full path.
   * It is assumed here that the source path is always a prefix of the full
   * path.
   *
   * For example, given full path http://example.com/foo/bar/baz/index.html
   *              and source path /foo/bar
   *              the relative path will be bar/baz/index.html
   */
  private final String relativePath;

  // file permission, encoded in short
  private final short permission;

  /*
   * URI for the Filesystem
   * For instance, the hostURI for http://abc.def.ghi/new/index.html is http://abc.def.ghi
   */
  private final String hostURI;

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileMetadata.class);

  protected AbstractFileMetadata(FileStatus fileStatus, String sourcePath) throws IOException {
    fileName = fileStatus.getPath().getName();
    fullPath = fileStatus.getPath().toUri().getPath();
    isFolder = fileStatus.isDirectory();
    modificationTime = fileStatus.getModificationTime();
    owner = fileStatus.getOwner();
    group = fileStatus.getGroup();
    fileSize = fileStatus.getLen();
    permission = fileStatus.getPermission().toShort();

    // check if sourcePath is a valid prefix of fullPath
    if (fullPath.startsWith(sourcePath)) {
      relativePath = fullPath.substring(sourcePath.lastIndexOf(Path.SEPARATOR) + 1);
    } else {
      throw new IOException("sourcePath should be a valid prefix of fullPath");
    }

    // construct host URI given the full path from filestatus
    try {
      hostURI = new URI(fileStatus.getPath().toUri().getScheme(), fileStatus.getPath().toUri().getHost(), null, null)
        .toString();
    } catch (URISyntaxException e) {
      throw new IOException(e.getMessage());
    }
  }

  protected AbstractFileMetadata(StructuredRecord record) {
    this.fileName = record.get(FILE_NAME);
    this.fullPath = record.get(FULL_PATH);
    this.modificationTime = record.get(MODIFICATION_TIME);
    this.group = record.get(GROUP);
    this.owner = record.get(OWNER);
    this.fileSize = record.get(FILE_SIZE);
    this.isFolder = record.get(IS_FOLDER);
    this.relativePath = record.get(RELATIVE_PATH);
    this.permission = record.get(PERMISSION);
    this.hostURI = record.get(HOST_URI);
  }

  /**
   * use this constructor to deserialize from DataInput
   * @param dataInput
   */
  protected AbstractFileMetadata(DataInput dataInput) throws IOException {
    this.fileName = dataInput.readUTF();
    this.fullPath = dataInput.readUTF();
    this.modificationTime = dataInput.readLong();
    this.group = dataInput.readUTF();
    this.owner = dataInput.readUTF();
    this.fileSize = dataInput.readLong();
    this.isFolder = dataInput.readBoolean();
    this.relativePath = dataInput.readUTF();
    this.permission = dataInput.readShort();
    this.hostURI = dataInput.readUTF();
  }

  public String getFullPath() {
    return fullPath;
  }

  public String getFileName() {
    return fileName;
  }

  public long getFileSize() {
    return fileSize;
  }

  public long getModificationTime() {
    return modificationTime;
  }

  public String getGroup() {
    return group;
  }

  public String getOwner() {
    return owner;
  }

  public boolean isFolder() {
    return isFolder;
  }

  public String getRelativePath() {
    return relativePath;
  }

  public short getPermission() {
    return permission;
  }

  public String getHostURI() {
    return hostURI;
  }

  /**
   * Converts to StructuredRecord
   */
  public StructuredRecord toRecord() {

    // initialize output schema
    Schema outputSchema;
    List<Schema.Field> fieldList = new ArrayList<>(DEFAULT_SCHEMA.getFields());
    fieldList.addAll(getCredentialSchema().getFields());
    outputSchema = Schema.recordOf("metadata", fieldList);

    StructuredRecord.Builder outputBuilder = StructuredRecord.builder(outputSchema)
      .set(FILE_NAME, fileName)
      .set(FULL_PATH, fullPath)
      .set(FILE_SIZE, fileSize)
      .set(MODIFICATION_TIME, modificationTime)
      .set(GROUP, group)
      .set(OWNER, owner)
      .set(IS_FOLDER, isFolder)
      .set(RELATIVE_PATH, relativePath)
      .set(PERMISSION, permission)
      .set(HOST_URI, hostURI);
    addCredentialsToBuilder(outputBuilder);

    return outputBuilder.build();
  }

  @Override
  public int compareTo(Object o) {
    return Long.compare(fileSize, ((AbstractFileMetadata) o).getFileSize());
  }

  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeUTF(getFileName());
    dataOutput.writeUTF(getFullPath());
    dataOutput.writeLong(getModificationTime());
    dataOutput.writeUTF(getGroup());
    dataOutput.writeUTF(getOwner());
    dataOutput.writeLong(getFileSize());
    dataOutput.writeBoolean(isFolder());
    dataOutput.writeUTF(getRelativePath());
    dataOutput.writeShort(getPermission());
    dataOutput.writeUTF(getHostURI());
  }

  /**
   * @return Credential schema for different filesystems
   */
  protected abstract Schema getCredentialSchema();

  protected abstract void addCredentialsToBuilder(StructuredRecord.Builder builder);
}
