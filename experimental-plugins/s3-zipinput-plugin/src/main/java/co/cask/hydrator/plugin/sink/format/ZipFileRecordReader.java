package co.cask.hydrator.plugin.sink.format;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * This RecordReader implementation extracts individual files from a ZIP
 * file and hands them over to the Mapper. The "key" is the decompressed
 * file name, the "value" is the file contents.
 */
public class ZipFileRecordReader
  extends RecordReader<Text, BytesWritable>
{
  /** InputStream used to read the ZIP file from the FileSystem */
  private FSDataInputStream fsin;

  /** ZIP file parser/decompresser */
  private ZipInputStream zip;

  /** Uncompressed file name */
  private Text currentKey;

  /** Uncompressed file contents */
  private BytesWritable currentValue;

  /** Used to indicate progress */
  private boolean isFinished = false;

  /**
   * Initialise and open the ZIP file from the FileSystem
   */
  @Override
  public void initialize( InputSplit inputSplit, TaskAttemptContext taskAttemptContext )
    throws IOException, InterruptedException
  {
    FileSplit split = (FileSplit) inputSplit;
    Configuration conf = taskAttemptContext.getConfiguration();
    Path path = split.getPath();
    FileSystem fs = path.getFileSystem( conf );

    // Open the stream
    fsin = fs.open( path );
    zip = new ZipInputStream( fsin );
  }

  /**
   * This is where the magic happens, each ZipEntry is decompressed and
   * readied for the Mapper. The contents of each file is held *in memory*
   * in a BytesWritable object.
   *
   * If the ZipFileInputFormat has been set to Lenient (not the default),
   * certain exceptions will be gracefully ignored to prevent a larger job
   * from failing.
   */
  @Override
  public boolean nextKeyValue()
    throws IOException, InterruptedException
  {
    ZipEntry entry = null;
    try
    {
      entry = zip.getNextEntry();
    }
    catch ( ZipException e )
    {
      if ( ZipFileInputFormat.getLenient() == false )
        throw e;
    }

    // Sanity check
    if ( entry == null )
    {
      isFinished = true;
      return false;
    }

    // Filename
    currentKey = new Text( entry.getName() );

    // Read the file contents
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    byte[] temp = new byte[8192];
    while ( true )
    {
      int bytesRead = 0;
      try
      {
        bytesRead = zip.read( temp, 0, 8192 );
      }
      catch ( EOFException e )
      {
        if ( ZipFileInputFormat.getLenient() == false )
          throw e;
        return false;
      }
      if ( bytesRead > 0 )
        bos.write( temp, 0, bytesRead );
      else
        break;
    }
    zip.closeEntry();

    // Uncompressed contents
    currentValue = new BytesWritable( bos.toByteArray() );
    return true;
  }

  /**
   * Rather than calculating progress, we just keep it simple
   */
  @Override
  public float getProgress()
    throws IOException, InterruptedException
  {
    return isFinished ? 1 : 0;
  }

  /**
   * Returns the current key (name of the zipped file)
   */
  @Override
  public Text getCurrentKey()
    throws IOException, InterruptedException
  {
    return currentKey;
  }

  /**
   * Returns the current value (contents of the zipped file)
   */
  @Override
  public BytesWritable getCurrentValue()
    throws IOException, InterruptedException
  {
    return currentValue;
  }

  /**
   * Close quietly, ignoring any exceptions
   */
  @Override
  public void close()
    throws IOException
  {
    try { zip.close(); } catch ( Exception ignore ) { }
    try { fsin.close(); } catch ( Exception ignore ) { }
  }
}
