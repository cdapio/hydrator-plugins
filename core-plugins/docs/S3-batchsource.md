# Amazon S3 Batch Source


Description
-----------
Batch source to use Amazon S3 as a Source.


Use Case
--------
This source is used whenever you need to read from Amazon S3.
For example, you may want to read in log files from S3 every hour and then store
the logs in a TimePartitionedFileSet.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**authenticationMethod:** Authentication method to access S3. Defaults to Access Credentials.
 User need to have AWS environment only to use IAM role based authentication.
 For IAM, URI scheme should be s3a://. (Macro-enabled)

**accessID:** Access ID of the Amazon S3 instance to connect to. Mandatory if authentication method is Access credentials. (Macro-enabled)

**accessKey:** Access Key of the Amazon S3 instance to connect to. Mandatory if authentication method is Access credentials. (Macro-enabled)

**path:** Path to file(s) to be read. If a directory is specified,
terminate the path name with a '/'. The path uses filename expansion (globbing) to read files. (Macro-enabled)

**fileRegex:** Regex to filter out files in the path. It accepts regular expression which is applied to the complete
path and returns the list of files that match the specified pattern.
To use the *TimeFilter*, input ``timefilter``. The TimeFilter assumes that it is
reading in files with the File log naming convention of *YYYY-MM-DD-HH-mm-SS-Tag*.
The TimeFilter reads in files from the previous hour if the field ``timeTable`` is
left blank. If it is currently *2015-06-16-15* (June 16th 2015, 3pm), it will read
in files that contain *2015-06-16-14* in the filename. If the field ``timeTable`` is
present, then it will read in files that have not yet been read. (Macro-enabled)

**timeTable:** Name of the Table that keeps track of the last time files
were read in. (Macro-enabled)

**inputFormatClass:** Name of the input format class, which must be a
subclass of FileInputFormat. Defaults to TextInputFormat. (Macro-enabled)

**maxSplitSize:** Maximum split-size for each mapper in the MapReduce Job. Defaults to 128MB. (Macro-enabled)

**ignoreNonExistingFolders:** Identify if path needs to be ignored or not, for case when directory or file does not
exists. If set to true it will treat the not present folder as 0 input and log a warning. Default is false.

**recursive:** Boolean value to determine if files are to be read recursively from the path. Default is false.


Example
-------
This example connects to Amazon S3 using Access Credentials and reads in files found in the specified directory while
using the stateful ``timefilter``, which ensures that each file is read only once. The ``timefilter``
requires that files be named with either the convention "yy-MM-dd-HH..." (S3) or "...'.'yy-MM-dd-HH..."
(Cloudfront). The stateful metadata is stored in a table named 'timeTable'. With the maxSplitSize
set to 1MB, if the total size of the files being read is larger than 1MB, CDAP will
configure Hadoop to use one mapper per MB:

    {
        "name": "S3",
        "type": "batchsource",
        "properties": {
            "authenticationMethod": "Access Credentials",
            "accessKey": "key",
            "accessID": "ID",
            "path": "s3n://path/to/logs/",
            "fileRegex": "timefilter",
            "timeTable": "timeTable",
            "maxSplitSize": "1048576",
            "ignoreNonExistingFolders": "false",
            "recursive": "false"
        }
    }
