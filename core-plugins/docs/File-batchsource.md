# File Batch Source


Description
-----------
Batch source to use any Distributed File System as a Source.


Use Case
--------
This source is used whenever you need to read from a distributed file system.
For example, you may want to read in log files from S3 every hour and then store
the logs in a TimePartitionedFileSet.


Properties
----------
**referenceName:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**fileSystem:** Distributed file system to read in from.

**fileSystemProperties:** A JSON string representing a map of properties
needed for the distributed file system.
For example, the property names needed for S3 are "fs.s3n.awsSecretAccessKey"
and "fs.s3n.awsAccessKeyId".

**path:** Path to file(s) to be read. If a directory is specified,
terminate the path name with a '/'.

**fileRegex:** Regex to filter out filenames in the path.
To use the *TimeFilter*, input ``timefilter``. The TimeFilter assumes that it is
reading in files with the File log naming convention of *YYYY-MM-DD-HH-mm-SS-Tag*.
The TimeFilter reads in files from the previous hour if the field ``timeTable`` is
left blank. If it's currently *2015-06-16-15* (June 16th 2015, 3pm), it will read
in files that contain *2015-06-16-14* in the filename. If the field ``timeTable`` is
present, then it will read in files that have not yet been read.

**timeTable:** Name of the Table that keeps track of the last time files
were read in.

**inputFormatClass:** Name of the input format class, which must be a
subclass of FileInputFormat. Defaults to CombineTextInputFormat.

**maxSplitSize:** Maximum split-size for each mapper in the MapReduce Job. Defaults to 128MB.


Example
-------
This example connects to Amazon S3 and reads in files found in the specified directory while
using the stateful Timefilter, which ensures that each file is read only once. The Timefilter
requires that files be named with either the convention ``"yy-MM-dd-HH..."`` (S3) or ``"...'.'yy-MM-dd-HH..."``
(Cloudfront). The stateful metadata is stored in a table named 'timeTable'. With the maxSplitSize
set to 1MB, if the total size of the files being read is larger than 1MB, CDAP will
configure Hadoop to use more than one mapper:

    {
        "name": "FileBatchSource",
        "type": "batchsource",
        "properties": {
            "fileSystem": "S3",
            "fileSystemProperties": "{
                \"fs.s3n.awsAccessKeyId\": \"accessID\",
                \"fs.s3n.awsSecretAccessKey\": \"accessKey\"
            }",
            "path": "s3n://path/to/logs/",
            "fileRegex": "timefilter",
            "timeTable": "timeTable",
            "maxSplitSize": "1048576"
        }
    }
