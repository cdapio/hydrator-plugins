# Windows Share Copy
Copies a file or files on a Microsoft Windows share to an HDFS directory.

## Plugin Configuration

| Configuration     | Required | Default | Description                                                           |
| ----------------- | :------: | :-----: | --------------------------------------------------------------------- |
| NetBios Domain Name | Yes       | `null`     | Specifies the NetBios domain name.                   |
| NetBios Hostname      | Yes       | n/a | Specifies NetBios hostname to import files from.         |
| NetBios Username        | Yes      | n/a     | Specifies the NetBios username to use when importing files from the Windows share  |
| NetBios Password | Yes       | n/a     | Specifies the NetBios password |
| NetBios Share Name | Yes       | n/a     | Specifies the NetBios share name |
| Number of Threads | No       | `1`     | Specifies the number of parallel tasks to use when executing the copy operation |
| Source Directory | Yes       | n/a     | Specifies the NetBios directory |
| Destination Directory | Yes       | n/a     | The valid full HDFS destination path in the same cluster where the file or files are to be moved. If a directory is specified as a destination with a file as the source, the source file will be put into that directory. If the source is a directory, it is assumed that destination is also a directory. This plugin does not check and will not catch any inconsistency |
| Buffer Size | No       | `4096`     | The size of the buffer to be used for copying the files. Value should be a multiple of the minimum size |
| Overwrite | No       | `true`     | Boolean that specifies if any matching files already present in the destination should be overwritten or not |
