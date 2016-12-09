# Run Transform


Description
-----------
Runs an executable binary which is installed and available on the local filesystem of the Hadoop nodes. Run transform
plugin allows the user to read the structured record as input and returns the output record, to be further processed
downstream in the pipeline.

Use Case
--------
In enterprise, there are some existing tools or executable binaries that perform complex transformations of data.
This plugin can be used, when user would like to execute these type of binaries that will read the structured record
as input, process it and retrieve the results back to the pipeline.

Properties
----------
**commandToExecute:** Command that will contain the full path to the executable binary present on the local
filesystem of the Hadoop nodes as well as how to execute that binary. It should not contain any input arguments. For
example, java -jar /home/user/ExampleRunner.jar, if the binary to be executed is of type jar.

**fieldsToProcess:** A comma-separated sequence of the fields that will be passed to the binary through STDIN as an
varying input. For example, 'firstname' or 'firstname,lastname' in case of multiple inputs. Please make sure that
 the sequence of fields is in the order as expected by binary. (Macro Enabled)

**fixedInputs:** A space-separated sequence of the fixed inputs that will be passed to the executable binary
through STDIN. Please make sure that the sequence of inputs is in the order as expected by binary. All the
fixed inputs will be followed by the variable inputs, provided through 'Fields to Process for Variable Inputs'.
(Macro enabled)

**outputField:** The field name that holds the output of the executable binary.

**outputFieldType:** Schema type of the 'Output Field'. Supported types are: boolean, bytes, double, float, int, long
and string.

Conditions
----------
Executable binary and its dependencies must be available on all the nodes of the Hadoop cluster, prior to the execution
of binary.

Executable binary will always read the input through STDIN and should generate the STDOUT for each input record.
Also, errors emitted by the executable through STDERR will be captured in logs.

Supported types for binary to be executed are: 'exe, sh, bat and jar'.

Path to the executable binary, specified in 'commandToExecute' property, should be an absolute path not the URI
path i.e. should not start with hdfs:// or file:///.

Executable binary can take 0 to N inputs. Source for the varying inputs will always be the structured records coming
through the Hydrator source stage and will passed to the binary through STDIN. Required fields can be provided
using 'fieldsToProcess' property.

Fixed inputs (if any), will always be followed by the varying inputs. All the inputs will be passed as space
separated sequence to the executable binary through STDIN. This will be the format for sending the inputs to the
executable binary.

Inputs should be in the expected order and the supported format. Any mismatch in the sequence will result into the
runtime failure.

Example
-------
This example will run the executable binary of type jar: '/home/user/Permutations.jar' present on the local
filesystem of the Hadoop node. Executable binary will read the varying input through 'word' field coming from
the input record. Also, it will take some fixed inputs '50 true' for the processing. Output of the executable
binary will be saved in the 'permutation' field.

    {
      "name": "Run",
      "type": "transform",
        "properties": {
          "commandToExecute": "java -jar /home/user/Permutations.jar",
          "fieldsToProcess": "word",
          "fixedInputs": "50 true",
          "outputField": "permutation",
          "outputFieldType": "string"
        }
    }

For example, suppose the Run transform receives the input record:

    +=============================+
    | id : STRING | word : STRING |
    +=============================+
    | W1          | AAC           |
    | W2          | ABC           |
    | W3          | AACE          |
    +=============================+

Output records will contain all the input fields along with the output field 'permutation', that will be passed to the
next stage in the pipeline:

    +========================================================================================================+
    | id : STRING | word : STRING | permutation : STRING                                                     |
    +========================================================================================================+
    | W1          | AAC           | [AAC, ACA, CAA]                                                          |
    | W2          | ABC           | [ACB, ABC, BCA, CBA, CAB, BAC]                                           |
    | W3          | AACE          | [AACE, AAEC, ACAE, ACEA, AEAC, AECA, CAAE, CAEA, CEAA, EAAC, EACA, ECAA] |
    +========================================================================================================+
