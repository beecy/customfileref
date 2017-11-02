# Manipulating Whole Files in StreamSets Data Collector

[StreamSets Data Collector](https://streamsets.com/products/sdc/) (SDC) can move entire files from an origin system to a destination system via its [Whole File](https://streamsets.com/documentation/datacollector/latest/help/index.html#Data_Formats/WholeFile.html#concept_nfc_qkh_xw) data format. SDC does not parse Whole File data; instead, it simply streams it from the origin system to the destination system. Currently, the S3, Directory and SFTP/FTP client origins support Whole Files, alongside the S3, Azure Data Lake Store, Hadoop FS, Local FS and MapR FS destinations.

This example custom processor shows how to manipulate Whole File data in the pipeline. The processor reads data from the incoming Whole File, applies [ROT13](https://en.wikipedia.org/wiki/ROT13) to it, writes the encoded data to a temporary file on disk, and sets the `/fileRef` and `/fileInfo` fields in the record to a custom `FileRef` implementation referencing the newly written file.

You will need to edit `${SDC_CONF}/sdc-security.policy` to give the custom processor permission to access the relevant directories on disk. For example:

	grant codebase "file://${sdc.dist.dir}/user-libs/customfileref/-" {
	  // Input files
	  permission java.io.FilePermission "/Users/pat/Downloads/shakespeare/-", "read";
	  // Output files
	  permission java.io.FilePermission "/tmp/rot13", "read";
	  permission java.io.FilePermission "/tmp/rot13/-", "read,write,execute,delete";
	  // File metadata
	  permission java.lang.RuntimePermission "accessUserInformation";
	};

NOTE - ROT13 should NEVER be used to encrypt data - it is simply used as an example of a transformation applied to an entire file!