# Manipulating Whole Files in StreamSets Data Collector

[StreamSets Data Collector](https://streamsets.com/products/sdc/) (SDC) can move entire files from an origin system to a destination system via its [Whole File](https://streamsets.com/documentation/datacollector/latest/help/index.html#Data_Formats/WholeFile.html#concept_nfc_qkh_xw) data format. SDC does not parse Whole File data; instead, it simply streams it from the origin system to the destination system. Currently, the S3, Directory and SFTP/FTP client origins support Whole Files, alongside the S3, Azure Data Lake Store, Hadoop FS, Local FS and MapR FS destinations.

This example custom processor shows how to manipulate Whole File data in the pipeline. The processor reads data from the incoming Whole File, applies [ROT13](https://en.wikipedia.org/wiki/ROT13) to it, writes the encoded data to a temporary file on disk, and sets the `/fileRef` and `/fileInfo` fields in the record to a custom `FileRef` implementation referencing the newly written file.

NOTE - ROT13 should NEVER be used to encrypt data - it is simply used as an example of a transformation applied to an entire file!