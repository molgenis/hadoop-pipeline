# hadoop-pipeline
Implementation of a hadoop (map-reduce) Next-Generation Sequencing pipeline for fast single sample genetic diagnostics.

## Preperations
Before using the tool, be sure that the following has been done:

* A `.tar.gz` archive containing the tools has been uploaded to HDFS containing the required tools (see below).
* The needed bwa index files are uploaded to HDFS. Note that these should be in the same directory and have the same prefix. The required files are:
	* `<bwa_reference_file_prefix>.fasta`
	* `<bwa_reference_file_prefix>.fasta.amb`
	* `<bwa_reference_file_prefix>.fasta.ann`
	* `<bwa_reference_file_prefix>.fasta.bwt`
	* `<bwa_reference_file_prefix>.fasta.fai`
	* `<bwa_reference_file_prefix>.fasta.pac`
	* `<bwa_reference_file_prefix>.fasta.sa`

### Creating a tools.tar.gz
1. Create a `tools` directory. to store the tools in.
2. Add the following tools to the created directory:
	* Burrows-Wheeler Aligner
		1. Download it from [http://bio-bwa.sourceforge.net/](http://bio-bwa.sourceforge.net/).
		2. Extract the archive.
		3. From inside the extracted archive, use `make` (GNU Make) to create an executable file.
		4. Copy the created executable file from the extracted archive to the tools directory.
3. From the directory storing the tools folder, create a `.tar.gz` archive using `tar -zcf tools.tar.gz tools/`

The final hierachy of the created `tools.tar.gz` should look as follows:

	tools.tar.gz
		|- tools/
			|- bwa

Note: The used tools folder/archive name is arbitrary, though they should have the same name. The rest of the structure should be left intact however!

### Preparing the halvade upload tool
1. Create a local clone of [https://github.com/ddcap/halvade.git](https://github.com/ddcap/halvade.git).
2. From within the `halvade/halvade_upload_tools/` directory, use `ant` (Apache Ant) to create a jar file.
	* Optional: Before using `ant`, set `private static int LEVEL = 2;` from `src/be/ugent/intec/halvade/uploader/Logger.java` (line 32) to `0` for less output to stdout. 

The needed file can be found at: `dist/HalvadeUploaderWithLibs.jar`

### Create a .jar file of the application source
1. Create a local clone of [https://github.com/molgenis/hadoop-pipeline](https://github.com/molgenis/hadoop-pipeline) (the most recent commit).
2. From within the `hadoop-pipeline/hadoop-pipeline-application/` folder, use `mvn install` (Apache Maven) to create a jar file.

The needed file can be found at: `target/HadoopPipelineApplicationWithDependencies.jar`

## Execution
1. Upload the fastq files to HDFS using the halvade upload tool.
	* Example: `hadoop jar HalvadeUploaderWithLibs.jar -D dfs.block.size=134217728 -1 reads1.fastq.gz -2 reads2.fastq.gz -O /path/to/hdfs/output/folder/ -size 124`
	* Note that a block-size is set using -D (that is slightly larger than the block size defined for the halvade upload tool and must be a mutliple of 512) so that each created file is stored as a single block on HDFS.
	* See [https://github.com/ddcap/halvade/wiki/Halvade-Preprocessing](https://github.com/ddcap/halvade/wiki/Halvade-Preprocessing) for more information about the halvade upload tool.
