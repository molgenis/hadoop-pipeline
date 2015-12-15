# hadoop-pipeline
Implementation of a hadoop (map-reduce) Next-Generation Sequencing pipeline for fast single sample genetic diagnostics.

## Preperations
Before using the tool, be sure that the following has been done:

* The required executable `.jar` files are created.
* A `.tar.gz` archive containing the tools has been uploaded to HDFS containing the required tools.
* The needed bwa index files are uploaded to HDFS. This can be done using:

		hdfs dfs -put /local/fileORfolder/to/upload /hdfs/folder/to/upload/data/to/

	Note that these files should be in the same directory and have the same prefix. The required files are:

	* `<bwa_reference_file_prefix>.fasta`
	* `<bwa_reference_file_prefix>.fasta.amb`
	* `<bwa_reference_file_prefix>.fasta.ann`
	* `<bwa_reference_file_prefix>.fasta.bwt`
	* `<bwa_reference_file_prefix>.fasta.fai`
	* `<bwa_reference_file_prefix>.fasta.pac`
	* `<bwa_reference_file_prefix>.fasta.sa`
	* `<bwa_reference_file_prefix>.dict`

* A `.bed` file containing the grouping regions for the SAM records after BWA alignment. If a record fits into multiple groups, an individual record will be added to all the groups it has a (partial) overlap in. For more information about the bed-format, see [this](https://genome.ucsc.edu/FAQ/FAQformat.html#format1) page.

	* IMPORTANT: The bed file shoulde be UTF-8 compliant!

* A samplesheet csv file is present with information about the input data. Note that this file will be used for comparison with the last directory of each input file, so be sure that all input folders that will be digested are mentioned in this csv file. Be sure that all used samples are mentioned in the samplesheet csv file (and only these)! If the samplesheet contains information about more samples than used within the job, the other samples will still be added using an @RG tag to each created output file by the job (this to reduce application running time).

### Preparing the halvade upload tool
1. Create a local clone of [https://github.com/ddcap/halvade.git](https://github.com/ddcap/halvade.git).
2. From within the `halvade/halvade_upload_tools/` directory, use `ant` (Apache Ant) to create a jar file.
	* Optional: Before using `ant`, set `private static int LEVEL = 2;` from `src/be/ugent/intec/halvade/uploader/Logger.java` (line 32) to `0` for less output to stdout. 

The needed file can be found at: `dist/HalvadeUploaderWithLibs.jar`

### Create a .jar file of the application source
1. Create a local clone of [https://github.com/molgenis/hadoop-pipeline](https://github.com/molgenis/hadoop-pipeline) (the most recent commit).
2. From within the `hadoop-pipeline/hadoop-pipeline-application/` folder, use `mvn install` (Apache Maven) to create a jar file.

The needed file can be found at: `target/HadoopPipelineApplicationWithDependencies.jar`

Note: While this is enough to create an executable jar, for more advanced usage (such as requirements for the TestNG tests and updating), please refer to its own [README](./hadoop-pipeline-application/README.md).

### Creating a tools.tar.gz
On the [molgenis downloads page](https://molgenis26.target.rug.nl/downloads/hadoop/) a `.tar.gz` can be found containing several testing files and an already prepared tools archive for archive for Linux (tested on a Hadoop cluster running CentOS 6.7) and OS X (tested on v10.10.5). If these tool archives do not work or the download link does not, please use the steps below to create a new one.

IMPORTANT: When creating binaries, be sure to compile them on the same operating system as the Hadoop cluster uses. Furthermore, these created binaries need to be static!

1. Create a `tools` directory. to store the tools in.
2. Add the following tools to the created directory:
	* Burrows-Wheeler Aligner
		1. Download it from [http://bio-bwa.sourceforge.net/](http://bio-bwa.sourceforge.net/).
		2. Extract the archive.
		3. From inside the extracted archive, use `make` (GNU Make) to create an executable file.
		4. Copy the created executable file from the extracted archive to the tools directory.
3. From the directory storing the tools folder, create a `.tar.gz` archive using `tar -zcf <archive_name>.tar.gz tools/`

The final hierachy of the created tools `.tar.gz` should look as follows:

	<archive_name>.tar.gz
		|- tools/
			|- bwa
			|- info.xml

IMPORTANT: Be sure to use the exact naming as shown above! Only the archive name itself does not matter.

The `info.xml` file contains information of all tools present in the archive and should adhere to [this](./hadoop-pipeline-application/src/main/resources/tools_archive_info.xsd) Schema. An example of a correct `info.xml` file can be found within the tools archives present in the `.tar.gz` which can be downloaded from the [molgenis downloads page](https://molgenis26.target.rug.nl/downloads/hadoop/).

## Execution
1. Upload the fastq files to HDFS using the halvade upload tool:
	
		yarn jar HalvadeUploaderWithLibs.jar -1 reads1.fastq.gz -2 reads2.fastq.gz -O /path/to/hdfs/output/folder/ -size <size in mb>
	
	* The last folder of the output path should use the following format (so the files in this sample can be identified using the accompanying samplesheet csv file during the MapReduce job): 
		
			<sequencingStartDate>_<sequencer>_<run>_<flowcell>_<lane>/
		
	* To make proper use of data locality, be sure that a single created file is smaller than the HDFS block size. You can check the set HDFS block size of a given file using `hdfs dfs -stat %o /hdfs/path/to/file`.
	* See [https://github.com/ddcap/halvade/wiki/Halvade-Preprocessing](https://github.com/ddcap/halvade/wiki/Halvade-Preprocessing) for more information about the halvade upload tool.
2. Run the HadoopPipelineApplication:
	
		yarn jar HadoopPipelineApplicationWithDependencies.jar -t /hdfs/path/to/tools.tar.gz -i /hdfs/path/to/input/folder/ -o /hdfs/path/to/output/folder/ -r /hdfs/path/to/bwa/reference/data/file.fa(sta) -s /hdfs/path/to/samples/info/file.csv -b /hdfs/path/to/groups/file.bed
	
3. Download the results:
	
		hdfs dfs -get /hdfs/path/to/output/folder/ /local/folder/to/copy/results/to/


## Troubleshooting

__Problem:__

When running HadoopPipelineApplicationWithDependencies.jar, I get the following error:

	java.io.IOException: Incorrectly named path or samplesheet missing information about: hdfs/path/to/halvade_0_0.fq.gz

__Solution:__

To allow usage of multiple different samples within a single job, the last directory of the path on HDFS that stores the input files has to be in a specific format. The reason for this is so that using the samplesheet that is given using `-s`, the individual input files can be matched within the MapReduce job with their correct sample. This means that even if you only use a single input sample, it is still needed to adhere to these requirements.

---

__Problem:__

When running the job, I get the following error:

	Error: java.io.FileNotFoundException: Path is not a file:

__Solution:__

One of the input directories you've given contains a folder. Every input folder given as parameter should ONLY contain the expected `.fq.gz` input files. Alternatively, if you want to give a single folder containing subfolders that each contain the `.fq.gz` files from a single sample that should function as input, add the following argument instead `-D mapreduce.input.fileinputformat.input.dir.recursive=true` when executing the job jar.

---

__Problem:__

When running the application, I get the error:

	java.io.IOException: Invalid .fq.gz file found:

__Solution:__

As the job jar is written to assume input uploaded using the `HalvadeUploaderWithLibs.jar`, each input split is validated whether the file name of that split starts with `halvade_`. The reason the application quits after finding a `.fq.gz` file that starts with a different name is because this could indicate a wrongly uploaded file. Please be sure that the input folder ONLY contains `.fq.gz` files that are named in the way it is expected. Other file extensions will not cause any problems as these will simply be ignored (though as these will still be initially treated as input splits and are only disgarded within the Mappers themselves, it is still not advisable to have other files present in the input directories).

---

__Problem:__
The `-D` argument suggested below does not work.

__Solution:__
Be sure to place the `-D` argument right behind the `myapplication.jar` argument on the command line:

	yarn jar myapplication.jar -D <key>=<value> <application-specific arguments here>

As many `-D <key>=<value>` arguments can be placed as needed. Just be sure that each `<key>=<value>` pair is defined by a new `-D` argument.
 
---

__Problem:__
When uploading my files to halvade, the block size on HDFS is smaller than a single `.fq.gz` file.

__Solution:__
Either use lower `-size` with the `HalvadeUploaderWithLibs.jar` or increase the HDFS block size. Increasing the HDFS block size can be done either in the cluster config files or by using `-D dfs.block.size=<size in bytes>` when running the `HalvadeUploaderWithLibs.jar`. Be sure to use a value that is a multiple of 512 when setting the HDFS block size. Furthermore, be sure to use a value for `-size` of the `HalvadeUploaderWithLibs.jar` that is slightly lower than the `dfs.block.size` (as otherwise a single uploaded file might still be slightly bigger).

---

__Problem:__
When running the HadoopPipelineApplication.jar, I get a `java.lang.OutOfMemoryError: Java heap space` error.

__Solution:__
Try increasing `mapreduce.map.memory.mb` and `mapreduce.map.java.opts`. This can either be done in the cluster config files or by using `-D mapreduce.map.memory.mb=<size in mb>` and `mapreduce.map.java.opts=-Xmx<size in mb>m` (see [this](http://stackoverflow.com/questions/24070557/what-is-the-relation-between-mapreduce-map-memory-mb-and-mapred-map-child-jav/25945896#25945896) stackoverflow link and _Configuring MapReduce 2_ [here](http://hortonworks.com/blog/how-to-plan-and-configure-yarn-in-hdp-2-0/) for more information).

---

__Problem:__
When running the HadoopPipelineApplication.jar, I get an error with exit code 255.

__Solution:__
Try the solution above. If that does not solve the problem, please refer the the log files to find out what causes the error.

---

__Problem:__
it seems like the application does nothing. It takes a lot longer than expected.

__Solution:__
This could be due to a lack of available memory to run the binary tools. If possible, try letting it run to see if it eventually throws a `java.lang.OutOfMemoryError: Java heap space` error/exit code 255. Alternatively, simply kill the job and initiate a new one with more memory (see solution above).

---

__Problem:__
I get an error similar to that shown below.

	<year/month/day hours:minutes:seconds> INFO mapreduce.Job: Task Id : attempt_<attempt id>, Status : FAILED
	AttemptID:attempt_<attempt id> Timed out after <number> secs

__Solution:__
Try increasing the time before Hadoop ends a mapper/reducer if it has not contacted the context yet. This can be done by increasing the `mapreduce.task.timeout` value (either in the cluster config files or by using `-D`).
