# hadoop-pipeline-application
The Hadoop MapReduce job jar. If you only want an executable jar, please review the general [README](../README.md) instead. This readme includes complete instruction so the TestNG tests and alike can also be ran.

## Requirements
A system running unix with the following programs runnable through the command line:

* `git`
* `mvn` (maven)
* `tr` (translate characters)
* `uname` (used by `TestNGPreprocessing.sh` to look up the kernel name to define the needed tools archive).

A tools archive has already been created for the following kernels:

* Linux (tested on CentOS 6.7)
* Darwin (tested on OS X 10.10.5)

If the created tool archives do not work properly, please view the general [README](../README.md) for information about how to create a tools archive.

## Preperations
1. use `git clone https://github.com/svandenhoek/hadoop-pipeline.git` to create a clone of the git repository.
2. Download the `hadoop-pipeline.tar.gz` from [here](https://molgenis26.target.rug.nl/downloads/hadoop/).
3. Extract the `hadoop-pipeline.tar.gz` to the exact same location as the created git clone (so that the directories will overlap with each other and the files will be placed in the correct locations).
4. Go to the  `hadoop-pipeline-application` directory.
5. Use `mvn install`.
6. Execute `sh TestNGPreprocessing.sh`.

An executable jar is created and the TestNG tests can now be executed.

IMPORTANT: Whenever doing `mvn clean`, follow the steps starting from step 5 before doing any TestNG tests again!
