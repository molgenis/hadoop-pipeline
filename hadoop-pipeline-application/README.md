# hadoop-pipeline-application
The Hadoop MapReduce job jar.

## Preperations
1. use `git clone https://github.com/svandenhoek/hadoop-pipeline.git` to create a clone of the git repository.
2. Download the `hadoop-pipeline.tar.gz` from [here](https://molgenis26.target.rug.nl/downloads/hadoop/).
3. Extract the `hadoop-pipeline.tar.gz` to the exact same location as the created git clone (so that the directories will overlap with each other and the files will be placed in the correct locations).
4. Go to the  `hadoop-pipeline-application` directory.
5. Use `mvn install`.
6. Execute `sh TestNGPreprocessing.sh`.

An executable jar is created and the TestNG tests can now be executed.

IMPORTANT: Whenever doing `mvn clean`, follow the steps starting from step 5 before doing any TestNG tests!
