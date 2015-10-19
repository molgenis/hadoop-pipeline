# hadoop-pipeline-application
The Hadoop MapReduce job jar.

## Preperations
1. use `git clone https://github.com/svandenhoek/hadoop-pipeline.git` to create a clone of the git repository.
2. Download the `hadoop-pipeline.tar.gz` from [here](https://molgenis26.target.rug.nl/downloads/hadoop/).
3. Extract the `hadoop-pipeline.tar.gz` to the exact same location as the created git clone (so that the directories will overlap with each other and the files will be placed in the correct locations).
4. Go to the  `hadoop-pipeline-application` directory.
5. Execute `sh TestNGPreprocessing.sh` so that the `tools.tar.gz` is copied and extracted to the `src/test/resources` folder.
6. Use `mvn install`.

An executable jar is created and the TestNG tests can be executed now.
