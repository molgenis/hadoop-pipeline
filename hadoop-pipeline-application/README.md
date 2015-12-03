# hadoop-pipeline-application
The Hadoop MapReduce job jar. If you only want an executable jar, please review the general [README](../README.md) instead. This readme includes information such as requirements for running the TestNG tests, updating the external library archive to a newer version and more.


## Running TestNG tests

### Requirements
A system running with the following programs runnable through the command line:

* `git`
* `mvn` (maven)
* `python` (used within some TestNG tests)
* `uname` (used by `TestNGPreprocessing.sh` to look up the kernel name to define the needed tools archive).

A tools archive has already been created for the following kernels:

* Linux (tested on CentOS 6.7)
* Darwin (tested on OS X 10.10.5)

If the created tool archives do not work properly, please view the general [README](../README.md#creating-a-toolstargz) for information about how to create a tools archive.

Within some TestNG tests a python script is used for testing the Process calling and input/output digestion. This is done by calling `python`, followed by the python script path and (where needed) some arguments. Note however that this python script has been tested to work with both Python v2.7.10 and Python v3.5.0, so it shouldn't matter whether the command `python` points to python2 or python3.

### Preperations
1. use `git clone https://github.com/svandenhoek/hadoop-pipeline.git` to create a clone of the git repository.
2. Download the `hadoop-pipeline.tar.gz` from [here](https://molgenis26.target.rug.nl/downloads/hadoop/).
3. Extract the content of `hadoop-pipeline.tar.gz` into directory of the cloned repository (so that the subdirectories will overlap with each other and the files will be placed in the correct location).
4. Go to the  `hadoop-pipeline-application` directory.
5. Use `mvn install`.
6. Execute `sh TestNGPreprocessing.sh`.

An executable jar is created and the TestNG tests can now be executed.

IMPORTANT: Whenever doing `mvn clean`, follow the steps starting from step 5 before doing any TestNG tests again!

## Updating the external files archive

When updating to a newer version of the application, sometimes a newer version of the external files archive is needed for testing the application. In this case, first remove all folders/files that were added when the archive was extracted into the repository folder (an `external_files.txt` was extracted into the main repository folder containing info about which folders/files were added with it). Then, extract the new external files archive over the repository folder.

Alternatively, simply create a new clone elsewhere and follow the steps as mentioned in _Preperations_.

## Troubleshooting

__Problem:__
When running a TestNG test, a `java.lang.NullPointerException` is given.

__Solution:__
Please be sure `sh TestNGPreprocessing.sh` was ran. If the error is caused in a line similar to `getClassLoader().getResource("<some_file_or_directory_path>").toString()`, the error is probably caused due to not all required files being present.
 
---

__Problem:__
The kernel/operating system I use is not supported.

__Solution:__
The `sh TestNGPreprocessing.sh` script is supplied as an added layer of convenience. However, manually copying/extracting the required files also works. Feel free to submit a pull request to include your kernel/OS of preference for the `TestNGPreprocessing.sh` script. Otherwise, follow the steps below to get the TestNG tests to work.

1. Copy the file `../hadoop-pipeline-tools/linux_tools.tar.gz` to `target/test-classes/`.
2. Extract the correct tools archive into `target/test-classes/` (a folder called `tools` with the created binaries in it should be created).
3. Make sure the binaries are allowed to be executed (by the owner).

IMPORTANT: Just like with the automiation script, these steps need to be repeated every time a `mvn clean` was done (in replacement of using the `TestNGPreprocessing.sh` script).

---

__Problem:__
I'm getting an error saying `java.io.IOException: Cannot run program "/path/to/target/test-classes/tools/<binary name>": error=13, Permission denied`.

__Solution:__
Assuming you added the needed files by hand instead of using `TestNGPreprocessing.sh`, be sure that step 3 is done for ALL binaries.

---

__Problem:__
The python script `CharacterReplacer.py` is not working.

__Solution:__
Be sure that python2 or python3 is installed. Windows normally doesn't come with python pre-installed. It can be downloaded [here](https://www.python.org/).

If python is installed, be sure it's callable by using `python` through the command line.

If the above also works, try switching to a different python version (so change the alias `python` to refer to python2 if you tested with python3 before and vice versa).

For UNIX systems where python can't run on, there is another fix as well. If `tr` (translate characters) is available through the command line, you can replace the usage of the python script with using `tr` instead. This can be done as follows:

Replace code occurences with `ProcessBuilder("python", getClassLoader().getResource("CharacterReplacer.py").getPath(), "<character>", "<character>")` within TestNG classes with `ProcessBuilder("tr", "<character>", "<character>")`.

---

__Problem:__
After updating all TestNG tests still don't run properly.

__Solution:__
Please compare the `external_files.txt` (based on git) with the `archive_files.txt` (based on the extracted external files archive) whether the correct external tools archive was used. Ideally, the files should be identical. However, in most cases a newer external files archive should work with an older git commit (though this is not guaranteed).

If these info files are the same, either check if the actual files which should be present are actually there or just remove all of them right away and extract the archive again over the repository directory.

If the above also fails, it is suggested to create a new clone combined with the most recent extern files archive.
