# hadoop-pipeline-halvade-testing
Short script to test if the created test data uploaded with the halvade upload tool remains intact.

## How-to
1. Upload `testdata_1.fq.gz` and `testdata2.fq.gz` present in the `./data` folder using the halvade upload tool as described in the main [README](../README.md).
	* Be sure to use only a single thread and the file size is set above 20 mb, as otherwise multiple files will be created.
2. Download the file created by the halvade upload tool from HDFS (called `halvade_0_0.fq.gz`).
3. Merge the original files using:

		cat ./data/testdata_1.fq.gz ./data/testdata_2.fq.gz > ./data/testdata_merged.fq.gz

4. Compare the merged original data with the created (and downloaded) output from the halvade upload tool:

		python3 ./FastqFileComparer/FastqFileComparer.py ./data/testdata_merged.fq.gz /location/to/downloaded/halvade_0_0.fq.gz

If the output is correct, it should look like:

	### loading files ###
	### comparing files ###
	equal:  True
	size:  297030  -  297030
	@1:20000000-21000000-10/1                                    - @1:20000000-21000000-10/1
	@1:20000000-21000000-10/2                                    - @1:20000000-21000000-10/2
	@1:20000000-21000000-100/1                                   - @1:20000000-21000000-100/1
	@1:20000000-21000000-100/2                                   - @1:20000000-21000000-100/2
	@1:20000000-21000000-1000/1                                  - @1:20000000-21000000-1000/1
	@1:20000000-21000000-1000/2                                  - @1:20000000-21000000-1000/2
	@1:20000000-21000000-10000/1                                 - @1:20000000-21000000-10000/1
	@1:20000000-21000000-10000/2                                 - @1:20000000-21000000-10000/2
	@1:20000000-21000000-100000/1                                - @1:20000000-21000000-100000/1
	@1:20000000-21000000-100000/2                                - @1:20000000-21000000-100000/2
	### differences ###
	n.a.
