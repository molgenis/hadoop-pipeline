package org.molgenis.hadoop.pipeline.application;

public enum TestFile
{
	/**
	 * Mini version of the L1 fastq testing data.
	 */
	FASTQ_DATA_MINI_L1("input_fastq_mini/150616_SN163_0648_AHKYLMADXX_L1/halvade_0_0.fq.gz", TestFileType.FASTQ_GZIP),
	/**
	 * The L1 fastq test data.
	 */
	FASTQ_DATA_L1("input_fastq/150616_SN163_0648_AHKYLMADXX_L1/halvade_0_0.fq.gz", TestFileType.FASTQ_GZIP),
	/**
	 * The SAM results when the L1 mini fastq data is aligned using: /path/to/bwa mem -p -M -R
	 * "@RG\tID:1\tPL:illumina\tLB:150616_SN163_648_AHKYLMADXX_L1\tSM:sample1"
	 * hadoop-pipeline-application/src/test/resources/reference_data/chr1_20000000-21000000.fa - <
	 * hadoop-pipeline-application/src/test/resources/input_fastq_mini/150616_SN163_0648_AHKYLMADXX_L1/halvade_0_0.fq.gz
	 * > hadoop-pipeline-application/src/test/resources/expected_bwa_outputs/output_L1_mini.sam
	 */
	ALIGNED_READS_MINI_L1("expected_bwa_outputs/output_L1_mini.sam", TestFileType.SAM),
	/**
	 * The SAM results when the L1 fastq data is aligned using: /path/to/bwa mem -p -M -R
	 * "@RG\tID:1\tPL:illumina\tLB:150616_SN163_648_AHKYLMADXX_L1\tSM:sample1"
	 * hadoop-pipeline-application/src/test/resources/reference_data/chr1_20000000-21000000.fa - <
	 * hadoop-pipeline-application/src/test/resources/three_samples/150616_SN163_0648_AHKYLMADXX_L1/halvade_0_0.fq.gz >
	 * hadoop-pipeline-application/src/test/resources/expected_bwa_outputs/output_L1.sam
	 */
	ALIGNED_READS_L1("expected_bwa_outputs/output_L1.sam", TestFileType.SAM),
	/**
	 * A grouping dataset for grouping aligned reads depending on the region they aligned to.
	 */
	GROUPS_SET1("bed_files/chr1_20000000-21000000.bed", TestFileType.BED);

	/**
	 * The path to the file.
	 */
	private String filePath;

	/**
	 * The file type. This determines which reader options can be used within the {@link TestFileReader}.
	 */
	private TestFileType fileType;

	TestFile(String filePath, TestFileType fileType)
	{
		this.filePath = filePath;
		this.fileType = fileType;
	}

	public String getFilePath()
	{
		return filePath;
	}

	public TestFileType getFileType()
	{
		return fileType;
	}

	/**
	 * The type of the {@link TestFile}.
	 */
	public enum TestFileType
	{
		/**
		 * A test gzipped fastq file.
		 */
		FASTQ_GZIP,
		/**
		 * A test SAM file.
		 */
		SAM,
		/**
		 * A test BED file.
		 */
		BED;
	}
}
