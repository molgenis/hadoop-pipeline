#!/bin/env bash

#########################################################################
# Name:     ReadGroupLineGenerator.sh                                   #
# Function: Using a samplesheet file, creates a readgroupline which can #
#           be uploaded for usage within a HadoopPipelineApplication    #
#           MapReduce job. The content of this readgroupline will be    #
#           based upon the given fastq file. Therefore, it is of vital  #
#           importance to use the same fastq file with this bash script #
#           as was used to create the uploaded fastq data that will be  #
#           used within the MapReduce job.                              #
#           Do note that while it is assumed the fastq is a path to an  #
#           existing file, any random string can be put there for       #
#           comparison (though Strings with a different pattern will    #
#           simply not match). If no match is found, nothing will be    #
#           written to stdout. Otherwise, a String containing the       #
#           readgroupline is written to stdout.                         #
#                                                                       #
# Usage:    ReadGroupLineGenerator.sh <samplesheet csv> <fastq file to  #
#           create readgroupline for>                                   #
#########################################################################


#########################################################################
# Name:     main                                                        #
# Function: Executes the script functionalities. Also prints the        #
#           results and any error messages.                             #
#########################################################################
function main {
	samplesheet=$1
	# Expected line fields format. The number represents the field position starting
	# from left (1-based), where each field is seperated by a ',' (comma):
	#  4: sequencer
	# 10: sequencingStartDate
	# 11: run
	# 12: flowcell
	# 13: lane
	
	fastqFile=$2
	# Expected file name format. The number represents the field position starting
	# from left (1-based), where each field is seperated by an '_' (underscore):
	#  1: sequencingStartDate
	#  2: sequencer
	#  3: run
	#  4: flowcell
	#  5: lane
	
	if $(checkIfVariableFilled $samplesheet) && $(checkIfVariableFilled $fastqFile)
	then
		generateReadGroupLine
		if $(checkIfVariableFilled $readGroupLine)
		then
			# If match was found, writes readgroupline to stdout.
			echo "$readGroupLine"
		else
			# If no matching pattern was found, writes message to stderr.
			echo "No matching sample data was found in "${samplesheet##*\/}" for the file "${fastqFile##*\/} >&2
		fi
	else
		echo 'Usage: ReadGroupLineGenerator.sh <samplesheet csv> <fastq file to create readgroupline for>'
	fi
}

#########################################################################
# Name:     checkIfVariableFilled                                       #
# Function: Checks whether a variable is empty.                         #
# Input:    String                                                      #
# Output:   1 if empty, 0 if filled.                                    #
#########################################################################
function checkIfVariableFilled {
    if [ -z "$1" ]
    then
        return 1
    else
        return 0
    fi
}

#########################################################################
# Name:     generateReadGroupLine                                       #
# Function: Compares the fields within the fastq file name to the lines #
#           present in the samplesheet csv file. The results are stored #
#           in a variable called readGroupLine. If the fields in a      #
#           specific line match with the available fields in the fastq  #
#           file name, the variable readGroupLine will store the        #
#           readgroupline data. If no matching line was found, the      #
#           variable will stay empty. If a match was found, stops       #
#           looking for any other lines that might match.               #
# Output:   Writes a String to stdout if a match was found, otherwise   #
#           does not write to stdout.                                   #
#########################################################################
function generateReadGroupLine {
	# echo: removes anything before (and including) the last '/' (forward slash)
	# echo: removes anything after (and including) the first '.' (dot)
	# awk:  reads the fields seperated by a _ and returns only the fields needed for comparison
	fastqFileFieldsToCompare=$(echo ${fastqFile##*\/})
	fastqFileFieldsToCompare=$(echo ${fastqFileFieldsToCompare%%.*} | awk -F '_' '{print $1"_"$2"_"$3"_"$4"_"$5}')
	
	echo $fastqFileFieldsToCompare
	
	readGroupLine=$(
	# Skips the first line.
	tail +2 $samplesheet | while read line
	do
		# echo: prints the line
		# awk:  reads the fields seperated by a _, rearanges it for comparison and returns only the needed fields
		#       for comparison (with field 5 starting with an L)
		sampleSheetFieldsToCompare=$(echo $line | awk -F ',' '{print $10"_"$4"_"$11"_"$12"_L"$13}')
		echo $sampleSheetFieldsToCompare
		
		# If a match is found, prints a readgroupline and stops looking for any other matching lines.
		if [ "$fastqFileFieldsToCompare" == "$sampleSheetFieldsToCompare" ]
		then
			# For a description of each number, see the expected format of a samplesheet in the function main.
			echo $line | awk -F ',' '{print "@RG\tID:"$13"\tPL:illumina\tLB:"$10"_"$4"_"$11"_"$12"_L"$13"\tSM:"$2}'
		fi
	done)
}

main $@
