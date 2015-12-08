#!/bin/env bash

#########################################################################
# Name:     ArchiveGenerator.sh                                         #
# Function: Create a new up-to-date external files archive.             #
#                                                                       #
# Usage:    ReadGroupLineGenerator.sh                                   #
#########################################################################


#########################################################################
# Name:     main                                                        #
# Function: Executes the script.                                        #
#########################################################################
function main {
	# Define archive name.
	archive="hadoop-pipeline_$(date "+%Y-%m-%d").tar"
	
	# Generate file that will store the content of the archive.
	date "+# VERSION: %Y-%m-%d" > external_files.txt;
	
	# Create tar with (almost) all files and store output in the content file.
	tar --exclude .DS_Store --exclude hadoop-pipeline-application/src/test/resources/character_replacer/ \
	-cvf ${archive} \
	hadoop-pipeline-application/src/test/resources/* \
	hadoop-pipeline-halvade-testing/data/* \
	hadoop-pipeline-tools/* \
	archive_files_info.md \
	2>> external_files.txt
	
	# Add an extra line defining that the content file is also added.
	echo "a archive_files.txt" >> external_files.txt
	
	# Create a copy of the content file (with a different name) that will be added to the archive.
	cp external_files.txt archive_files.txt
	
	# Add the content file to the archive.
	tar -rf ${archive} archive_files.txt
	
	# Gzip the archive.
	gzip ${archive}
}

main
