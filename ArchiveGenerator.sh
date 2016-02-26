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
	# Define archive names.
	archivePrefix="hadoop-pipeline_$(date "+%Y-%m-%d")"
	completeArchive="${archivePrefix}.tar"
	toolsOnlyGzipArchive="${archivePrefix}_tools_only.tar.gz"
	
	# Only creates the new archives if neither exist yet.
	if [ -f "${completeArchive}.gz" ] || [ -f "${toolsOnlyGzipArchive}" ]
	then
		echo "Archive(s) with the current date already exist. Please (re)move it/them before creating new ones."
	else
		# Generate file that will store the content of the archive.
		date "+# VERSION: %Y-%m-%d" > external_files.txt;
		
		# Create tar with (almost) all files and store output in the content file.
		tar --exclude .DS_Store --exclude hadoop-pipeline-application/src/test/resources/character_replacer/ \
		-cvf ${completeArchive} \
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
		tar -rf ${completeArchive} archive_files.txt
		
		# Gzip the archive.
		gzip ${completeArchive}
		
		# Creates an archive with the tools only.
		tar --exclude .DS_Store -czf ${toolsOnlyGzipArchive} \
		hadoop-pipeline-tools/*
	fi
}

main
