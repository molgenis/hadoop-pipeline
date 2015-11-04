#!/bin/env bash

#########################################################################
#Name:     TestNGPreprocessing.sh                                       #
#Function: Automation script to copy, extract and add correct rights to #
#          tool binaries used for TestNG tests.                         #
#                                                                       #
#Usage: ExpressionSitesLocatorHadoop.sh                                 #
#########################################################################


#########################################################################
#Name:     main                                                         #
#Function: Executes the script functionalites                           #
#########################################################################
function main {
	# Go to the goal folder.
	cd target/test-classes/
	
	# Copy the tool archives.
	cp -R ../../../hadoop-pipeline-tools/* ./
	
	# Extract the correct binary.
	extractCorrectToolsArchive
	
	# Set the cirght permissions
	chmod u+x tools/bwa
	
	# Return to the original folder.
	cd ../../
}

#########################################################################
#Name:     extractCorrectToolsArchive                                   #
#Function: Extracts a tool archive depending on the OS                  #
#########################################################################
function extractCorrectToolsArchive {
	# Retrieve the operating system name.
	unamestr=`uname`
	
	# Extract the correct archive.
	if [[ "$unamestr" == 'Linux' ]]; then
		tar -zxvf linux_tools.tar.gz tools/
	elif [[ "$unamestr" == 'Darwin' ]]; then
		tar -zxvf mac_tools.tar.gz tools/
	else
		echo "Your OS not supported. Please create a tools archive yourself (for more information, view the README.md in the 'hadoop-pipeline' folder."
	fi
}

# Execute bash script.
main
