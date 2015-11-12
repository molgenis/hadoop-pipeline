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
	
	# Copy the correct tool archives.
	copyCorrectToolsArchive
	
	# Extract the correct binary.
	tar -zxvf tools.tar.gz tools/
	
	# Set the cirght permissions
	chmod u+x tools/bwa
	
	# Return to the original folder.
	cd ../../
}

#########################################################################
#Name:     copyCorrectToolsArchive                                      #
#Function: Copies a specific tools archive to the target/test-classes/  #
#          folder depending on the OS                                   #
#########################################################################
function copyCorrectToolsArchive {
	# Retrieve the operating system name.
	unamestr=`uname`
	
	# Extract the correct archive.
	if [[ "$unamestr" == 'Linux' ]]; then
		cp ../../../hadoop-pipeline-tools/linux_tools.tar.gz tools.tar.gz
	elif [[ "$unamestr" == 'Darwin' ]]; then
		cp ../../../hadoop-pipeline-tools/mac_tools.tar.gz tools.tar.gz
	else
		echo "Your OS not supported. Please create a tools archive yourself (for more information, view the README.md in the 'hadoop-pipeline' folder."
	fi
}

# Execute bash script.
main
