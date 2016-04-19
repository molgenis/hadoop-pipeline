#!/user/bin/env python3
"""
Name: HadoopJobInformationRetriever.py
Usage:
	HadoopJobInformationRetriever.py <history server address:port> <job_id> <output_dir>

Example:
	HadoopJobInformationRetriever.py http://myserver.com:1234 job_0123456789012_01234 ~/output/

Description: Downloads jobhistory information so it can be used locally for further analyzing.
This script should be used in a terminal that has already been authenticated using kerberos to a Hadoop cluster
(using the "kinit" command). If the output directory path does not exists, creates it.

See also https://hadoop.apache.org/docs/r2.6.0/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/HistoryServerRest.html
for more information about the MapReduce History Server REST API's (Hadoop v2.6.0).
"""

import sys
import os
import json
import requests # installation: see http://docs.python-requests.org/en/master/user/install/#install (use pip3!)
from requests_kerberos import HTTPKerberosAuth # installation: pip3 install git+https://github.com/requests/requests-kerberos

def main():
	"""
	Name:
		main
		
	Info:
		Runs the main application.
	"""
	
	# Generate required variables from user-input.
	historyServerAddress = sys.argv[1]
	basicPath = historyServerAddress.rstrip('/') + '/ws/v1/history/mapreduce/jobs/'
	jobId = sys.argv[2]
	outputDir = sys.argv[3].rstrip('/') + '/' + jobId + '/'
	
	# Generates the output directory.
	createOutputDirectory(outputDir)
	
	# Retrieve and writes job info.
	jobInfo = downloadData(basicPath + jobId)
	writeToFile(outputDir, 'job_info.json', jobInfo.text)
	
	# Retrieve and writes job counters.
	jobCounters = downloadData(basicPath + jobId + '/counters')
	writeToFile(outputDir, 'job_counters.json', jobCounters.text)
	
	# Retrieve and writes tasks info belonging to the job.
	tasks = downloadData(basicPath + jobId + '/tasks')
	writeToFile(outputDir, 'tasks_info.json', tasks.text)
	
	# Retrieve and writes for each task belonging to the job.
	for task in tasks.json()['tasks']['task']:
		taskId = task['id']
		
		# Retrieve and writes task info.
		taskInfo = downloadData(basicPath + jobId + '/tasks/' + taskId)
		writeToFile(outputDir, taskId + '_info.json', taskInfo.text)
		
		# Retrieve and writes task counters.
		taskCounters = downloadData(basicPath + jobId + '/tasks/' + taskId + '/counters')
		writeToFile(outputDir, taskId + '_counters.json', taskCounters.text)
		
		# Retrieve and writes task attempts.
		taskAttempts = downloadData(basicPath + jobId + '/tasks/' + taskId + '/attempts')
		writeToFile(outputDir, taskId + '_attempts.json', taskAttempts.text)

def downloadData(address):
	"""
	Name:
		downloadData
		
	Info:
		Downloads data defined by address using requests and raises an error
		if a bad request was made. Uses Kerberos authentication for connecting.
	
	Input:
		address - String: Web address for data to be retrieved.
	"""
	
	data = requests.get(address, auth=HTTPKerberosAuth())
	data.raise_for_status()
	return data

def createOutputDirectory(directory):
	"""
	Name:
		createOutputDirectory
		
	Info:
		Checks if the path exists. If not, creates it (recursively).
	
	Input:
		directory - String: Path on OS.
	"""
	
	if not os.path.exists(directory):
		os.makedirs(directory)

def writeToFile(outputDir, fileName, text):
	"""
	Name:
		writeToFile
		
	Info:
		Writes text to a file.
	
	Input:
		outputDir - String: The output directory.
		fileName - String: The output file name to be used.
		text - String: The text to be written to the output file.
	"""
	
	f = open(outputDir + fileName, 'w')
	f.write(text)
	f.close()


# Only run main() automatically if this script is executed directly.
if __name__ == '__main__':
	main()
