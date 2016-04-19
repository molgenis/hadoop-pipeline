#!/user/bin/env python3

import sys

def main():
	"""
	Simple scipt to make sure that each read used in the testdata has a unique name.
	sys.argv[1] is the input file, which should be using a naming format such as:
	<sequencingStartDate>_<sequencer>_<run>_<flowcell>_L<lane>_<forward/reverse>
	
	When manipulating a directory filled with multiple .tar.gz files which need to be made compatible,
	use the following command from the directory with the files that need to be adjusted:
	for file in *.fq.gz; do gunzip $file; done && for file in *.fq; \
	do python3 /path/to/TestDataCompatibilityManipulator.py $file \
	> "new_"$file; rm $file && mv "new_"$file $file && gzip $file; done
	"""
	
	laneName = sys.argv[1].split("_")[4]
	
	for counter, line in enumerate(open(sys.argv[1])):
		if counter % 4 == 0:
			lineSplits = line.split("/")
			line = lineSplits[0] + "-" + laneName + "/" + lineSplits[1]
		
		print(line, end="")

main()
