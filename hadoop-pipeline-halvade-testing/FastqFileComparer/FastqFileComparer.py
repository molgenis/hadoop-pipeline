#! /user/bin/env python3
"""
Name: FastqFileComparer.py
Usage:
        FastqFileComparer.py <gzipped fastq file 1> <gzipped fastq file 2>

Description: Compares 2 gzipped fastq files whether they are equal. This is done by collecting (and sorting) all
sequence headers, comparing if the number of collected sequence headers is equal and whether the actual sequence
headers are equal (as the lists are sorted, if the files are equal the headers should be in the same position in the
lists).

IMPORTANT: Under normal circumstances it can be assumed that the halvade upload tool simply works correctly. This script
is only added as extra validation for developers and should under normal circumstances not be needed. It should
therefore be noted that this script will not validate individual user input for all kinds of possible mistakes (such as
using a different/no archived files as input). Therefore Python knowledge and/or following the exact instructions
present in the README file(s) should be followed.
"""

import sys
import os
import gzip


def main():
    """
    Main class for simple fastq compare script.
    """
    if len(sys.argv) != 3:
        print("invalid number of arguments. Requires 2: FastqFileComparer.py fastq1.fastq.gz fastq2.fastq.gz")
        return

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    # Exits script prematurely if one of the input files does not exist.
    if not (checkIfFileExists(file1) and checkIfFileExists(file2)):
        print("one of the input files does not exist")
        return

    # Reads in files (returned data is ordered).
    print("### loading files ###")
    seqHeaders1 = collectSequenceHeaders(file1)
    seqHeaders2 = collectSequenceHeaders(file2)

    # Compares files and prints basic info.
    print("### comparing files ###")
    fileEquality = seqHeaders1 == seqHeaders2
    print("equal: ", fileEquality)
    print("size: ", len(seqHeaders1), " - ", len(seqHeaders2))
    for i in range(0, 10):
        # Prints first 10 headers as manual control.
        print("{:60} - {}".format(seqHeaders1[i], seqHeaders2[i]))

    # Prints differences in files.
    print("### differences ###")
    if not fileEquality:
        # Read differences not printed if number of reads differ.
        if len(seqHeaders1) != len(seqHeaders2):
            print("not shown as number of reads differ")
        else:
            for i in range(0, len(seqHeaders1) - 1):
                if seqHeaders1[i] != seqHeaders2[i]:
                    print("{:60} - {}".format(seqHeaders1[i], seqHeaders2[i]))
    else:
        print("n.a.")


def checkIfFileExists(fileLocation):
    """
    Checks if a file exists. If a file exists, returns true. Otherwise, returns false.

    :param fileLocation: String
    :return: boolean
    """
    return os.path.isfile(fileLocation)


def collectSequenceHeaders(fileLocation):
    """
    Collects the fast read headers and returns these sorted.

    :param fileLocation: String
    :return: Sorted List containing Strings
    """
    seqHeaders = []

    for counter, line in enumerate(gzip.open(fileLocation, "rb")):
        if counter % 4 == 0:
            seqHeaders.append(line.decode("utf-8").rstrip("\n"))

    return sorted(seqHeaders)


main()
