#! /user/bin/env python3

import sys
import fileinput


def main():
    if len(sys.argv) != 3:
        print("invalid number of arguments. Requires 2: CharacterReplacer.py <character to replace> <character to replace with>")
        return

    for line in sys.stdin:
        sys.stdout.write(str(line).replace(sys.argv[1], sys.argv[2]))

main()
