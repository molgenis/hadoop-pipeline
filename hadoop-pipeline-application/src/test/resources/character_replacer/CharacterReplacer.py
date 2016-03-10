#!/user/bin/env python3

import sys

def main():
    """
    Simple script for the Java TestNG tests. Expects a String as input from stdin and converts a given character to a
    different character. The line is then written to stdout.
    """
    if len(sys.argv) != 3:
        print("invalid number of arguments. Requires 2: CharacterReplacer.py <character to replace> <character to replace with>")
    else:
        for line in sys.stdin:
            sys.stdout.write(str(line).replace(sys.argv[1], sys.argv[2]))

main()
