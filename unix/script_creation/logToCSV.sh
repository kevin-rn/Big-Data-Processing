#!/usr/bin/env bash
# This program should take an fileIn as first parameter
# It takes the input log file that has the same format as access_log and maps it to a csv format
# the csv format is:
# Client, Time, Type, Path, Status, Size
#
# The program should not create a csv file.
# This can be done by piping the output to a file.
# example: './logToCSV access_log > output.csv'
# It could take some time to convert all of access_log. Consider using a small subset for testing.

client=$(cat "$1" | cut -d ":" -f 1 | cut -d " " -f 1  | sed 's/$/,/'| sed "\$a\n")
time=$(cat "$1" | cut -d " " -f 4 | sed 's/\[//g' | sed 's/$/,/' | sed "\$a\n")
type=$(cat "$1" | cut -d " " -f 6 | sed 's/\"//g' | sed 's/$/,/' | sed "\$a\n")
path=$(cat "$1" | cut -d " " -f 7 | sed 's/$/,/'| sed "\$a\n")
statusCode=$(cat "$1" | cut -d " " -f 9 | sed 's/$/,/'| sed "\$a\n")
size=$(cat "$1" | cut -d " " -f 10| sed "\$a\n")
accessData=$(paste -d "" <(echo "$client") <(echo "$time") <(echo "$type") <(echo "$path") <(echo "$statusCode") <(echo "$size") | sed '$ d')
echo "$accessData"

cd .. || exit