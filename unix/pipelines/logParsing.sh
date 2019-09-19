z#!/usr/bin/env bash
echo "Running logParsing.sh"

# ---- APACHE ----
cd ./../data/apacheLog || exit

# -- Q1 --
echo "-- Q1 --"
# Write a pipeline that from access_log get all POST requests and displays time, clientIP, path, statusCode, size
# For example: "10/Mar/2004:12:02:59", "10.0.0.153", "/cgi-bin/mailgraph2.cgi", "200", "2987"
time=$(grep -n "POST" access_log | cut -d " " -f 4 | sed 's/\[//g' |sed 's/$/,/'| sed "\$a\n")
clientIP=$(grep -n "POST" access_log | cut -d ":" -f 2 | cut -d " " -f 1  | sed 's/$/,/'| sed "\$a\n")
path=$(grep -n "POST" access_log | cut -d " " -f 7 | sed 's/$/,/'| sed "\$a\n")
statusCode=$(grep -n "POST" access_log | cut -d " " -f 9 | sed 's/$/,/'| sed "\$a\n")
size=$(grep -n "POST" access_log | cut -d " " -f 10| sed "\$a\n")
accessData=$(paste -d "" <(echo "$time") <(echo "$clientIP") <(echo "$path") <(echo "$statusCode") <(echo "$size") | sed '$ d')
# Print accessData
echo "Access data:"
echo "$accessData"

echo "--------"
# -- Q2 --
echo "-- Q2 --"
# Write a pipeline that returns the IP and path of largest size of the response to a POST request
# so for example: "192.168.0.1,/actions/logout"
# hint: you could re-use accessData to make it easier
maxsize=$(grep -n "POST" access_log | cut -d " " -f 10 | sort -nr | head -1)
largestResponse=$(grep "$maxsize" access_log | sed 's/\ -.*.POST /\,/g' | sed 's/\ H.*//g')
echo "The largest Response was to:"
echo "$largestResponse"

echo "--------"
# -- Q3--
echo "-- Q3 --"
# Write a pipeline that returns the amount and IP of the client that did the most POST requests
# so for example: "20 192.168.0.1"
# hint: you could re-use accessData to make it easier
mostRequests=$(echo "$clientIP"| sort | uniq -c | sort -nr | head -1 | cut -d "," -f 1 | cut -c 7-)
echo "The most requests where done by:"
echo "$mostRequests"

echo "--------"

#end on start path
cd ../../pipelines/ || exit