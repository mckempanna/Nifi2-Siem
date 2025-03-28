/opt/splunk/bin/splunk search "index=main" -output csv -maxout 1000 -auth $1:$2 > /home/mdinep/test.csv
