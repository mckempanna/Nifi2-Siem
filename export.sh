/opt/splunk/bin/splunk search "index=main" -output csv -maxout 1000 -auth root:$1 > /home/mdinep/test.csv
