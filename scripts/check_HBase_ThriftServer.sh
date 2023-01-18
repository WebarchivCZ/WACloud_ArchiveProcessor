# Script for checking if HBase ThriftServer is running. If not, the script will try to start it.

# All communication between the application and HBase is managed by HBase ThriftServer, 
# which is not running in the Ambari by default. After each reboot of the namenode, 
# it must be started manually (e.g. by running this script).

if ps aux | grep "hbase.thrift.ThriftServer" | grep -v grep >/dev/null 2>&1; then 
    echo "Everything OK, HBase ThriftServer is running."
else
    echo "HBase ThriftServer is not running. Trying to start it..."
    sudo -u hbase /usr/hdp/3.1.4.0-315/hbase/bin/hbase-daemon.sh start thrift -p 9099
fi
