
# Kafka installation
sudo yum clean all
sudo yum install kafka
sudo yum install kafka-server

# Start Zookeeper
sudo service zookeeper-server restart

#Start Kafka bloker
sudo service kafka-server restart
 
# Create a topic on Kafka
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic books

#delete the topic
kafka-topics --zookeeper localhost:2181 --delete --topic books


# List topics
kafka-topics --list --zookeeper localhost:2181

# Run:
kafka-console-producer --broker-list localhost:9092 --topic books
kafka-console-consumer --zookeeper localhost:2181 --topic books --from-beginning

# Change permission of /tmp/hive
 sudo chmod -R 777 /tmp/hive

# start mysql
sudo service mysqld start

# run mysql 
mysql -u root -p

