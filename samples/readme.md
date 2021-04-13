### Start Kafka Server

```bash
zookeeper-server-start config/zookeeper.properties
kafka-server-start config/server.properties
```

### Create Kafka topic

This is optional if your kafka broker is configured to automatically create topic then this step is not required.
```bash
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic ping_pong
```

### Run the program

```bash
go run pingpong.go
```