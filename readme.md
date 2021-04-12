## Kafka Manager

Formalises eventing between microservices using kafka.

### Motivation

There is already a wonderful library for cloud events in [go](https://github.com/cloudevents/sdk-go)
But the current state kafka client libraries in go is little chaotic. There are multiple libraries for kafka clients in go,
every library has its own advantages and disadvantages.

Cloud Event's Go SDK for Kafka is currently supported over [sarama](https://github.com/Shopify/sarama)
