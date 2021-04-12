package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dineshgowda24/kafka_manager"
	"github.com/segmentio/kafka-go"
)

func main() {
	manager := kafka_manager.NewKafkaManager()
	manager.AddProducer(kafka_manager.DefaultProducerSetting("ping_pong"))
	c := kafka_manager.DefaultConsumerSetting("ping_pong")
	c.Callback = handleMsg(manager)
	manager.AddConsumer(c)
	manager.Consume()
}

func handleMsg(m *kafka_manager.KafkaManager) kafka_manager.Callback {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := m.Produce(ctx, "ping_pong", "", []byte("Ping..."))
	if err != nil {
		panic(err)
	}
	return func(reader *kafka.Reader, wg *sync.WaitGroup) {
		count := 0
		for {
			count += 1
			//time.Sleep(time.Second * 2)
			if count > 100000 {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err = m.Produce(ctx, "ping_pong", "", []byte("Ping..."))
			if err != nil {
				panic(err)
			}

			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				panic(err)
			}
			fmt.Println(fmt.Sprintf("Received a new msg with partition [%d] offset [%d] : %s", msg.Partition, msg.Offset, string(msg.Value)))
		}
	}
}
