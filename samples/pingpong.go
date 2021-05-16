package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dineshgowda24/kafka_manager"
	"github.com/segmentio/kafka-go"
)

func main() {
	mgr , err:= kafka_manager.New(kafka_manager.DefaultSettings())
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		_, err := mgr.Produce(context.Background(), "ping_pong_v2", "", []byte("Ping Pong..."))
		if err != nil {
			log.Fatal(err)
		}
	}

	cb := func(reader *kafka.Reader, wg *sync.WaitGroup) {
		log.Println("Started to consume")
		count := 0
		for {
			count += 1
			if count > 100 {
				break
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := mgr.Produce(ctx, "ping_pong_v1", "", []byte("Ping Pong..."))
			if err != nil {
				log.Fatal(err)
			}
		}
		for {
			msg, err := reader.FetchMessage(context.Background())
			if err != nil {
				log.Fatal(err)
			}
			log.Println(fmt.Sprintf("Received a new msg with partition [%d] offset [%d] : %s", msg.Partition, msg.Offset, string(msg.Value)))

			cCtx, cCancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cCancel()
			if err := reader.CommitMessages(cCtx, msg); err != nil {
				log.Fatal(err)
			}
		}
		wg.Done()
	}

	consumerSettings := kafka_manager.DefaultConsumerSettings("ping_pong_v2", cb)
	consumerSettings.GroupID = "ping_pong_grp_v12"
	if err := mgr.AddConsumer(consumerSettings); err != nil {
		log.Fatal("failed adding consumer")
	}
	mgr.Consume() // blocking call
}
