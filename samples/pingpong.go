package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dineshgowda24/kafka_manager"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

func main() {
	mgr := kafka_manager.NewKafkaManager()
	mgr.AddProducer(kafka_manager.DefaultProducerSetting("ping_pong"))
	mgr.AddConsumer(&kafka_manager.ConsumerSetting{
		Topic:    "ping_pong",
		GroupID:  "ping_pong_grp",
		Brokers:  []string{"localhost"},
		Callback: handleMsg(mgr),
	})
	// blocking call
	mgr.Consume()
}

func handleMsg(m *kafka_manager.KafkaManager) kafka_manager.Callback {

	logrus.SetFormatter(&logrus.TextFormatter{}) // Do not use this in production
	lgr := logrus.StandardLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lgr.Info("Started producing 1st msg")
	_, err := m.Produce(ctx, "ping_pong", "", []byte("Ping Pong..."))
	if err != nil {
		panic(err)
	}
	return func(reader *kafka.Reader, wg *sync.WaitGroup) {
		count := 0
		for {
			count += 1
			if count > 100 {
				break
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err = m.Produce(ctx, "ping_pong", "", []byte("Ping Pong..."))
			if err != nil {
				lgr.WithError(err).Fatal(err)
			}

			msg, err := reader.FetchMessage(context.Background())
			if err != nil {
				lgr.WithError(err).Fatal(err)
			}
			lgr.Info(fmt.Sprintf("Received a new msg with partition [%d] offset [%d] : %s", msg.Partition, msg.Offset, string(msg.Value)))

			if err := reader.CommitMessages(context.Background(), msg); err != nil {
				lgr.WithError(err).Fatal(err)
			}
		}
		wg.Done()
	}
}
