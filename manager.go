package kafka_manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaManager struct {
	sync.Mutex
	wg              *sync.WaitGroup
	producers       map[string]*kafka.Writer
	consumers       map[string]*kafka.Reader
	producerSetting []*ProducerSetting
	consumerSetting []*ConsumerSetting
	initialized     bool
	brokers         []string
}

type Callback func(reader *kafka.Reader, wg *sync.WaitGroup)

type ProducerSetting struct {
	Topic        string
	Compression  kafka.Compression
	RequiredAcks kafka.RequiredAcks
	BatchTimeout time.Duration
	BatchSize    int
	Brokers      []string
}

type ConsumerSetting struct {
	Topic          string
	GroupID        string
	Callback       Callback
	CommitInterval time.Duration
	Brokers        []string
}

func DefaultProducerSetting(topic string) *ProducerSetting {
	return &ProducerSetting{
		Brokers:      []string{"localhost"},
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireAll,
		BatchSize:    100,
		BatchTimeout: 20 * time.Millisecond,
		Topic:        topic,
	}
}

func DefaultConsumerSetting(topic string) *ConsumerSetting {
	return &ConsumerSetting{
		Brokers:        []string{"localhost"},
		CommitInterval: 10 * time.Millisecond,
		Topic:          topic,
	}
}

func NewKafkaManager() *KafkaManager {
	return &KafkaManager{
		Mutex:           sync.Mutex{},
		wg:              &sync.WaitGroup{},
		producers:       make(map[string]*kafka.Writer),
		consumers:       make(map[string]*kafka.Reader),
		producerSetting: make([]*ProducerSetting, 0, 0),
		consumerSetting: make([]*ConsumerSetting, 0, 0),
	}
}

func (f *KafkaManager) connect() {
	f.Lock()
	defer f.Unlock()
	f.setUpConsumers()
	f.setUpProducers()
	f.initialized = true
}

func (f *KafkaManager) AddConsumer(c *ConsumerSetting) error {
	if f.initialized {
		// Can not add while consumers are already setup
		return ErrKafkaManagerAlreadyInitialized
	}
	f.consumerSetting = append(f.consumerSetting, c)
	return nil
}

func (f *KafkaManager) AddProducer(c *ProducerSetting) error {
	if f.initialized {
		// Can not add while consumers are already setup
		return ErrKafkaManagerAlreadyInitialized
	}
	f.producerSetting = append(f.producerSetting, c)
	return nil
}

func (f *KafkaManager) Produce(ctx context.Context, topic, key string, msg []byte) (bool, error) {
	f.Lock()
	defer f.Unlock()
	f.setUpProducers()
	w, ok := f.producers[topic]
	if !ok {
		//return topic not found error
		return false, ErrTopicNotInitialized
	}
	if err := w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: msg,
	}); err != nil {
		er, ok := err.(*kafka.Error)
		if ok {
			return er.Temporary(), er
		}
		return false, err
	}
	return false, nil
}

func (f *KafkaManager) Consume() {
	f.connect()
	for _, c := range f.consumerSetting {
		reader, ok := f.consumers[c.Topic]
		if ok {
			f.wg.Add(1)
			go c.Callback(reader, f.wg)
		}
	}
	f.wg.Wait()
}

func (f *KafkaManager) Disconnect() {
	for _, p := range f.producers {
		p.Close()
	}
	for _, c := range f.consumers {
		c.Close()
	}
}

func (f *KafkaManager) setUpProducers() {
	if len(f.producers) == len(f.producerSetting) {
		return
	}
	for _, s := range f.producerSetting {
		_, ok := f.producers[s.Topic]
		if !ok {
			fmt.Println("Setting up new producer")
			f.producers[s.Topic] = getNewWriter(s)
		}
	}
}

func (f *KafkaManager) setUpConsumers() {
	if len(f.consumers) == len(f.consumerSetting) {
		return
	}
	for _, s := range f.consumerSetting {
		_, ok := f.consumers[s.Topic]
		if !ok {
			f.consumers[s.Topic] = getNewReader(s)
		}
	}
}

func getNewWriter(s *ProducerSetting) *kafka.Writer {
	return &kafka.Writer{
		Topic:        s.Topic,
		Compression:  s.Compression,
		BatchTimeout: s.BatchTimeout,
		BatchSize:    s.BatchSize,
		RequiredAcks: s.RequiredAcks,
		Addr:         kafka.TCP(s.Brokers...),
	}
}

func getNewReader(s *ConsumerSetting) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Topic:          s.Topic,
		GroupID:        s.GroupID,
		Brokers:        s.Brokers,
		CommitInterval: s.CommitInterval,
	})
}
