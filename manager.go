package kafka_manager

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaManager struct {
	sync.Mutex
	wg              *sync.WaitGroup
	producers       map[string]*kafka.Writer // Topic to Writer map
	consumers       map[string]*kafka.Reader // Topic to Reader map
	producerSetting []*ProducerSetting       // Producer Setting for Topic
	consumerSetting []*ConsumerSetting       // Consumer Setting for Topic
	initialized     bool                     // Indicates if manager is running
}

type Callback func(reader *kafka.Reader, wg *sync.WaitGroup)

type ProducerSetting struct {
	Topic        string             // Topic name
	Compression  kafka.Compression  // Message Compression type
	RequiredAcks kafka.RequiredAcks // Acknowledgement type
	BatchTimeout time.Duration      // Flush queued after batch timeout
	BatchSize    int                // Number of msgs in a batch
	Brokers      []string           // List of brokers
}

type ConsumerSetting struct {
	Topic          string        // Topic name
	GroupID        string        // Group ID of the consumer
	Callback       Callback      // Callback handler for processing msg
	CommitInterval time.Duration // Commit messages after
	Brokers        []string      // List of brokers
}

func DefaultProducerSetting(topic string) *ProducerSetting {
	return &ProducerSetting{
		Brokers:      []string{"localhost"},
		Compression:  kafka.Snappy,
		RequiredAcks: kafka.RequireAll,
		BatchSize:    10,
		BatchTimeout: 20 * time.Millisecond,
		Topic:        topic,
	}
}

func DefaultConsumerSetting(topic string) *ConsumerSetting {
	return &ConsumerSetting{
		Brokers:        []string{"localhost"},
		Topic:          topic,
		CommitInterval: 0,
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

func (f *KafkaManager) AddConsumer(c *ConsumerSetting) error {
	if f.initialized {
		// Can not add while consumers are already setup
		return ErrKafkaManagerAlreadyInitialized
	}

	if err := validateConsumerSetting(c); err != nil {
		return err
	}
	f.consumerSetting = append(f.consumerSetting, c)
	if err := f.initConsumer(c); err != nil {
		return err
	}
	return nil
}

func validateConsumerSetting(c *ConsumerSetting) error {
	if strings.TrimSpace(c.Topic) == "" {
		return ErrEmptyTopic
	}
	return nil
}

func (f *KafkaManager) initConsumer(c *ConsumerSetting) error {
	_, ok := f.consumers[c.Topic]
	if ok {
		return ErrTopicAlreadyInitialized
	}
	f.consumers[c.Topic] = getNewReader(c)
	return nil
}

func (f *KafkaManager) AddProducer(p *ProducerSetting) error {
	if f.initialized {
		// Can not add while consumers are already setup
		return ErrKafkaManagerAlreadyInitialized
	}
	if err := validateProducerSetting(p); err != nil {
		return err
	}
	f.producerSetting = append(f.producerSetting, p)
	if err := f.initProducer(p); err != nil {
		return err
	}
	return nil
}

func validateProducerSetting(p *ProducerSetting) error {
	if strings.TrimSpace(p.Topic) == "" {
		return ErrEmptyTopic
	}
	if len(p.Brokers) == 0 {
		p.Brokers = []string{"localhost"}
	}
	return nil
}

func (f *KafkaManager) initProducer(p *ProducerSetting) error {
	_, ok := f.producers[p.Topic]
	if ok {
		return ErrTopicAlreadyInitialized
	}
	f.producers[p.Topic] = getNewWriter(p)
	return nil
}

func (f *KafkaManager) Produce(ctx context.Context, topic string, key, msg []byte) (bool, error) {
	f.Lock()
	defer f.Unlock()
	w, ok := f.producers[topic]
	if !ok {
		//return topic not found error
		return false, ErrTopicNotInitialized
	}
	if err := w.WriteMessages(ctx, kafka.Message{
		Key:   key,
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
	for _, c := range f.consumerSetting {
		reader, ok := f.consumers[c.Topic]
		if ok {
			f.wg.Add(1)
			go c.Callback(reader, f.wg)
		}
	}
	f.wg.Wait()
}

func (f *KafkaManager) Close(grace time.Timer) {
	go func() {
		<-grace.C
		for _, p := range f.producers {
			p.Close()
		}
		for _, c := range f.consumers {
			c.Close()
		}
	}()
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
