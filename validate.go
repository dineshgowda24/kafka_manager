package kafka_manager

import (
	"strings"

	"errors"
)

func validateSettings(s *Settings) error {
	if s == nil {
		return errors.New("Settings is nil")
	}
	if len(s.Brokers) == 0 {
		return ErrEmptyBrokers
	}
	return validateProducerSettings(s.ProducerSetting)
}

func validateProducerSettings(p *ProducerSettings) error {
	if p == nil {
		return errors.New("ProducerSettings is nil")
	}
	if p.BatchSize < 0 {
		return errors.New("Invalid batch size")
	}
	return nil
}

func validateConsumerSetting(c *ConsumerSettings) error {
	if strings.TrimSpace(c.Topic) == "" {
		return ErrEmptyTopic
	}
	if c.MaxBytes <= 0 || c.MinBytes <= 0 || c.MaxBytes <= c.MinBytes {
		return errors.New("Invalid value for min or max bytes")
	}
	if c.ReadBackoffMin >= c.ReadBackoffMax || c.ReadBackoffMin <= 0 || c.ReadBackoffMax <= 0 {
		return errors.New("Invalid value for read backoff min or max")
	}
	return nil
}
