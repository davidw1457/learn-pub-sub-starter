package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/davidw1457/learn-pub-sub-starter/internal/routing"
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonVal, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("publishJSON: %w", err)
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonVal,
		},
	)
	if err != nil {
		return fmt.Errorf("publishJSON: %w", err)
	}

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("declareAndBind: %w", err)
	}

	queue, err := ch.QueueDeclare(
		queueName,
		queueType == Durable,
		queueType == Transient,
		queueType == Transient,
		false,
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		},
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("declareAndBind: %w", err)
	}

	err = ch.QueueBind(
		queueName,
		key,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("declareAndBind: %w", err)
	}

	return ch, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("subscribeJSON: %w", err)
	}

	deliveryCh, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("subscribeJSON: %w", err)
	}

	go func() {
		for message := range deliveryCh {
			var body T

			err = json.Unmarshal(message.Body, &body)
			if err != nil {
				log.Printf("subscribeJSON: %s\n", err)
				continue
			}

			ack := handler(body)
			switch ack {
			case Ack:
				log.Println("Ack")
				err = message.Ack(false)
			case NackRequeue:
				log.Println("NackRequeue")
				err = message.Nack(false, true)
			case NackDiscard:
				log.Println("NackDiscard")
				err = message.Nack(false, false)
			default:
				err = fmt.Errorf("invalid AckType: %v", ack)
			}
			if err != nil {
				log.Printf("subscribeJSON: %s\n", err)
			}
		}
	}()

	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	err := encoder.Encode(val)
	if err != nil {
		return fmt.Errorf("publishGob: %w", err)
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        buf.Bytes(),
		},
	)
	if err != nil {
		return fmt.Errorf("publishGob: %w", err)
	}

	return nil
}

func PublishGameLog(ch *amqp.Channel, message, username string) error {
	err := PublishGob(
		ch,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.GameLogSlug, username),
		message,
	)
	if err != nil {
		return fmt.Errorf("publishGameLog: %w", err)
	}

	return nil
}
