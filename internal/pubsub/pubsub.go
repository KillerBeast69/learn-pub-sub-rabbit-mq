package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	DurableQueue SimpleQueueType = iota
	TransientQueue
)

// Export AckType so other packages can use it
type AckType int

const (
	Ack         AckType = 0
	NackRequeue AckType = 1
	NackDiscard AckType = 2
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("failed to marshal: %v", err)
	}

	err = ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        jsonBytes,
		},
	)

	return err
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
		return nil, amqp.Queue{}, fmt.Errorf("failed to open a channel: %v", err)
	}

	exchangeType := "direct"
	if exchange == routing.ExchangePerilTopic {
		exchangeType = "topic"
	}

	err = ch.ExchangeDeclare(
		exchange,
		exchangeType,
		true,  // durable
		false, // autoDelete
		false, // internal
		false, // noWait
		nil,   // args
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to declare exchange: %v", err)
	}

	isDurable := queueType == DurableQueue
	isAutoDelete := queueType == TransientQueue
	isExclusive := queueType == TransientQueue

	args := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}

	queue, err := ch.QueueDeclare(
		queueName,
		isDurable,
		isAutoDelete,
		isExclusive,
		false,
		args,
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to declare queue: %v", err)
	}

	err = ch.QueueBind(
		queue.Name,
		key,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to bind queue: %v", err)
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
	unmarshaller := func(data []byte) (T, error) {
		var val T
		err := json.Unmarshal(data, &val)
		return val, err
	}

	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		unmarshaller,
	)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("failed to declare and bind: %v", err)
	}

	msgs, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %v", err)
	}

	go func() {
		for d := range msgs {
			buffer := bytes.NewReader(d.Body)
			decoder := gob.NewDecoder(buffer)

			var val T
			err := decoder.Decode(&val)
			if err != nil {
				fmt.Printf("failed to decode message: %v\n", err)
				continue
			}

			result := handler(val)

			switch result {
			case Ack:
				d.Ack(false)
				fmt.Println("message ACKed")
			case NackRequeue:
				d.Nack(false, true)
				fmt.Println("message NACKed (requeue)")
			case NackDiscard:
				d.Nack(false, false)
				fmt.Println("message NACKed (discard)")
			}
		}
	}()

	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer

	enc := gob.NewEncoder(&buffer)

	if err := enc.Encode(val); err != nil {
		return fmt.Errorf("failed to encode data: %v", err)
	}

	err := ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        buffer.Bytes(),
		},
	)
	return err
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return fmt.Errorf("failed to declare and bind: %v", err)
	}

	msgs, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to register a consume: %v", err)
	}

	go func() {
		for d := range msgs {
			val, err := unmarshaller(d.Body)
			if err != nil {
				fmt.Printf("failed to unmarshal message: %v\n", err)
				continue
			}

			result := handler(val)

			switch result {
			case Ack:
				d.Ack(false)
				fmt.Println("message ACKed")
			case NackRequeue:
				d.Nack(false, true)
				fmt.Println("message NACKed (requeue)")
			case NackDiscard:
				d.Nack(false, false)
				fmt.Println("message NACKed (discard)")
			}
		}
	}()
	return nil
}
