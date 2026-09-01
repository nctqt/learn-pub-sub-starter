package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"

	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	jsonData, err := json.Marshal(val)
	if err != nil {
		return err
	}

	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        jsonData,
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)
}

// 'enum' construction
type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	newChannel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	var durable bool
	var autodelete bool
	var exclusive bool

	if queueType == Transient {
		durable = false
		autodelete = true
		exclusive = true
	} else {
		durable = true
		autodelete = false
		exclusive = false
	}

	table := amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}

	queue, err := newChannel.QueueDeclare(queueName, durable, autodelete, exclusive, false, table)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	err = newChannel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return newChannel, queue, nil
}

type AckType string

const (
	Ack         AckType = "Ack"
	NackRequeue AckType = "NackRequeue"
	NackDiscard AckType = "NackDiscard"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	channel, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	channel.Qos(10, 0, false)
	deliveryChan, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range deliveryChan {
			var result T
			err := json.Unmarshal(msg.Body, &result)
			if err != nil {
				log.Printf("json unmarshall error: %s", err)
				continue
			}
			ack := handler(result)
			switch ack {
			case "Ack":
				msg.Ack(false)
				log.Println("ack")
			case "NackRequeue":
				msg.Nack(false, true)
				log.Println("nack requeue")
			case "NackDiscard":
				msg.Nack(false, false)
				log.Println("nack discard")
			default:
			}
		}
	}()
	return nil
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}

	msg := amqp.Publishing{
		ContentType: "application/gob",
		Body:        buffer.Bytes(),
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	channel, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	channel.Qos(10, 0, false)
	deliveryChan, err := channel.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range deliveryChan {
			var result T
			result, err := decode[T](msg.Body)
			if err != nil {
				log.Printf("Error decoding gob message: %v\n", err)
				msg.Nack(false, false) // NackDiscard on bad payload
				continue
			}
			ack := handler(result)
			switch ack {
			case "Ack":
				msg.Ack(false)
				log.Println("ack")
			case "NackRequeue":
				msg.Nack(false, true)
				log.Println("nack requeue")
			case "NackDiscard":
				msg.Nack(false, false)
				log.Println("nack discard")
			default:
			}
		}
	}()
	return nil
}

func decode[T any](data []byte) (T, error) {
	var target T

	// here is where the decoder gets created EVERY time a message arrives:
	reader := bytes.NewReader(data)
	decoder := gob.NewDecoder(reader)

	err := decoder.Decode(&target)
	if err != nil {
		return target, err
	}

	return target, nil
}
