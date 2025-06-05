package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"nats-rabbitmq/fault"

	"bitbucket.org/vayana/walt-go/result"
	"github.com/streadway/amqp"
)

// this is sample implementation of rabbitmq in vayana just for understanding purpose
type conn struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

type RabbitMQConfig struct {
	host     string
	port     int
	userName string
	password string
	vhost    string
}

func NewRabbitMQConfig(host string, port int, userName string, password string, vhost string) *RabbitMQConfig {
	return &RabbitMQConfig{
		host:     host,
		port:     port,
		userName: userName,
		password: password,
	}
}

func (c *RabbitMQConfig) GetUrl() string {
	return fmt.Sprint("amqps://", c.userName, ":", c.password, "@", c.host, ":", c.port, "/", c.vhost)
}

type RabbitMQ struct {
	config         *RabbitMQConfig
	consumer       string
	autoAck        bool
	noWait         bool
	durable        bool
	connectionPtr  *conn
	deliveriesChan <-chan amqp.Delivery
}

func NewRabbitMQBuilder(
	config *RabbitMQConfig,
) *RabbitMQ {
	return &RabbitMQ{
		config:  config,
		autoAck: false,
		noWait:  false,
		durable: false,
	}
}

func (obj *RabbitMQ) WithConsumer(consumer string) *RabbitMQ {
	obj.consumer = consumer
	return obj
}

func (obj *RabbitMQ) WithAutoAck() *RabbitMQ {
	obj.autoAck = true
	return obj
}

func (obj *RabbitMQ) WithNoWait() *RabbitMQ {
	obj.noWait = true
	return obj
}

func (obj *RabbitMQ) WithDurable() *RabbitMQ {
	obj.durable = true
	return obj
}

func (obj *RabbitMQ) Connect() result.Result[bool] {
	var err error
	obj.connectionPtr = &conn{}
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // Disable certificate validation
	}
	obj.connectionPtr.connection, err = amqp.DialTLS(obj.config.GetUrl(), tlsConfig)
	if err != nil {
		return result.NewErr[bool](fault.AppConfigError(err))
	}

	obj.connectionPtr.channel, err = obj.connectionPtr.connection.Channel()
	if err != nil {
		return result.NewErr[bool](fault.RabbitConnectionError(obj.config.host, string(obj.config.port), err))
	}
	ok := true
	return result.NewOk(&ok)
}

func (obj *RabbitMQ) Send(destinationExchange string, routingKey string, message []byte) result.Result[bool] {
	if err := obj.connectionPtr.channel.Publish(
		destinationExchange, // publish to an exchange
		routingKey,          // routing to 0 or more queues
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			Headers:         nil,
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            message,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,
		},
	); err != nil {
		return result.NewErr[bool](fault.RabbitMQSendMessageError(destinationExchange, routingKey, err))
	}
	ok := true
	return result.NewOk(&ok)
}

func (obj *RabbitMQ) Consume(queueName string) result.Result[bool] {
	var err error
	obj.deliveriesChan, err = obj.connectionPtr.channel.Consume(
		queueName, //queue
		obj.consumer,
		obj.autoAck, //Auto Ack
		false,       //exclusive
		false,       //noLocal
		obj.noWait,  //noWait
		nil)
	if err != nil {
		err = errors.New(queueName + " : consume fail")
		return result.NewErr[bool](fault.RabbitMQConsumeError(queueName, obj.consumer, err))
	}
	ok := true
	return result.NewOk(&ok)
}

func (obj *RabbitMQ) Shutdown() result.Result[bool] {
	if err := obj.connectionPtr.connection.Close(); err != nil {
		return result.NewErr[bool](fault.RabbitMQShutDownError(err))
	}
	ok := true
	return result.NewOk(&ok)
}

func (obj *RabbitMQ) CheckAliveness() result.Result[bool] {
	if err := fmt.Errorf("%s", <-obj.connectionPtr.connection.NotifyClose(make(chan *amqp.Error))); err != nil {
		return result.NewErr[bool](fault.RabbitMQCheckAliveError(err))
	}
	ok := true
	return result.NewOk(&ok)
}

func (obj *RabbitMQ) GetDeliveryChannel() <-chan amqp.Delivery {
	return (<-chan amqp.Delivery)(obj.deliveriesChan)
}
