package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/streadway/amqp"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("🔁 Enter messages to send to RabbitMQ (type 'exit' to quit):")
	sendingFromTerminal(reader, ch, q)

	// sendingLargeNumberOfMessages(ch, q, 100000)

}
func sendingFromTerminal(reader *bufio.Reader, ch *amqp.Channel, q amqp.Queue) {
	for {
		fmt.Print(">> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalf("Error reading input: %v", err)
		}

		input = strings.TrimSpace(input)

		if input == "exit" {
			fmt.Println("Exiting producer...")
			break
		}

		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key (queue name)
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(input),
			},
		)
		if err != nil {
			log.Printf("Failed to publish message: %v", err)
		} else {
			log.Printf("Sent: %s", input)
		}
	}
}
func sendingLargeNumberOfMessages(ch *amqp.Channel, q amqp.Queue, n int) {
	for i := 1; i <= n; i++ {
		body := fmt.Sprintf("Message #%d", i)
		err := ch.Publish(
			"",     // exchange
			q.Name, // routing key (queue name)
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			},
		)
		if err != nil {
			log.Printf("Failed to publish message %d: %v", i, err)
		} else {
			log.Printf("Sent: %s", body)
		}
	}
	log.Println("Finished sending %s messages", n)
}
