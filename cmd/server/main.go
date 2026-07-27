package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connectionString := "amqp://guest:guest@127.0.0.1:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("failed to connect to rabbitMQ: %v\n", err)
	}
	defer connection.Close()
	fmt.Println("successfully connected to RabbitMQ server")

	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v\n", err)
	}
	defer ch.Close()
	fmt.Println("successfully opened a channel")

	gamelogic.PrintServerHelp()

	q, err := ch.QueueDeclare(
		"game_logs",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to declare a queue: %v\n", err)
	}
	fmt.Printf("successfully declared a queue: %s\n", q.Name)

	err = ch.QueueBind(
		q.Name,
		routing.GameLogSlug,
		routing.ExchangePerilTopic,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to bind a queue: %v\n", err)
	}
	fmt.Printf("successfully bound queue %s to exchange %s with routing key %s\n", q.Name, routing.ExchangePerilTopic, routing.GameLogSlug)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		command := words[0]
		switch command {
		case "pause":
			fmt.Println("Sending pause message...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true},
			)
			if err != nil {
				log.Printf("could not publish time: %v", err)
			}
		case "resume":
			fmt.Println("Sending resume message...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false},
			)
			if err != nil {
				log.Printf("could not publish time: %v", err)
			}
		case "quit":
			fmt.Println("exiting...")
			return
		default:
			fmt.Println("I don't understand that command.")
		}
	}
}
