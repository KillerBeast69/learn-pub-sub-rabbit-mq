package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("failed to get client welcome message: %v\n", err)
	}

	queueName := routing.PauseKey + "." + username
	_, _, err = pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.TransientQueue)
	if err != nil {
		log.Fatalf("failed to declare and bind queue: %v\n", err)
	}

	fmt.Printf("Client %s connected to queue %s\n", username, queueName)

	// Wait for interrupt signal to gracefully shutdown the server

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("shutting down server...")
}
