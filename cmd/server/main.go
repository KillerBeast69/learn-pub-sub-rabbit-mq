package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	state := routing.PlayingState{
		IsPaused: true,
	}

	err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, state)
	if err != nil {
		log.Fatalf("failed to publish JSON: %v\n", err)
	}

	// Wait for interrupt signal to gracefully shutdown the server

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("shutting down server...")
}
