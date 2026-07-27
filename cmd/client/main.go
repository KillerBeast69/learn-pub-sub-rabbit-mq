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
	fmt.Println("Starting Peril client...")

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

	gamestate := gamelogic.NewGameState(username)

	queueName := routing.PauseKey + "." + username

	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.TransientQueue,
		handlerPause(gamestate),
	)
	if err != nil {
		log.Fatalf("failed to subscribe to queue %s: %v\n", queueName, err)
	}

	fmt.Printf("Client %s connected to queue %s\n", username, queueName)

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			err = gamestate.CommandSpawn(words)
			if err != nil {
				fmt.Printf("Error spawning unit: %v\n", err)
			}
		case "move":
			_, err := gamestate.CommandMove(words)
			if err != nil {
				fmt.Printf("Error moving unit: %v\n", err)
				continue
			}
			fmt.Println("unit moved successfully")
		case "status":
			gamestate.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Printf("unknown command: %s\n", words[0])
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(msg routing.PlayingState) {
		defer fmt.Print("> ")
		gs.HandlePause(msg)
	}
}
