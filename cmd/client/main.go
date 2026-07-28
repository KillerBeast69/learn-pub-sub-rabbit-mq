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

	// 1. ADDED: We need to open a channel so the client can PUBLISH messages
	publishCh, err := connection.Channel()
	if err != nil {
		log.Fatalf("failed to open publish channel: %v\n", err)
	}
	defer publishCh.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("failed to get client welcome message: %v\n", err)
	}

	gamestate := gamelogic.NewGameState(username)

	// Subscribe to Pause Messages
	pauseQueueName := routing.PauseKey + "." + username
	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilDirect,
		pauseQueueName,
		routing.PauseKey,
		pubsub.TransientQueue,
		handlerPause(gamestate),
	)
	if err != nil {
		log.Fatalf("failed to subscribe to pause messages: %v\n", err)
	}

	// 2. ADDED: Subscribe to Move Messages using the army_moves.* wildcard
	moveQueueName := routing.ArmyMovesPrefix + "." + username
	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		moveQueueName,
		routing.ArmyMovesPrefix+".*",
		pubsub.TransientQueue,
		handlerMove(gamestate),
	)
	if err != nil {
		log.Fatalf("failed to subscribe to move messages: %v\n", err)
	}

	fmt.Printf("Client %s connected and subscribed to queues\n", username)

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
			move, err := gamestate.CommandMove(words)
			if err != nil {
				fmt.Printf("Error moving unit: %v\n", err)
				continue
			}

			// 3. ADDED: Publish the move to the topic exchange!
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+username,
				move,
			)
			if err != nil {
				fmt.Printf("Error publishing move: %v\n", err)
				continue
			}
			fmt.Println("Move published successfully.")

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

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(msg routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(msg)
		return pubsub.Ack
	}
}

// 4. ADDED: A new handler function for processing moves from other players
func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(msg gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(msg)

		switch outcome {
		case gamelogic.MoveOutComeSafe, gamelogic.MoveOutcomeMakeWar:
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}
