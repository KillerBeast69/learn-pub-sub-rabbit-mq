package main

import (
	"fmt"
	"log"
	"time"

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

	// 1. We need to open a channel so the client can PUBLISH messages
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

	// Subscribe to Move Messages using the army_moves.* wildcard
	moveQueueName := routing.ArmyMovesPrefix + "." + username
	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		moveQueueName,
		routing.ArmyMovesPrefix+".*",
		pubsub.TransientQueue,
		handlerMove(gamestate, publishCh), // Pass the publish channel here!
	)
	if err != nil {
		log.Fatalf("failed to subscribe to move messages: %v\n", err)
	}

	// Subscribe to War Messages using the war.* wildcard
	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		"war",
		routing.WarRecognitionsPrefix+".*",
		pubsub.DurableQueue,
		handlerWar(gamestate, publishCh), // <-- FIX: Pass publishCh here!
	)
	if err != nil {
		log.Fatalf("failed to subscribe to war messages: %v\n", err)
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

			// Publish the move to the topic exchange!
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

// Updated handler for processing moves and detecting war
func handlerMove(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(msg gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(msg)

		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			// Publish war recognition message
			err := pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: msg.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("error publishing war recognition: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack // The madness begins here
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}

// New handler for processing war messages
func handlerWar(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(msg gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(msg)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue // Requeue so another client can pick it up
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon, gamelogic.WarOutcomeDraw:
			var logMsg string
			if outcome == gamelogic.WarOutcomeDraw {
				logMsg = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			} else {
				logMsg = fmt.Sprintf("%s won a war against %s", winner, loser)
			}

			gameLog := routing.GameLog{
				CurrentTime: time.Now(),
				Message:     logMsg,
				Username:    gs.GetUsername(),
			}

			// FIX: routing key should be GameLogSlug.username (with the dot)
			routingKey := routing.GameLogSlug + "." + msg.Attacker.Username

			err := pubsub.PublishGob(
				publishCh, // FIX: Pass the actual amqp channel
				routing.ExchangePerilTopic,
				routingKey,
				gameLog,
			)
			if err != nil {
				fmt.Printf("Failed to publish game log: %v\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Println("error: unknown war outcome")
			return pubsub.NackDiscard
		}
	}
}
