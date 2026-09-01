package main

import (
	"fmt"
	"log"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	const rabbitMQServer = "amqp://guest:guest@localhost:5672/"

	serverConn, err := amqp.Dial(rabbitMQServer)
	if err != nil {
		log.Fatalf("Error connecting to server: %v", err)
	}
	defer serverConn.Close()

	fmt.Println("Server connection success!")

	newChannel, err := serverConn.Channel()
	if err != nil {
		log.Fatalf("Error creating channel: %v", err)
	}

	err = pubsub.SubscribeGob(serverConn, routing.ExchangePerilTopic, "game_logs", "game_logs.*", pubsub.Durable, handlerPrintLogs())
	if err != nil {
		log.Fatalf("Error on subscribe gob: %v", err)
	}

	gamelogic.PrintServerHelp()

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "pause":
			fmt.Println("pausing...")
			err = pubsub.PublishJSON(newChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("Error publishing json: %v", err)
			}
		case "resume":
			fmt.Println("resuming...")
			err = pubsub.PublishJSON(newChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("Error publishing json: %v", err)
			}
		case "quit":
			fmt.Println("quitting...")
			os.Exit(0)
		default:
			fmt.Println("invalid command")
		}
	}
}

func handlerPrintLogs() func(routing.GameLog) pubsub.AckType {
	return func(gamelog routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gamelog)
		if err != nil {
			log.Printf("Error writing game log: %v", err)
			return pubsub.NackRequeue
		}
		return pubsub.Ack
	}
}
