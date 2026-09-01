package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

// general rabbitmq flow
// create a queue to process messages
// bind that queue to an exchange (router) with a key (flags for sorting)
// consumers subscribe to a queue if they want the sorted messages in that queue
// publishers push to the exchange, which sorts and pushes to queues
// a broadcast requires each consumer to have its own queue for that message type
// multiple exchanges might be used in large systems for separation of duties or permissions
// to make this all happen, channels have to be defined, they run within the tcp conn
// typically one channel for publishing and another for consuming (subscriptions)

func main() {
	fmt.Println("Starting Peril client...")

	// initial connection
	const rabbitMQServer = "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(rabbitMQServer)
	if err != nil {
		log.Fatalf("Error connecting as client: %v", err)
	}
	defer conn.Close()
	fmt.Println("Client connection success!")

	// user input
	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error setting user name: %v", err)
	}

	// game state
	state := gamelogic.NewGameState(userName)

	// the publish channel
	pubChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Error creating pub channel: %v", err)
	}

	// name queue and subscribe to it, pass a handler function for messages on that queue
	// direct exchange is for exact keys
	// subscribing handles channel creation
	// pause queue
	queueName := fmt.Sprintf("pause.%s", userName)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.Transient, handlerPause(state))
	if err != nil {
		log.Fatalf("Error subscribe json: %v", err)
	}

	// topic exchange is for keys with wildcards
	// moves queue
	queueName = fmt.Sprintf("army_moves.%s", userName)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, queueName, "army_moves.*", pubsub.Transient, handlerMove(state, pubChannel))
	if err != nil {
		log.Fatalf("Error subscribe json: %v", err)
	}

	// war queue
	queueName = "war"
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, queueName, "war.*", pubsub.Durable, handlerWarMessages(state, pubChannel))
	if err != nil {
		log.Fatalf("Error subscribe json: %v", err)
	}

	// repl
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "spawn":
			err = state.CommandSpawn(input)
			if err != nil {
				fmt.Printf("Error on spawn: %v", err)
			}
		case "move":
			move, err := state.CommandMove(input)
			if err != nil {
				fmt.Printf("Error on move: %v", err)
			}
			// key matches queuename from above
			key := fmt.Sprintf("army_moves.%s", userName)
			err = pubsub.PublishJSON(pubChannel, routing.ExchangePerilTopic, key, move)
			if err != nil {
				log.Fatalf("Error publishing json: %v", err)
			}
			fmt.Println("move successful")
		case "status":
			state.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(input) != 2 {
				fmt.Println("spam command takes format: <spam x>")
				continue
			}
			num, err := strconv.Atoi(input[1])
			if err != nil {
				log.Printf("Error converting string: %v", err)
				continue
			}
			for range num {
				gamelog := gamelogic.GetMaliciousLog()
				gl := routing.GameLog{
					CurrentTime: time.Now(),
					Message:     gamelog,
					Username:    userName,
				}
				_ = publishGameLog(pubChannel, gl)
			}
		case "quit":
			gamelogic.PrintQuit()
			os.Exit(0)
		default:
			fmt.Println("invalid command")
		}
	}
}

// handler functions
func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(mv gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(mv)
		switch moveOutcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			warKey := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername())
			warLogic := gamelogic.RecognitionOfWar{
				Attacker: mv.Player,
				Defender: gs.GetPlayerSnap(),
			}
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, warKey, warLogic)
			if err != nil {
				log.Printf("Error publishing json: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWarMessages(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			gl := routing.GameLog{
				CurrentTime: time.Now(),
				Message:     msg,
				Username:    rw.Attacker.Username,
			}
			return publishGameLog(ch, gl)
		case gamelogic.WarOutcomeYouWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			gl := routing.GameLog{
				CurrentTime: time.Now(),
				Message:     msg,
				Username:    rw.Attacker.Username,
			}
			return publishGameLog(ch, gl)
		case gamelogic.WarOutcomeDraw:
			msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			gl := routing.GameLog{
				CurrentTime: time.Now(),
				Message:     msg,
				Username:    rw.Attacker.Username,
			}
			return publishGameLog(ch, gl)
		default:
			log.Printf("Error on war handler")
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(ch *amqp.Channel, gl routing.GameLog) pubsub.AckType {
	key := fmt.Sprintf("%s.%s", routing.GameLogSlug, gl.Username)
	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, key, gl)
	if err != nil {
		log.Printf("Error publishing game log: %v", err)
		return pubsub.NackRequeue
	}
	return pubsub.Ack
}
