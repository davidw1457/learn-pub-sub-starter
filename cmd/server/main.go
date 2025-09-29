package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/davidw1457/learn-pub-sub-starter/internal/gamelogic"
	"github.com/davidw1457/learn-pub-sub-starter/internal/pubsub"
	"github.com/davidw1457/learn-pub-sub-starter/internal/routing"
)

const connStr string = "amqp://guest:guest@localhost:5672/"

func main() {
	fmt.Println("Starting Peril server...")

	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	fmt.Println("Connection successful...")

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalln(err)
	}

	gamelogic.PrintServerHelp()

game:
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case "pause":
			fmt.Println("sending pause message")

			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				log.Fatalln(err)
			}
		case "resume":
			fmt.Println("sending resume message")

			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				log.Fatalln(err)
			}
		case "quit":
			fmt.Println("exiting game")
			break game
		default:
			fmt.Printf("unknown command: %s\n", input[0])
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Signal received. Shutting down server...")
}
