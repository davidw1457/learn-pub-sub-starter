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
	fmt.Println("Starting Peril client...")

	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	fmt.Println("Connection successful...")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalln(err)
	}

	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, userName),
		routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		log.Fatalln(err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("Signal received. Shutting down server...")
}
