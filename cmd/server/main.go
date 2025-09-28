package main

import (
    "fmt"
    "log"
    "os"
    "os/signal"

    amqp "github.com/rabbitmq/amqp091-go"

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
    
    pubsub.PublishJSON(
        ch,
        routing.ExchangePerilDirect,
        routing.PauseKey,
        routing.PlayingState{
            IsPaused: true,
        },
    )

    signalChan := make(chan os.Signal, 1)
    signal.Notify(signalChan, os.Interrupt)
    <-signalChan

    fmt.Println("Signal received. Shutting down server...")
}
