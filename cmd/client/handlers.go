package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/davidw1457/learn-pub-sub-starter/internal/gamelogic"
	"github.com/davidw1457/learn-pub-sub-starter/internal/pubsub"
	"github.com/davidw1457/learn-pub-sub-starter/internal/routing"
)

func handlerPause(
	gs *gamelogic.GameState,
) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(
	gs *gamelogic.GameState,
	ch *amqp.Channel,
) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		outcome := gs.HandleMove(move)
		defer fmt.Print("> ")
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				fmt.Sprintf(
					"%s.%s",
					routing.WarRecognitionsPrefix,
					gs.Player.Username,
				),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.Player,
				},
			)
			if err != nil {
				log.Printf("handlerMove: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			fallthrough
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(
	gs *gamelogic.GameState,
	ch *amqp.Channel,
) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(row gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(row)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			fallthrough
		case gamelogic.WarOutcomeYouWon:
			err := pubsub.PublishGameLog(
				ch,
				fmt.Sprintf("%s won a war against %s", winner, loser),
				row.Attacker.Username,
			)
			if err != nil {
				return pubsub.NackRequeue
			}

			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			err := pubsub.PublishGameLog(
				ch,
				fmt.Sprintf("%s and %s resulted in a draw", winner, loser),
				row.Attacker.Username,
			)
			if err != nil {
				return pubsub.NackRequeue
			}

			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}
