package main

import (
	"log/slog"

	"github.com/playsthisgame/binq-client/binq"
	"github.com/playsthisgame/binq/types"
)

func main() {
	client, err := binq.NewBinqClient(&binq.Config{Host: "localhost", Port: 3000})

	if err != nil {
		slog.Error("Error creating file:", "error", err)
	}

	queueName := "test_q"

	// create a new queue

	// client.Create(types.Queue{
	// 	Name:          queueName,
	// 	MaxPartitions: 100,
	// })

	// send 100 message to a queue

	// for i := 0; i < 100; i++ {
	// 	client.Publish(types.Message{
	// 		QueueName: queueName,
	// 		Data:      []byte(fmt.Sprintf("this is a message thats sent to binq %v", i)),
	// 	})
	// }

	// create a consumer and receive messages

	consumerClient, err := binq.NewBinqConsumerClient(client, &types.ConsumerRequest{QueueName: queueName, BatchSize: 1})

	for {
		msgs, err := consumerClient.Receive()
		if err != nil {
			slog.Error("error receiving messages", "queueName", queueName)
		}

		// ack messages
		var ids = make([]uint, len(msgs.Messages))
		for i, msg := range msgs.Messages {
			ids[i] = msg.ID
			slog.Info("messages received", "message", string(msg.Data), "id", msg.ID)
		}

		if len(ids) > 0 {
			consumerClient.Acknowledge(&types.AckMessages{
				MessageIds: ids,
			})
		}
	}

}
