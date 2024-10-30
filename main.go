package main

import (
	"log/slog"
	"os"

	"github.com/playsthisgame/binq-client/binq"
)

func main() {
	c, err := binq.Connect(&binq.Config{
		Host: "localhost",
		Port: 3000,
	})
	if err != nil {
		slog.Error("Error connecting to server", "error", err)
		os.Exit(1)
	}

	defer c.Close()

	data := []byte("test")
	err = c.Create(&data)
	if err != nil {
		slog.Error("Error writing data", "error", err)
	}
}
