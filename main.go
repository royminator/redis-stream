package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	ctx := context.Background()

	_, err := client.Ping(ctx).Result()

	if err != nil {
		log.Fatal("Unable to reach redis")
	}

	data := make(chan string)
	go reader(client, ctx, data)
	go sender(client, ctx)
	for {
		s := <-data
		fmt.Println("recv:", s)
	}
}

func reader(client *redis.Client, ctx context.Context, data chan<- string) {
	var id string = "$"
	for {
		readData(client, ctx, id, data)
	}
}

func readData(client *redis.Client, ctx context.Context, id string, data chan<- string) {
	args := redis.XReadArgs{
		Streams: []string{"commands", id},
		Block: 0,
		Count: 1,
	}
	result, err := client.XRead(ctx, &args).Result()

	if err != nil {
		panic(err)
	}

	data <- result[0].Messages[0].Values["message"].(string)
}

func sender(client *redis.Client, ctx context.Context) {
	for {
		sendData(client, ctx)
		<-time.After(time.Second * 3)
	}
}

func sendData(client *redis.Client, ctx context.Context) {
	data := map[string]interface{}{
		"email":   "redis@gmail.com",
		"message": "We have received your order and we are working on it.",
	}
	args := redis.XAddArgs{
		Stream:       "commands",
		ID:           "",
		MaxLen:       0,
		MaxLenApprox: 0,
		Values:       data,
	}
	err := client.XAdd(ctx, &args).Err()
	if err != nil {
		panic(err)
	}
}
