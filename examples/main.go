package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"

	"git.intra.weibo.com/adx/kafka_producer_sarama"
)

func main() {
	c := &kafka_producer_sarama.Config{
		Brokers:   []string{"10.182.29.28:19092", "10.182.29.28:29092", "10.182.29.28:39092"},
		Topic:     "my_producer_topic",
		LogOut:    os.Stdout,
		SaramaCfg: sarama.NewConfig(),
	}
	p, err := kafka_producer_sarama.NewProducer(c)
	if err != nil {
		log.Printf("Start error, err: %s\n", err.Error())
		return
	}

	ch := make(chan struct{})
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		var i int
		for {
			select {
			case <-ch:
				return
			default:
				// do something
			}
			msg := map[string]interface{}{
				"topic": c.Topic,
				"index": i,
			}
			msgJson, _ := json.Marshal(msg)
			p.Write(string(msgJson))
			log.Println(string(msgJson))
			i++
			time.Sleep(time.Millisecond * 100)
		}
	}()

	<-sigterm
	close(ch)
	wg.Wait()
	p.Close()
	log.Println("finished by signa, exit")
}
