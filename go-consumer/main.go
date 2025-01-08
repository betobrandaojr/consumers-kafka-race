package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "topic.appointments"
	brokers := []string{"localhost:9092"}
	groupID := "go-kafka-go-consumer"

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
		s := <-sigchan
		fmt.Printf("Sinal recebido: %v, finalizando...\n", s)
		cancel()
	}()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})
	defer r.Close()

	var messageCount int64
	const targetCount int64 = 1_000_000
	var startTime time.Time

	log.Println("Iniciando consumo...")

	for {
		_, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("Contexto encerrado, encerrando consumo.")
				break
			}
			log.Printf("Erro ao ler mensagem: %v\n", err)
			continue
		}

		if atomic.LoadInt64(&messageCount) == 0 {
			startTime = time.Now()
			log.Println("Recebemos a primeira mensagem. Contagem iniciada...")
		}

		newCount := atomic.AddInt64(&messageCount, 1)

		if newCount == targetCount {
			endTime := time.Now()
			totalTime := endTime.Sub(startTime)
			log.Printf("Processamos %d mensagens em %v (%.2f seg).",
				targetCount, totalTime, totalTime.Seconds(),
			)
		}
	}

	log.Println("Finalizando aplicação.")
}
