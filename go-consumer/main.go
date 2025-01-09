package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type TelemetryData struct {
	ID         int64           `json:"id"`
	EventTime  time.Time       `json:"event_time"`
	InsertDate time.Time       `json:"insert_date"`
	Device     json.RawMessage `json:"device"`
	Asset      json.RawMessage `json:"asset"`
	Driver     json.RawMessage `json:"driver"`
	Details    json.RawMessage `json:"details"`
	Point      json.RawMessage `json:"point"`
	Telemetry  json.RawMessage `json:"telemetry"`
}

const (
	batchSize   = 10000
	targetCount = 1_000_000
	numWorkers  = 10 // Número de workers para processamento paralelo
)

func main() {
	topic := "topic.appointments"
	brokers := []string{"localhost:9092"}
	groupID := "go-kafka-go-consumer"

	connStr := "postgresql://postgres:postgres@localhost:5432/postgres"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Erro ao conectar ao banco de dados: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Erro ao verificar conexão com o banco: %v", err)
	}

	log.Println("Conexão com o banco de dados estabelecida.")

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
	var startTime time.Time

	batchChan := make(chan []TelemetryData, numWorkers)
	wg := &sync.WaitGroup{}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(ctx, db, batchChan, wg)
	}

	log.Println("Iniciando consumo...")

	var batch []TelemetryData
	for {
		if atomic.LoadInt64(&messageCount) >= targetCount {
			break
		}

		m, err := r.ReadMessage(ctx)
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

		var data TelemetryData
		if err := json.Unmarshal(m.Value, &data); err != nil {
			log.Printf("Erro ao deserializar mensagem: %v\n", err)
			continue
		}

		batch = append(batch, data)
		atomic.AddInt64(&messageCount, 1)

		if len(batch) == batchSize {
			batchChan <- batch
			batch = nil
		}
	}

	if len(batch) > 0 {
		batchChan <- batch
	}

	close(batchChan)
	wg.Wait()

	endTime := time.Now()
	totalTime := endTime.Sub(startTime)
	log.Printf("Finalizado! Processamos %d mensagens e as inserimos no banco em %v (%.2f seg).",
		messageCount, totalTime, totalTime.Seconds(),
	)
}

func worker(ctx context.Context, db *sql.DB, batchChan <-chan []TelemetryData, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			log.Println("Worker encerrando devido ao contexto cancelado.")
			return
		case batch, ok := <-batchChan:
			if !ok {
				//log.Println("Canal de batches fechado, encerrando worker.")
				return
			}
			if err := insertTelemetryBatch(db, batch); err != nil {
				log.Printf("Erro ao inserir lote no banco: %v\n", err)
			}
		}
	}
}

func insertTelemetryBatch(db *sql.DB, batch []TelemetryData) error {
	query := `
		INSERT INTO telemetry_data (
			id, event_time, insert_date, device, asset, driver, details, point, telemetry
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, data := range batch {
		_, err := stmt.Exec(
			data.ID,
			data.EventTime,
			data.InsertDate,
			data.Device,
			data.Asset,
			data.Driver,
			data.Details,
			data.Point,
			data.Telemetry,
		)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}
