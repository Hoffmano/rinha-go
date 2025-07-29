package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const (
	host         = "database"
	port         = 5432
	user         = "postgres"
	password     = "postgres"
	databaseName = "postgres"
)

var db *sqlx.DB

var paymentsQueue = make(chan Payment, 10000)

type Payment struct {
	CorrelationID string  `json:"correlationId" db:"correlation_id"`
	Amount        float32 `json:"amount" db:"amount"`
	Processor     string  `json:"processor" db:"processor"`
	RequestedAt   string  `json:"requestedAt" db:"requested_at"`
}

type PaymentRequest struct {
	Payment
}

type PaymentProcessorSummary struct {
	TotalRequests int16   `json:"totalRequests" db:"total_requests"`
	TotalAmount   float32 `json:"totalAmount" db:"total_amount"`
}

type PaymentsSummary struct {
	Default  PaymentProcessorSummary `json:"default"`
	Fallback PaymentProcessorSummary `json:"fallback"`
}

type ProcessorRow struct {
	ProcessorName string  `db:"processor"`
	TotalRequests int16   `db:"total_requests"`
	TotalAmount   float32 `db:"total_amount"`
}

func main() {
	log.Println("starting")
	http.HandleFunc("POST /payments", postPayment)
	http.HandleFunc("GET /payments-summary", getPaymentsSummary)

	connectToDatabase()
	// TODO: I think that I'm processing my payments synchronously, I will need to made this in parallel way
	// go paymentWorker()
	numWorkers := 10      // Define how many workers you want
	var wg sync.WaitGroup // Use a WaitGroup to wait for all workers to finish

	// Start multiple workers
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1) // Increment the WaitGroup counter for each worker
		go paymentWorker(i, &wg)
	}

	log.Println("Server is up.1")
	http.ListenAndServe(":9999", nil)
}

func connectToDatabase() {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, databaseName)
	var err error
	db, err = sqlx.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	db.Exec(`
		create table if not exists payments (
			correlation_id text primary key,
			amount real
		)
	`)

	db.Exec(`
		alter table payments add column if not exists requested_at timestamp
	`)

	db.Exec(`
		alter table payments add column if not exists processor boolean
	`)
	fmt.Println("Successfully connected to database!")
}

func postPayment(w http.ResponseWriter, r *http.Request) {
	var payment Payment
	json.NewDecoder(r.Body).Decode(&payment)
	paymentsQueue <- payment
}

func paymentWorker(workerID int, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Worker %d started.", workerID)
	for payment := range paymentsQueue {
		payment.RequestedAt = time.Now().Format(time.RFC3339)
		// log.Println(payment)
		json, _ := json.Marshal(payment)
		body := bytes.NewBuffer(json)
		res, _ := http.Post("http://payment-processor-default:8080/payments", "application/json", body)
		// log.Println(res.StatusCode)
		if res.StatusCode != http.StatusOK {
			log.Println(res.StatusCode)
			log.Println("fallback")
			res, _ := http.Post("http://payment-processor-fallback:8080/payments", "application/json", bytes.NewBuffer(json))
			if res.StatusCode != http.StatusOK {
				log.Println(res.StatusCode)
				log.Println("return to queue")
				paymentsQueue <- payment
				return
			}
			_, err := db.NamedExec("insert into payments (correlation_id, amount, processor, requested_at) values (:correlation_id, :amount, true, :requested_at)", payment)
			if err != nil {
				log.Println(err)
			}
			return
		}
		// log.Println("insert")
		_, err := db.NamedExec("insert into payments (correlation_id, amount, processor, requested_at) values (:correlation_id, :amount, false, :requested_at)", payment)
		if err != nil {
			log.Println(err)
		}
	}
}

// func getPayments(w http.ResponseWriter, r *http.Request) {
// 	payments, _ := db.Query("select * from payments")
//
// 	var selectedPayments []Payment
// 	for payments.Next() {
// 		var payment Payment
// 		payments.Scan(&payment.CorrelationID, &payment.Amount)
// 		selectedPayments = append(selectedPayments, payment)
// 	}
// 	json, _ := json.MarshalIndent(selectedPayments, "", "  ")
// 	fmt.Fprintf(w, "%s", string(json))
// }

func getPaymentsSummary(w http.ResponseWriter, r *http.Request) {
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")

	fromTime, _ := time.Parse(time.RFC3339Nano, from)
	toTime, _ := time.Parse(time.RFC3339Nano, to)
	// log.Println(from, to, fromTime, toTime)

	var summary PaymentsSummary
	var processorRows []ProcessorRow
	err := db.Select(&processorRows, `
		select 
			processor,
			count(*) as total_requests, 
			coalesce(sum(amount),0) as total_amount 
		from payments 
		where 
			requested_at >= $1
			and requested_at <= $2
		group by processor
		`, fromTime.Format(time.DateTime), toTime.Format(time.DateTime))
	if err != nil {
		log.Println(err)
	}

	for _, row := range processorRows {
		switch row.ProcessorName {
		case "false":
			summary.Default.TotalRequests = row.TotalRequests
			summary.Default.TotalAmount = row.TotalAmount
		case "true":
			summary.Fallback.TotalRequests = row.TotalRequests
			summary.Fallback.TotalAmount = row.TotalAmount
		default:
			log.Printf("Warning: Unknown processor type '%s' encountered in database results. Skipping.", row.ProcessorName)
		}
	}

	json, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Fprintf(w, "%s", string(json))
}
