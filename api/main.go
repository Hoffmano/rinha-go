package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
	CorrelationID string  `json:"correlationId"`
	Amount        float32 `json:"amount"`
	Processor     string  `db:"processor"`
	RequestedAt   string  `json:"requestedAt"`
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

func main() {
	http.HandleFunc("POST /payments", postPayment)
	http.HandleFunc("GET /payments-summary", getPaymentsSummary)

	connectToDatabase()
	// TODO: I think that I'm processing my payments synchronously, I will need to made this in parallel way
	go paymentWorker()

	log.Println("Server is up.")
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

func paymentWorker() {
	for payment := range paymentsQueue {
		payment.RequestedAt = time.Now().Format(time.DateTime)
		// log.Println(payment)
		json, _ := json.Marshal(payment)
		body := bytes.NewBuffer(json)
		_, err := http.Post("http://payment-processor-default:8080/payments", "application/json", body)
		if err != nil {
			log.Println("fallback")
			log.Println(err)
			_, err := http.Post("http://payment-processor-fallback:8080/payments", "application/json", body)
			if err != nil {
				log.Println("return to queue")
				log.Println(err)
				paymentsQueue <- payment
				db.Exec("insert into payments (correlation_id, amount, processor, requested_at) values ($1, $2, true, $3)", payment.CorrelationID, payment.Amount, payment.RequestedAt)
				return
			}
			return
		}
		db.Exec("insert into payments (correlation_id, amount, processor, requested_at) values ($1, $2, false, $3)", payment.CorrelationID, payment.Amount, payment.RequestedAt)

	}
}

func getPayments(w http.ResponseWriter) {
	payments, _ := db.Query("select * from payments")

	var selectedPayments []Payment
	for payments.Next() {
		var payment Payment
		payments.Scan(&payment.CorrelationID, &payment.Amount)
		selectedPayments = append(selectedPayments, payment)
	}
	json, _ := json.MarshalIndent(selectedPayments, "", "  ")
	fmt.Fprintf(w, "%s", string(json))
}

func getPaymentsSummary(w http.ResponseWriter, r *http.Request) {
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")

	fromTime, _ := time.Parse(time.RFC3339Nano, from)
	toTime, _ := time.Parse(time.RFC3339Nano, to)
	// log.Println(from, to, fromTime, toTime)

	var summary PaymentsSummary
	err := db.Get(&summary.Default, `
		select 
			count(*) as total_requests, 
			coalesce(sum(amount),0) as total_amount 
		from payments 
		where 
			processor = false
			and requested_at >= $1
			and requested_at <= $2
		`, fromTime.Format(time.DateTime), toTime.Format(time.DateTime))
	if err != nil {
		log.Println(err)
	}

	json, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Fprintf(w, "%s", string(json))
}
