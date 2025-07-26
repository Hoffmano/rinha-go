package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq"
)

const (
	host         = "database"
	port         = 5432
	user         = "postgres"
	password     = "postgres"
	databaseName = "postgres"
)

var db *sql.DB

var paymentsQueue = make(chan Payment, 10000)

type Payment struct {
	CorrelationID string  `json:"correlationId"`
	Amount        float32 `json:"amount"`
}

type PaymentRequest struct {
	Payment
	RequestedAt time.Time `json:"requestedAt"`
}

func main() {
	http.HandleFunc("POST /payments", postPayment)
	http.HandleFunc("GET /payments-summary", getPaymentsSummary)

	connectToDatabase()
	// FIX: I think that I'm processing my payments synchronously, I will need to made this in parallel way
	go paymentWorker()

	log.Println("Server is up.")
	http.ListenAndServe(":9999", nil)
}

func connectToDatabase() {
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, databaseName)
	var err error
	db, err = sql.Open("postgres", psqlInfo)
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

	fmt.Println("Successfully connected to database!")
}

func postPayment(w http.ResponseWriter, r *http.Request) {
	var payment Payment
	json.NewDecoder(r.Body).Decode(&payment)
	paymentsQueue <- payment
}

func paymentWorker() {
	for payment := range paymentsQueue {
		var paymentRequest PaymentRequest
		paymentRequest.RequestedAt = time.Now()
		paymentRequest.Payment = payment
		json, _ := json.Marshal(paymentRequest)
		body := bytes.NewBuffer(json)
		response, err := http.Post("http://payment-processor-default:8080/payments", "application/json", body)
		db.Exec("insert into payments (correlation_id, amount) values ($1, $2)", payment.CorrelationID, payment.Amount)
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
}
