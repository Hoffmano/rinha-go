package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

const (
	MaxQueueSize          = 100000
	NumberOfWorkers       = 10
	ExternalPaymentAPIURL = "http://payment-processor-default:8080/payments"
)

// Payment representa a estrutura de dados de um pagamento.
// As tags JSON foram ajustadas para corresponder à API externa.
type Payment struct {
	CorrelationId string    `json:"correlationId"`
	Ammount       float32   `json:"amount"`
	RequestedAt   time.Time `json:"requestedAt"`
}

type PaymentSummary struct {
	TotalRequests int     `json:"totalRequests"`
	TotalAmount   float32 `json:"totalAmount"`
}

type PaymentsSummaryResponse struct {
	Default  PaymentSummary `json:"default"`
	Fallback PaymentSummary `json:"fallback"`
}

var paymentQueue chan Payment

var (
	processedPayments = make(map[string]Payment)
	mu                sync.RWMutex
)

func main() {
	paymentQueue = make(chan Payment, MaxQueueSize)

	for i := 1; i <= NumberOfWorkers; i++ {
		go worker(i, paymentQueue)
	}

	http.HandleFunc("/payment", paymentHandler)
	http.HandleFunc("/payments-summary", paymentsSummaryHandler)
	http.HandleFunc("/purge-payments", purgePaymentsHandler)

	log.Println("Servidor pronto para receber pagamentos na porta 3000.")
	http.ListenAndServe(":9999", nil)
}

func purgePaymentsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Apenas o método POST é permitido", http.StatusMethodNotAllowed)
		return
	}

	// Adquire um lock de escrita total para modificar o mapa com segurança.
	mu.Lock()
	defer mu.Unlock()

	count := len(processedPayments)
	// Reinicializa o mapa, efetivamente limpando-o.
	processedPayments = make(map[string]Payment)

	log.Printf("PURGE: %d pagamentos processados foram removidos.", count)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Todos os pagamentos processados foram removidos com sucesso."))
}

// O worker agora chama a API externa antes de salvar o pagamento.
func worker(id int, queue <-chan Payment) {
	client := &http.Client{
		Timeout: 15 * time.Second,
	}

	for payment := range queue {
		log.Printf("[Worker %d] Processando pagamento %s", id, payment.CorrelationId)

		// 1. Serializa o pagamento para o formato JSON esperado pela API externa.
		payload, err := json.Marshal(payment)
		if err != nil {
			log.Printf("[Worker %d] ERRO ao serializar pagamento %s: %v", id, payment.CorrelationId, err)
			continue // Pula para o próximo pagamento.
		}

		// 2. Cria a requisição POST para a API externa.
		req, err := http.NewRequest("POST", ExternalPaymentAPIURL, bytes.NewBuffer(payload))
		if err != nil {
			log.Printf("[Worker %d] ERRO ao criar requisição para %s: %v", id, payment.CorrelationId, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		// 3. Executa a chamada para a API externa.
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("[Worker %d] ERRO ao chamar API externa para %s: %v", id, payment.CorrelationId, err)
			continue
		}
		defer resp.Body.Close()

		// 4. Verifica se o pagamento foi processado com sucesso.
		if resp.StatusCode != http.StatusOK {
			log.Printf("[Worker %d] FALHA no processamento externo para %s. Status: %s", id, payment.CorrelationId, resp.Status)
			continue
		}

		log.Printf("[Worker %d] SUCESSO no processamento externo para %s.", id, payment.CorrelationId)

		// 5. Apenas se a chamada externa foi bem-sucedida, salva o pagamento localmente.
		mu.Lock()
		processedPayments[payment.CorrelationId] = payment
		mu.Unlock()
	}
}

func paymentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Apenas o método POST é permitido", http.StatusMethodNotAllowed)
		return
	}

	var p Payment
	// O JSON de entrada usa "ammount", então decodificamos para um mapa genérico primeiro.
	var input map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, "Corpo do pedido inválido", http.StatusBadRequest)
		return
	}

	// Mapeia os campos manualmente
	if id, ok := input["correlationId"].(string); ok {
		p.CorrelationId = id
	}
	if amt, ok := input["ammount"].(float64); ok { // JSON decodifica números como float64
		p.Ammount = float32(amt)
	}
	p.RequestedAt = time.Now()

	select {
	case paymentQueue <- p:
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("Pagamento recebido e colocado na fila para processamento."))
	default:
		http.Error(w, "Serviço sobrecarregado, tente novamente mais tarde.", http.StatusServiceUnavailable)
	}
}

func paymentsSummaryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Apenas o método GET é permitido", http.StatusMethodNotAllowed)
		return
	}

	fromParam := r.URL.Query().Get("from")
	toParam := r.URL.Query().Get("to")

	var fromTime, toTime time.Time
	var err error

	if fromParam != "" {
		fromTime, err = time.Parse(time.RFC3339, fromParam)
		if err != nil {
			http.Error(w, "Formato inválido para o parâmetro 'from'. Use RFC3339 (ex: 2020-07-10T12:34:56Z)", http.StatusBadRequest)
			return
		}
	} else {
		fromTime = time.Time{}
	}

	if toParam != "" {
		toTime, err = time.Parse(time.RFC3339, toParam)
		if err != nil {
			http.Error(w, "Formato inválido para o parâmetro 'to'. Use RFC3339 (ex: 2020-07-10T12:34:56Z)", http.StatusBadRequest)
			return
		}
	} else {
		toTime = time.Now()
	}

	var defaultSummary PaymentSummary
	var fallbackSummary PaymentSummary

	mu.RLock()
	defer mu.RUnlock()

	for _, p := range processedPayments {
		isAfterFrom := p.RequestedAt.After(fromTime) || p.RequestedAt.Equal(fromTime)
		isBeforeTo := p.RequestedAt.Before(toTime) || p.RequestedAt.Equal(toTime)

		if isAfterFrom && isBeforeTo {
			defaultSummary.TotalRequests++
			defaultSummary.TotalAmount += p.Ammount
		}
	}

	response := PaymentsSummaryResponse{
		Default:  defaultSummary,
		Fallback: fallbackSummary,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Erro ao codificar a resposta do resumo: %v", err)
		http.Error(w, "Falha ao gerar resposta JSON", http.StatusInternalServerError)
	}
}
