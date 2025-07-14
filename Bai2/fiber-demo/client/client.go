package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	// "golang.org/x/tools/go/analysis/passes/defers"
)

const (
	TotalRequests  = 1000000             // Tổng số request cần gửi
	ConcurrentGoroutines = 200          // Số lượng goroutine (worker) chạy song song
	BatchSize      = 1000               // Số request mỗi đợt
	BatchDelay     = 100 * time.Millisecond // Thời gian delay sau mỗi batch
	ServerURL      = "http://localhost:3001" // Địa chỉ server
)

type Payload struct {
	Name string `json:"name"`
	Type uint   `json:"type"`
}

// Tạo client tối ưu
func createHTTPClient() *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        1000,
		MaxConnsPerHost:     1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
}

// Gửi GET đến /health để kiểm tra server có online không.
func checkServerHealth(client *http.Client) bool {
	resp, err := client.Get(ServerURL + "/health")
	if err != nil {
		fmt.Printf("❌ Server health check failed: %v\n", err)
		return false
	}
	defer resp.Body.Close()
	
	if resp.StatusCode == http.StatusOK {
		fmt.Println("✅ Server health check passed")
		return true
	}
	
	fmt.Printf("❌ Server health check failed: status %d\n", resp.StatusCode)
	return false
}

func main() {

	fmt.Println("🚀 Bắt đầu test load với", TotalRequests, "requests")
	
	// Tạo HTTP client để test kết nối
	client := createHTTPClient()
	defer client.CloseIdleConnections()

	// Kiểm tra server health
	fmt.Println("🔍 Kiểm tra server health...")
	if !checkServerHealth(client) {
		fmt.Println("❌ Server không sẵn sàng. Vui lòng khởi động server trước.")
		return
	}

	// Tạo danh sách users
	users := []string{"User1", "User2", "User3", "User4", "User5"} 

	// Tạo channel cho payloads
	payloadChan := make(chan Payload, ConcurrentGoroutines*2)
	
	var wg sync.WaitGroup

	for i := 0; i < ConcurrentGoroutines; i++ {
		
		wg.Add(1)
		go func(){
			defer wg.Done()
			
			for payload := range payloadChan {
				jsonData, err := json.Marshal(payload)
		
				if err != nil {
					fmt.Printf("Lỗi: %s",err)
				return
				}

				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				//Tạo 1 yêu cầu http Post nhưng chưa gửi đi
				// Fact: http.NewRequestWithContext và client.Post có tác dụng gần như nhau nhưng client.Post cùi hơn không cấu hình được gì bù lại chỉ cần lệnh đấy là vừa tạo vừa chạy được luôn
				req, err := http.NewRequestWithContext(ctx, "POST", ServerURL+"/receive-payload", bytes.NewBuffer(jsonData))
				if err != nil {
					fmt.Printf("Lỗi: %s",err)
					return
				}

				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Connection", "keep-alive")

				// Gửi yêu cầu http Post đã tạo đến địa chỉ được ghi
				resp, err := client.Do(req)
				if err != nil {
					fmt.Printf("Lỗi: %s",err)
					return
				}
				defer resp.Body.Close()

				// Đọc toàn bộ response body để tránh memory leak
				_, err = io.ReadAll(resp.Body)
				if err != nil {
					fmt.Printf("Lỗi: %s",err)
					return
				}
			}

		}()
	}

	// Goroutine chạy ngầm
	go func() {
		defer close(payloadChan)
		
		for i := 0; i < TotalRequests; i++ {
			payload := Payload{
				Name: users[i%len(users)],
				Type: uint((i % 5) + 1),
			}
			
			payloadChan <- payload
			
			// Thêm delay nhỏ sau mỗi batch để tránh overwhelm server
			if i > 0 && i%BatchSize == 0 {
				time.Sleep(BatchDelay)
				if i%50000 == 0 {
					fmt.Printf("📊 Đã gửi %d/%d requests\n", i, TotalRequests)
				}
			}
		}
	}()

	fmt.Println("⏳ Đang chờ tất cả requests hoàn tất...")
	wg.Wait()
	fmt.Println("🎉 Hoàn tất gửi toàn bộ request.")
}