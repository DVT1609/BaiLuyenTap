package main

import (
	"io"
	"fmt"
	"log"
	"sync"
	"time"
	"strings"
	"net/http"
	"gorm.io/gorm"
	"gorm.io/driver/mysql"
    

)

//Struct bảng dữ liệu
type GooglePerformance struct {
    ID            uint      `gorm:"primaryKey"`
    Mcc           string
    Status        string
    Placement     string
    PlacementType string
    Impression    int
    CreatedAt     time.Time
    DeletedAt     time.Time
    AdsTxt        string `gorm:"column:ads_txt"`
}

//Struct để lấy các cột cần
type PlacementInfo struct {
    Placement     string
}

// Result lưu trữ kết quả từ mỗi yêu cầu HTTP
type Result struct {
    Placement  string
    Data       string
}

// Hàm để lấy đúng tên bảng 
func (GooglePerformance) TableName() string {
    return "google_performance"
}

// Hàm kết nối database
func ConnectDB() *gorm.DB {
    dsn := "root:@tcp(127.0.0.1:3306)/test_pro?charset=utf8mb4&parseTime=True&loc=Local"
    db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
    if err != nil {
        log.Fatalf("Không thể kết nối đến MySQL: %v", err)
    }
    log.Println("Kết nối MySQL thành công!")
    return db
}

func checkContentData(url string, maxRetries int) string{
	var body string
    client := &http.Client{}

	for i:= 0; i< maxRetries; i++ {
		resp, err := client.Get(url)
		
		if err == nil && resp.StatusCode == 200 {
			
			ct := resp.Header.Get("Content-Type")
			if strings.Contains(ct, "text/html") {
				break
			} else {

				bodyTemp, readErr := io.ReadAll(resp.Body)
				resp.Body.Close()
				if readErr != nil {
					break
				} else {

					text := strings.TrimSpace(string(bodyTemp))
					if strings.HasPrefix(strings.ToLower(text), "<!doctype") || strings.HasPrefix(strings.ToLower(text), "<html") {
						break
					}
					body = string(text)
				}
			}
		}

	} 
	return string(body)

}

func TakeContentData(database *gorm.DB) (<-chan Result, int) {
    resultsContentData := make(chan Result) // Biến trả kết quả của hàm
    var listData []PlacementInfo 
    
    err := database.Model(&GooglePerformance{}).Where("ads_txt IS NULL OR ads_txt = ''").Distinct("placement").Find(&listData).Error
    
    if err != nil {
        log.Printf("Lỗi khi lấy dữ liệu từ database: %v", err)
        } 
        
        go func(){
            defer close(resultsContentData)
               
            var wgLocal sync.WaitGroup    
            sem := make(chan struct{}, 600)

            for _,value := range listData {
                
                wgLocal.Add(1)
                sem <- struct{}{}

                go func(placement string) {
                    defer wgLocal.Done()
                    defer func() { <-sem }()
                    url := fmt.Sprintf("https://%s/ads.txt", placement)
                    content := checkContentData(url, 1)
                    if content == ""{
                        content = "ERROR" //Nếu checkContentData ko trả về giá trị có thể do domain lỗi hoặc domain trả về html thì gán bằng ERROR tránh lỗi select
                    }

                    resultsContentData <- Result{Placement: placement, Data: content}
                }(value.Placement)
            }

            wgLocal.Wait()
        }()

    return resultsContentData, len(listData) // len(listData) để lấy xem có bản ghi không nếu không có sẽ break khỏi vòng for ở main
}

// Hàm cập nhật database
func UpdateADS(jobs <- chan Result, db *gorm.DB,  wg *sync.WaitGroup){
    	defer wg.Done()
	for list := range jobs {

		errorUpdate :=db.Model(&GooglePerformance{}).Where("placement", list.Placement).Update("ads_txt", list.Data)
        
        if errorUpdate.Error != nil {
            log.Fatalf("❌ Lỗi khi cập nhật placement=%s: %v", list.Placement, errorUpdate.Error)
        }
	}

}

func main(){

    countTime := time.Now()
    database := ConnectDB()

    for  {
        listContentData, count := TakeContentData(database)
        if(count == 0 ){
            break
        }
        var wg sync.WaitGroup
        wg.Add(1)
        go UpdateADS(listContentData,database, &wg)
        wg.Wait()
    }

    fmt.Printf("Tổng thời gian: %v", time.Since(countTime))
}

// UPDATE google_performances SET ads_txt = NULL