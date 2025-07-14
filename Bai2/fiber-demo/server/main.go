package main

import (
	// "container/list"
	// "errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type IncreaseInfo interface {
	GetFieldName() string
}

type Count1 struct{}
type Count2 struct{}
type Count3 struct{}
type Count4 struct{}
type Count5 struct{}

func (c Count1) GetFieldName() string {
	return "count_1"
}

func (c Count2) GetFieldName() string {
	return "count_2"
}

func (c Count3) GetFieldName() string {
	return "count_3"
}

func (c Count4) GetFieldName() string {
	return "count_4"
}

func (c Count5) GetFieldName() string {
	return "count_5"
}

// type info_user struct {
// 	ID       uint
// 	Name     string
// 	Count1   uint      `gorm:"column:count_1"`
// 	Count2   uint      `gorm:"column:count_2"`
// 	Count3   uint      `gorm:"column:count_3"`
// 	Count4   uint      `gorm:"column:count_4"`
// 	Count5   uint      `gorm:"column:count_5"`
// 	UpdateAt time.Time `gorm:"column:update_at"`
// }

// func (info_user) TableName() string {
// 	return "info_user"
// }

type Payload struct {
	Name string `json:"name"`
	Type uint   `json:"type"`
}

// Hàm kết nối database
func ConnectDB() *gorm.DB {
	dsn := "root:@tcp(127.0.0.1:3306)/l2_request_counter?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Không thể kết nối đến MySQL: %v", err)
	}

	// Tối ưu hóa connection pool
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("Không thể lấy underlying sql.DB: %v", err)
	}
	
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetMaxIdleConns(50)
	sqlDB.SetConnMaxLifetime(time.Hour)

	log.Printf("Kết nối MySQL thành công!\n")
	return db
}

func ModelHandlerDB(database *gorm.DB, batch []Payload) {
	// var countNumber int
	var countHandlers = []IncreaseInfo{
		Count1{}, Count2{}, Count3{}, Count4{}, Count5{},
	}

	// Tạo 1 map lồng map[name]map[type]số_lượng] để đếm số lượng 1 người với loại của người đấy xuẩt hiện mấy lần trong request
	counter := map[string]map[uint]uint{} // chứa tên, loại, số lần xuất hiện
	for _, valueBatch := range batch {
		// countNumber++
		// fmt.Printf("đang xử lý %v request\n", countNumber)
		_, exists := counter[valueBatch.Name] // có tên này trong counter thì trueLaCo sẽ là true

		if !exists { // Nếu trueLaCo là Fail
			counter[valueBatch.Name] = map[uint]uint{} // Khởi tạo map con để lưu type
		}
		counter[valueBatch.Name][valueBatch.Type] += 1
	}

	// tickerTime := time.NewTicker(20 * time.Second) // đặt thời gian

	for name, typeMap := range counter { //typeMap chính là map con bên trong map[type]số_lượng] nếu muốn lấy thông tin thì phải lặp lần nữa for type, value := range typeMap{}
		for typeUser, quantity := range typeMap {
			field := countHandlers[typeUser-1].GetFieldName() // Lấy count_ đúng với đúng loại

			newData := map[string]interface{}{
				"name": name,
				field:  quantity,
			}
	
			// tương đương với lệnh sql: INSERT INTO users (name, count_1) VALUES ('name', quantity) On DUPLICATE KEY UPDATE count_1 = count_1 + quantity
			errDB := database.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "name"}}, // Trường dùng để kiểm tra xung đột
				DoUpdates: clause.Assignments(map[string]interface{}{
					field: gorm.Expr(fmt.Sprintf("%s + ?", field), quantity),
				}), // Cập nhật trường count tương ứng với loại người dùng
			}).Table("info_user").Create(newData).Error

			if errDB != nil {
				log.Printf("❌ Lỗi khi upsert user %s: %v\n", name, errDB)
			}

		}
	}
}

func GetInfoUser(bufferGet []Payload) map[string]interface{} {

	infoUser := make(map[string]map[string]interface{}) // Biến để lưu thông tin người dùng

	for index, v := range bufferGet {
		_, exists := infoUser[v.Name] // Kiểm tra xem tên người dùng đã tồn tại trong map chưa
		if !exists {
			infoUser[v.Name] = map[string]interface{}{
				"count":     0,
				"positions": []int{},        // {} nghĩa là khởi tạo slice int rỗng
				"types":     map[uint]int{}, //{} nghĩa là khởi tạo map với key và value là int rỗng
			} // Khởi tạo map con nếu chưa có
		}

		infoUser[v.Name]["count"] = infoUser[v.Name]["count"].(int) + 1
		infoUser[v.Name]["positions"] = append(infoUser[v.Name]["positions"].([]int), (index + 1)) // Lấy vị trí của người dùng trong bufferGet, index + 1 để vị trí bắt đầu từ 1 thay vì 0

		typesMap := infoUser[v.Name]["types"].(map[uint]int) // infoUser[v.name].["types"] sẽ là interface{} và làm thế này sẽ ép interface{} thành map[uint]int rồi gán vào typesMap => typesMap có kiểu map[uint]int
		typesMap[v.Type]++                                   // Tăng số lượng loại tương ứng trong map types
	}

	return map[string]interface{}{
		"total_users":    len(infoUser),
		"total_requests": len(bufferGet),
		"details":        infoUser,
	}

}

var dataChannel chan Payload       // Khai báo channel toàn cục để nhận dữ liệu từ client
var dataChannelRecent chan Payload // Khai báo channel toàn cục để nhận dữ liệu từ client
var recentSummary map[string]interface{}
var counterNumber int

// var countNumberRequest int // Biến toàn cục để đếm số lượng request đã xử lý

func main() {
	database := ConnectDB()
	muRecent := sync.Mutex{} // Khởi tạo mutex để bảo vệ biến recentSummary

	dataChannel = make(chan Payload, 2000000)       // Gán giá trị cho channel toàn cục
	dataChannelRecent = make(chan Payload, 2000000) // Gán giá trị cho channel toàn cục để nhận dữ liệu từ client

	// Khởi động worker xử lý ngầm chỉ một lần khi chương trình start
	go func() { // func xử lý ngầm chỉ chạy 1 lần để kiểm tra, insert, update đb
		fmt.Println("Hàm DB sẵn sàng")
		var mu sync.Mutex
		batchSize := 200                         // Khi đủ số lượng này sẽ đẩy đi xử lý logic trước
		semWorker := make(chan struct{}, 1000)     // Giới hạn số lượng goroutine xử lý đồng thời
		ticker := time.NewTicker(30 * time.Second) // Tạo ticker để kiểm tra mỗi 10 giây
		var buffer []Payload
		buffer = make([]Payload, 0, batchSize*2)

		for {
			select {
			case data := <-dataChannel:
				mu.Lock()
				buffer = append(buffer, data) // Dấu ... để nối tất cả phần tử của data vào buffer, ko có thì go nghĩ là đang đưa cả 1 slice Payload vào 1 slice Payload chứ không phải từng phần tử
				// fmt.Printf("Kích thước buffer là: %v\n", len(buffer))
				if len(buffer) >= batchSize {
					temp := make([]Payload, len(buffer))
					copy(temp, buffer)
					buffer = buffer[:0]
					mu.Unlock()
					
					select {
					case semWorker <- struct{}{}:
						go func() {
							defer func() { <-semWorker }()
							ModelHandlerDB(database, temp)
						}()
					default:
						// Nếu worker pool đầy, xử lý đồng bộ
						ModelHandlerDB(database, temp)
					}
				} else {
					mu.Unlock()
				}
			case <-ticker.C:
				mu.Lock()
				if len(buffer) > 0 {
					temp := make([]Payload, len(buffer))
					copy(temp, buffer)
					buffer = buffer[:0]
					mu.Unlock()
					
					select {
					case semWorker <- struct{}{}:
						go func() {
							defer func() { <-semWorker }()
							ModelHandlerDB(database, temp)
						}()
					default:
						ModelHandlerDB(database, temp)
					}
				} else {
					mu.Unlock()
				}
			}
		}
	}()

	go func() {
		fmt.Println("Hàm Recent sẵn sàng")
		tickerTimeGet := time.NewTicker(10 * time.Second)
		getMax := 1000
		var bufferGet []Payload
		bufferGet = make([]Payload, 0, getMax*2)

		for {
			select {
			case dataGet := <-dataChannelRecent:
				bufferGet = append(bufferGet, dataGet) // Dấu ... để nối tất cả phần tử của data vào bufferGet

				if len(bufferGet) == getMax {

					listInfoUsers := GetInfoUser(bufferGet) // Gọi hàm để xử lý dữ liệu
					muRecent.Lock()
					recentSummary = listInfoUsers // Lưu kết quả vào biến toàn cục recentSummary
					muRecent.Unlock()
				} else if len(bufferGet) > getMax {
					bufferGet = bufferGet[len(bufferGet)-getMax:]
					listInfoUsers := GetInfoUser(bufferGet) // Gọi hàm để xử lý dữ liệu
					muRecent.Lock()
					recentSummary = listInfoUsers // Lưu kết quả vào biến toàn cục recentSummary
					muRecent.Unlock()
				}

			case <-tickerTimeGet.C:

				if len(bufferGet) > 0 {
					muRecent.Lock()
					listInfoUsers := GetInfoUser(bufferGet) // Gọi hàm để xử lý dữ liệu
					muRecent.Unlock()
					recentSummary = listInfoUsers // Lưu kết quả vào biến toàn cục recentSummary
				}
			}
		}
	}()

	app := fiber.New(fiber.Config{
		BodyLimit:             500 * 1024 * 1024, // Cho phép tối đa 50MB cho mỗi request
		Prefork:               false,
		DisableStartupMessage: false,
		ReadTimeout:           30 * time.Second,
		WriteTimeout:          30 * time.Second,
		IdleTimeout:           120 * time.Second,
		ReadBufferSize:        131072,
		WriteBufferSize:       131072,
		// ServerHeader:          "Fiber",
		// AppName:               "Request Counter v1.0.0",
	})

	// 🔗 Route 1: Kiểm tra xem server chạy chưa
	// Health check endpoint
	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status": "ok",
			"time":   time.Now().Format(time.RFC3339),
		})
	})

	// 🔗 Route 2: Nhận request từ client
	app.Post("/receive-payload", func(c *fiber.Ctx) error {
		var data Payload
		// ✅ Phân tích JSON từ body vào struct
		err := c.BodyParser(&data)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "❌ Dữ liệu không hợp lệ",
			})
		}

		select {
		case dataChannel <- data:
			counterNumber++
			fmt.Printf("Đếm: %v\n",counterNumber)
		default:
			log.Println("❌ Channel bị đầy, bỏ qua request này")
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error": "Server đang quá tải",
			})
		}

		select {
		case dataChannelRecent <- data:
		default:
			log.Println("❌ ChannelRecent bị đầy, bỏ qua request này")
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"error": "Server đang quá tải",
			})
		}

		return c.JSON(fiber.Map{
			"message": "Đã nhận",
		})
	})

	// 🔗 Route 3: Truy xuất 100.000 người gần nhất
	app.Post("/recent-users", func(c *fiber.Ctx) error {
		muRecent.Lock()
		summaryCopy := recentSummary
		muRecent.Unlock()

		// Nếu chưa có dữ liệu nào được ghi vào recentSummary
		if summaryCopy == nil {
			return c.Status(fiber.StatusOK).JSON(fiber.Map{
				"message":        "⚠️ Chưa có dữ liệu nào được xử lý gần đây",
				"total_requests": 0,
				"total_users":    0,
				"data":           nil,
			})
		}

		// Trường hợp đã có dữ liệu
		return c.Status(fiber.StatusOK).JSON(fiber.Map{
			"message":        "✅ Truy xuất thành công",
			"total_requests": summaryCopy["total_requests"],
			"total_users":    summaryCopy["total_users"],
			"data":           summaryCopy["details"],
		})
	})

	// Lắng nghe
	fmt.Println("🚀 Server Fiber đang khởi động...")
	app.Listen(":3001")
}
