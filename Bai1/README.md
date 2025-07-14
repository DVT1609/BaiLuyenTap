# Bài 1
1. Thêm 1 cột ads.txt vào bảng này
2.
- Cột placement của bảng này sẽ là domain => mỗi một domain này e gán thêm /ads.txt phía sau
-Ví dụ: nosilk.xyz => nosilk.xyz/ads.txt
- Nó sẽ sẽ trả ra 1 content Json
3. Đọc từ Json đấy lưu nó thành dạng string trọng cột ads.txt
4. Yêu cầu xử lý Goroutine để Crawl trên nhiều Domain cùng lúc và ghi vào DB => Sao cho khi Craw hết đống domain này không được quá 30p