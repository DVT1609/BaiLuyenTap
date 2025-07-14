# Bài 2
- Tạo một link request chịu tải lớn. Xây dựng các tính năng dựa trên việc lưu này.
1. Tạo một link để nhận Request từ Client payload là một json bao gồm { type, name} => đề xuất dùng fiber
type sẽ có các type count_1, count_2, count_3, count_4, count_5
2. Tạo một DB có các cột name, count_1, count_2, count_3, count_4, count_5
Mỗi khi request vào sẽ cộng số đếm cho name đấy lên theo type tương ứng với cột đó => đề xuất dùng interface
3. Yêu cầu link phải chịu được tải hàng triệu request vào cùng lúc 
4. Thêm 1 link get ra để kiểm tra trong 100000 request gần nhất. Các Name nào đang được Request và có bao nhiêu request mỗi type