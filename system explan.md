# Giải Thích Hệ Thống Data Pipeline — 2 Giai Đoạn

---

## TỔNG QUAN

```
╔═══════════════════════════════════════════════════════════════╗
║  GIAI ĐOẠN 1: LOAD DỮ LIỆU VÀO DATA LAKE                   ║
║  Eventsim → Kafka → Spark Streaming → GCS (Data Lake)        ║
╠═══════════════════════════════════════════════════════════════╣
║  GIAI ĐOẠN 2: ELT — TỪ DATA LAKE ĐẾN BÁO CÁO               ║
║  GCS → Airflow(Load) → BigQuery → Airflow(dbt Transform)    ║
║       → Staging → Core(Star Schema) → Marts(BI)             ║
╚═══════════════════════════════════════════════════════════════╝
```

---

# GIAI ĐOẠN 1: LOAD DỮ LIỆU VÀO DATA LAKE

> Thu thập hành vi người dùng theo thời gian thực, làm sạch sơ bộ, lưu vào GCS.

### 1.1 — Eventsim (Nguồn)
Giả lập ứng dụng nghe nhạc, phát sinh **6 loại sự kiện**: listen, page_view, auth, session, status_change, user_info — mỗi sự kiện là bản ghi JSON.

### 1.2 — Kafka (Bộ đệm phân tán)
Nhận tất cả sự kiện, phân loại vào 6 Topic riêng. **Tách rời** nguồn và xử lý — Spark crash thì dữ liệu vẫn an toàn trong Kafka.

### 1.3 — Spark Streaming (Sơ chế)
Đọc micro-batch mỗi 60s từ Kafka → giải mã JSON → ép kiểu → lọc rác → chuẩn hóa timestamp → ghi Parquet ra GCS. **Checkpoint** đảm bảo exactly-once.

### 1.4 — GCS Data Lake
Lưu file Parquet phân vùng theo `month/day/hour` → sẵn sàng cho Giai đoạn 2.

---

# GIAI ĐOẠN 2: ELT — TỪ DATA LAKE ĐẾN BÁO CÁO

> Nạp dữ liệu từ GCS vào BigQuery, biến đổi nghiệp vụ bằng dbt, tạo báo cáo BI.

### 2.1 — Extract & Load (Airflow Producer DAGs)

6 DAG producer (`load_{event}_producer`) chạy **mỗi giờ**, mỗi DAG xử lý 1 loại sự kiện:

```
Kiểm tra GCS có file mới? → Tạo External Table → Tạo bảng đích → INSERT → Xóa External Table tạm
```

Khi hoàn thành, mỗi DAG **phát tín hiệu** (Dataset outlet) báo "dữ liệu event X đã sẵn sàng".

### 2.2 — Transform (dbt Consumer DAG)

File [dbt_transformation_consumer.py](file:///home/duythai/grad_project_2026/orchestration/airflow/dags/dbt_transformation_consumer.py) — DAG này dùng cơ chế **Dataset-Aware Scheduling**: chỉ chạy khi **cả 6 DAG producer** đều hoàn thành (cả 6 Dataset outlets được cập nhật).

**Luồng thực thi tuần tự — 5 bước:**

```
① dbt seed (state_codes)     — Nạp dữ liệu tĩnh (bảng mã tiểu bang)
        │
        ▼
② dbt run (tag:layer_stg)    — Chạy tầng Staging: dọn dẹp, dedup, cast
        │
        ▼
③ dbt snapshot (app_user_profile__s2) — Chụp snapshot SCD Type 2 cho user
        │
        ▼
④ dbt run (tag:layer_ods)    — Chạy tầng Core: Star Schema, Fact + Dimension
        │
        ▼
⑤ dbt run (tag:layer_rpt)    — Chạy tầng Marts: Báo cáo tổng hợp cho BI
```

**Tại sao phải chạy tuần tự?**
- Staging phải xong trước → vì Core phụ thuộc vào Staging
- Snapshot phải chạy **giữa** Staging và Core → vì Core cần `app_users__s2` (SCD Type 2 từ snapshot)
- Marts phải chạy cuối → vì nó tổng hợp từ Core

**3 tầng dữ liệu bên trong dbt:**

| Tầng | Tag | Vai trò | Ví dụ |
|---|---|---|---|
| **Staging** | `layer_stg` | Dọn dẹp kỹ thuật: rename, cast, dedup | `stg_listen_events__fa` |
| **Core** | `layer_ods` | Mô hình Star Schema: Dim + Fact + Surrogate Key | `app_users__s2`, `app_listen_events__fa` |
| **Marts** | `layer_rpt` | Báo cáo tổng hợp sẵn cho BI | `f_user_daily_activity` |

---

# PHẦN VẤN ĐÁP — CHUẨN BỊ TRẢ LỜI HỘI ĐỒNG

---

## CÂU HỎI VỀ KAFKA

### Q: "Tại sao cần Kafka? Sao không để Eventsim ghi thẳng vào GCS?"

> **Trả lời:** *"Nếu ghi thẳng vào GCS, khi lưu lượng đột biến (ví dụ giờ cao điểm), hệ thống ghi file sẽ bị nghẽn và có thể mất dữ liệu. Kafka đóng vai trò bộ đệm phân tán — nó nhận tất cả sự kiện ngay lập tức, giữ an toàn, và cho phép Spark đọc theo tốc độ của mình. Ngoài ra, Kafka tách rời nguồn dữ liệu và hệ thống xử lý — nếu Spark bị lỗi, dữ liệu vẫn nằm trong Kafka chờ xử lý lại. Đây gọi là nguyên tắc decoupling trong kiến trúc phân tán."*

### Q: "Tại sao chia thành 6 Topic riêng biệt thay vì 1 Topic chung?"

> **Trả lời:** *"Mỗi loại sự kiện có schema khác nhau — listen_events có trường song, artist; auth_events có trường success, method. Nếu gom vào 1 Topic, Spark phải parse và phân loại lại rất phức tạp. Khi chia 6 Topic, mỗi luồng Spark xử lý song song và độc lập — nếu 1 luồng lỗi, 5 luồng còn lại không bị ảnh hưởng."*

---

## CÂU HỎI VỀ SPARK

### Q: "Micro-batch 60 giây nghĩa là gì? Tại sao không xử lý từng sự kiện một (true real-time)?"

> **Trả lời:** *"Micro-batch nghĩa là Spark gom các sự kiện trong 60 giây thành một lô nhỏ rồi xử lý cùng lúc, thay vì xử lý từng sự kiện một. Lý do là ghi file Parquet — mỗi lần ghi tạo 1 file, nếu ghi từng sự kiện sẽ tạo hàng nghìn file nhỏ mỗi phút, gây ra vấn đề 'small file problem' làm chậm hệ thống khi đọc lại. 60 giây là khoảng cân bằng giữa độ trễ chấp nhận được và hiệu suất ghi file."*

### Q: "Exactly-once semantics hoạt động thế nào?"

> **Trả lời:** *"Spark ghi lại offset (vị trí đã đọc đến đâu trong Kafka) vào file checkpoint trên GCS. Ví dụ Spark đã đọc đến offset 1000 rồi crash. Khi khởi động lại, nó đọc checkpoint, thấy offset 1000, và tiếp tục từ offset 1001. Do đó không có sự kiện nào bị đọc 2 lần hay bị bỏ sót. Trong code em set `failOnDataLoss=False` để Spark không crash nếu Kafka đã xóa message cũ, mà chỉ tiếp tục từ offset hiện có."*

### Q: "Schema on Read là gì?"

> **Trả lời:** *"Dữ liệu từ Kafka đến dưới dạng binary — Spark không biết bên trong chứa gì. Em định nghĩa schema bằng StructType trong file `schema.py`, rồi dùng hàm `from_json` để ép dữ liệu binary vào schema đó. Nếu dữ liệu không khớp schema → trả về null → bị lọc ra. Đây gọi là 'Schema on Read' vì schema được áp dụng tại thời điểm đọc, không phải tại thời điểm ghi."*

---

## CÂU HỎI VỀ AIRFLOW

### Q: "6 DAG producer hoạt động cụ thể thế nào?"

> **Trả lời:** *"Em tạo 6 DAG bằng vòng lặp for — mỗi event một DAG riêng. Mỗi giờ, DAG kiểm tra xem trên GCS có file mới cho khung giờ đó không (ví dụ month=3/day=15/hour=14). Nếu có, nó tạo External Table — đây là cách BigQuery 'trỏ' trực tiếp vào file trên GCS mà không copy dữ liệu. Sau đó chạy INSERT SELECT để chuyển dữ liệu từ External Table vào bảng staging chính, rồi xóa External Table tạm. Bước ShortCircuitOperator ở đầu đảm bảo nếu không có file mới thì DAG dừng ngay, không chạy phí."*

### Q: "Consumer DAG (dbt_transformation_dag) được trigger bằng cách nào?"

> **Trả lời:** *"Em dùng cơ chế Dataset-Aware Scheduling của Airflow 2.x. Mỗi DAG producer, khi hoàn thành task cuối cùng, phát tín hiệu thông qua `outlets` — đánh dấu Dataset `bq://project/dataset/event` đã được cập nhật. Consumer DAG lắng nghe cả 6 Dataset này. Khi TẤT CẢ 6 Dataset đều được cập nhật, Airflow tự động trigger Consumer DAG chạy dbt. Cơ chế này đảm bảo dbt chỉ chạy khi dữ liệu của cả 6 loại sự kiện đã sẵn sàng trên BigQuery."*

### Q: "Tại sao Consumer DAG chia thành 5 task tuần tự mà không chạy 1 lệnh `dbt run` duy nhất?"

> **Trả lời:** *"Vì dbt có dependency giữa các tầng. Staging phải xong trước để Core đọc được. Đặc biệt, bước Snapshot (SCD Type 2) phải chạy SAU Staging nhưng TRƯỚC Core — vì Core cần bảng `app_users__s2` đã được cập nhật snapshot mới nhất. Nếu chạy `dbt run` một cục, dbt tự giải quyết dependency nhưng sẽ KHÔNG chạy snapshot (vì snapshot là lệnh riêng). Do đó em tách ra: seed → stg → snapshot → ods → rpt, đảm bảo đúng thứ tự."*

---

## CÂU HỎI VỀ DBT

### Q: "SCD Type 2 hoạt động thế nào trong hệ thống?"

> **Trả lời:** *"SCD Type 2 lưu lịch sử thay đổi của người dùng. Ví dụ user Thái đổi gói từ Free sang Paid ngày 15/03. Bảng snapshot sẽ có 2 dòng: dòng 1 là Free (valid_from: 01/01, valid_to: 15/03), dòng 2 là Paid (valid_from: 15/03, valid_to: null). Khi JOIN với bảng Fact listen_events, em dùng điều kiện `event_datetime >= row_effective_datetime AND event_datetime < row_expiry_datetime` — nhờ đó biết chính xác user ở trạng thái nào TẠI THỜI ĐIỂM sự kiện xảy ra. Nếu không có SCD Type 2, mọi sự kiện cũ sẽ bị gán nhầm trạng thái hiện tại."*

### Q: "Surrogate Key là gì? Tại sao cần?"

> **Trả lời:** *"Surrogate Key là khóa thay thế, được tạo bằng cách hash các cột tự nhiên. Ví dụ `listen_event_key` = hash(event_datetime + user_id + song + artist + duration + location). Lý do cần: dữ liệu nguồn không có primary key tự nhiên đáng tin cậy — userId có thể trùng nếu xét nhiều thời điểm khác nhau. Surrogate Key đảm bảo mỗi bản ghi có ID duy nhất, và nó ổn định qua các lần chạy (cùng input → cùng hash → cùng key)."*

### Q: "Incremental loading hoạt động thế nào?"

> **Trả lời:** *"Thay vì xóa toàn bộ bảng rồi load lại, em dùng incremental: lấy MAX(event_datetime) trong bảng hiện tại, chỉ load các bản ghi có event_datetime lớn hơn. Ví dụ bảng hiện tại có dữ liệu đến ngày 14/03, lần chạy mới chỉ load từ 15/03 trở đi. Bảng Fact dùng chiến lược `insert_overwrite` — ghi đè theo partition ngày, tránh trùng lặp. Bảng User dùng chiến lược `merge` với unique_key là user_id — nếu user đã tồn tại thì UPDATE, chưa có thì INSERT."*

### Q: "Tại sao staging có bước dedup?"

> **Trả lời:** *"Dữ liệu streaming có thể bị trùng do nhiều nguyên nhân: Kafka retry, Spark reprocess khi checkpoint bị lỗi, hoặc Eventsim gửi trùng. Ở bảng `stg_users__fu`, em dùng ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY tf_sourcing_at DESC) để chỉ giữ bản ghi mới nhất cho mỗi user_id. Đây là hàng rào bảo vệ đầu tiên trước khi dữ liệu vào Core."*

---

## CÂU HỎI VỀ KIẾN TRÚC TỔNG THỂ

### Q: "Tại sao chọn mô hình EtLT thay vì ETL truyền thống?"

> **Trả lời:** *"ETL truyền thống biến đổi nặng TRƯỚC khi load vào warehouse — tốn tài nguyên máy chủ riêng. EtLT của em chỉ biến đổi nhẹ (t nhỏ) ở Spark — lọc rác, ép kiểu — rồi load thẳng vào BigQuery. Biến đổi nghiệp vụ nặng (T lớn) chạy TRONG BigQuery bằng dbt. Tận dụng sức mạnh MPP (Massively Parallel Processing) của BigQuery — nó có thể quét hàng triệu dòng trong vài giây. Kết quả: nhanh hơn, rẻ hơn, và dễ bảo trì hơn vì logic nghiệp vụ nằm trong SQL dbt thay vì code Spark phức tạp."*

### Q: "Nếu Kafka sập thì sao? Nếu Spark sập thì sao?"

> **Trả lời:** *"Kafka sập: Eventsim không gửi được sự kiện → dữ liệu mất trong khoảng thời gian Kafka chưa khởi động lại. Nhưng Kafka cluster có replication — nếu 1 broker sập, broker khác vẫn hoạt động. Spark sập: dữ liệu vẫn an toàn trong Kafka (Kafka giữ message theo retention policy). Khi Spark khởi động lại, đọc checkpoint và tiếp tục từ đúng chỗ dừng. Airflow sập: các DAG dừng lại, nhưng dữ liệu trên GCS và BigQuery không bị ảnh hưởng. Khi Airflow khởi động lại, catchup=True sẽ chạy bù các run bị lỡ."*

### Q: "Tại sao dùng Terraform?"

> **Trả lời:** *"Thay vì lên Google Cloud Console click tạo từng VM, Bucket, Dataset bằng tay — dễ sai, khó tái tạo — em dùng Terraform viết code mô tả toàn bộ hạ tầng. Chạy `terraform apply` một lần là tạo xong: VM cho Kafka, VM cho Airflow, Dataproc cluster cho Spark, GCS bucket cho Data Lake, BigQuery dataset cho Warehouse. Nếu cần dựng lại từ đầu hoặc dựng môi trường mới, chỉ cần chạy lại lệnh đó."*

### Q: "Tại sao chọn Parquet thay vì CSV hay JSON?"

> **Trả lời:** *"Parquet là định dạng columnar — lưu dữ liệu theo cột thay vì theo dòng. Khi query chỉ cần 3 cột trong bảng 20 cột, BigQuery chỉ đọc 3 cột đó → nhanh hơn và rẻ hơn rất nhiều so với CSV phải scan toàn bộ dòng. Ngoài ra Parquet tự nén dữ liệu (compression) và giữ thông tin kiểu dữ liệu (schema embedded) — không cần define schema khi load."*

---

## CÂU HỎI VỀ DATASET-AWARE SCHEDULING

### Q: "Dataset-Aware Scheduling khác gì so với cron schedule thông thường?"

> **Trả lời:** *"Với cron, em phải đoán: 'DAG producer chạy mất khoảng 10 phút, vậy schedule DAG consumer sau 15 phút'. Nếu producer chạy lâu hơn dự kiến → consumer chạy khi dữ liệu chưa sẵn sàng → lỗi. Dataset-Aware Scheduling giải quyết vấn đề này: consumer KHÔNG chạy theo giờ cố định mà chờ đến khi cả 6 producer phát tín hiệu hoàn thành. Đây là cơ chế event-driven — đảm bảo dbt luôn chạy trên dữ liệu đầy đủ nhất."*
