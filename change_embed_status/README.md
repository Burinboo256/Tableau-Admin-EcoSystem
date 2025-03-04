# Tableau-Admin-EcoSystem

```mermaid
graph TD;
    Start(เริ่มต้น) -->|โหลด config และเชื่อมต่อ| LoadConfig
    LoadConfig -->|ดึงข้อมูล Datasource| GetDatasource
    GetDatasource -->|วนลูปแต่ละ Datasource| LoopDatasource
    LoopDatasource -->|อัปเดต Embed Password| UpdateEmbed
    UpdateEmbed -->|ตรวจสอบสถานะหลังอัปเดต| CheckStatus
    CheckStatus -->|บันทึก Log ลง SQL Server| InsertLog
    InsertLog -->|วนลูปไปยัง Datasource ถัดไป| LoopDatasource
    InsertLog -->|ตั้งเวลาให้ทำงานทุกวันจันทร์| Schedule
    Schedule -->|สิ้นสุด| End

    style Start fill:#66ccff,stroke:#333,stroke-width:2px;
    style End fill:#ff6666,stroke:#333,stroke-width:2px;
    style LoadConfig fill:#ffff99,stroke:#333,stroke-width:2px;
    style GetDatasource fill:#ffff99,stroke:#333,stroke-width:2px;
    style LoopDatasource fill:#ffcc99,stroke:#333,stroke-width:2px;
    style UpdateEmbed fill:#ffcc99,stroke:#333,stroke-width:2px;
    style CheckStatus fill:#ffcc99,stroke:#333,stroke-width:2px;
    style InsertLog fill:#ffcc99,stroke:#333,stroke-width:2px;
    style Schedule fill:#99ff99,stroke:#333,stroke-width:2px;
