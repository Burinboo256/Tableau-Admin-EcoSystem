# Tableau-Auto-Delete-Users

```mermaid
graph TD;
    Start(เริ่มต้น) --> Schedule

    Schedule --> |วันที่ 1 หรือ 16 ของทุกเดือนตอน 5 ทุ่ม| get_withdraw_users



    get_withdraw_users -->|ถ้ามีคนลาที่ลาออก|get_tableau_user
    get_tableau_user -->|นำข้อมูลคนที่ลาออกมาค้นหาใน Tableau Database| Tableau_User
    Tableau_User -->|ถ้ามี User ใน Tableau|get_workbooks
    get_workbooks  -->|ถ้าคนที่ลาออกเคยสร้าง Workbook|get_site_luid

    get_site_luid-->|กรอก User เจ้าของคนใหม่|Change_Workbook_Owner
    Change_Workbook_Owner --> get_project

    get_project --> |ถ้าคนที่ลาออกเคยสร้าง Project| list_project
    list_project --> |บันทึกไว้ในไฟล์ txt|get_datasources 

    get_datasources --> |ถ้าคนที่ลาออกเคยสร้าง Datasource| list_datasource
    list_datasource --> |บันทึกไว้ในไฟล์ txt| get_views

    get_views --> |ถ้าคนที่ลาออกเคยสร้าง View| list_view
    list_view --> |บันทึกไว้ในไฟล์ txt| insert_tableau_user_log

    insert_tableau_user_log --> |เอาข้อมูล User ที่ถูกลบไปบันทึกใน Database| delete_tableau_user

    delete_tableau_user-->|ลบข้อมูล Tableau User ที่ลาออก|End
    End --> Schedule
    
    style Start fill:#66ccff,stroke:#333,stroke-width:2px;
    style Schedule fill:#E7BDB3,stroke:#333,stroke-width:2px;
    style get_withdraw_users fill:#A1B6B4,stroke:#333,stroke-width:2px;
    style get_tableau_user fill:#B2A8B6,stroke:#333,stroke-width:2px;
    style Tableau_User fill:#807F89,stroke:#333,stroke-width:2px;
    style get_workbooks fill:#C7CEEA,stroke:#333,stroke-width:2px;
    style get_site_luid fill:#C7CEEA,stroke:#333,stroke-width:2px;
    style Change_Workbook_Owner fill:#C7CEEA,stroke:#333,stroke-width:2px;
    style get_project fill:#E4B8CA,stroke:#333,stroke-width:2px;
    style list_project fill:#E4B8CA,stroke:#333,stroke-width:2px;
    style get_datasources fill:#9DCEBD,stroke:#333,stroke-width:2px;
    style list_datasource fill:#9DCEBD,stroke:#333,stroke-width:2px;
    style get_views fill:#E4CA99,stroke:#333,stroke-width:2px;
    style list_view fill:#E4CA99,stroke:#333,stroke-width:2px;
    style insert_tableau_user_log fill:#958676,stroke:#333,stroke-width:2px;
    style delete_tableau_user fill:#958676,stroke:#333,stroke-width:2px;



    style End fill:#ff6666,stroke:#333,stroke-width:2px;
