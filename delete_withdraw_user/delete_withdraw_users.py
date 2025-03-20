# Main V2
import pyodbc
import pandas as pd
from datetime import date,timedelta,datetime
import config
import tableauserverclient as TSC
import psycopg2
import schedule
import time
import os
from dotenv import load_dotenv

# โหลดตัวแปรจากไฟล์ .env
load_dotenv()

def job():
    username = []
    user_id = []
    tableau_user_id = []
    tableau_username = []
    tableau_user_luid = []
    owner_id =()
    workbooks = []
    workbook_name = []
    workbook_id = []
    projects = []
    site_luid = []
    views = []

    # Current Date Between - 15day and now
    current_date = date.today()
    start_date = current_date - timedelta(days=15)

    # Tableau Database 
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("tableau_database_dbname"),
            user=os.getenv("tableau_database_user"),
            password=os.getenv("tableau_database_password"),
            host=os.getenv("tableau_database_host"),
            port=os.getenv("tableau_database_port")
    )
    except Exception as e:
        print("Error Connect to Tableau Database")
        print(e)

    # SQl Server For Get Withdraw User
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={os.getenv("panda_server_address")};'
        f'DATABASE={os.getenv("panda_database")};'
        f'UID={os.getenv("panda_username")};'
        f'PWD={os.getenv("panda_password")}'
    )

    # For insert log delete User
    da_dev = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={os.getenv("da_dev_server")};'
        f'DATABASE={os.getenv("da_dev_database")};'
        f'UID={os.getenv("da_dev_username")};'
        f'PWD={os.getenv("da_dev_password")}'
    )

    tableau_auth = TSC.PersonalAccessTokenAuth(os.getenv("tableau_TOKEN_NAME"), os.getenv("tableau_TOKEN_VALUE"), site_id=os.getenv("tableau_site"))
    server = TSC.Server(os.getenv("tableau_server_url"), use_server_version=True)


    def get_withdraw_users():
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # real query
        query = f"SELECT DomainUser FROM [dbo].[M_HR_Employee_Edoc] where EmploymentCode = '0' and WithdrawnOrInactiveDate between '{start_date}' and '{current_date}'"
        
        # for test
        # query = f"SELECT 'test.del' DomainUser UNION select 'test.del2' DomainUser "
        
        
        # Substring Domain With draw User
        cursor.execute(query)
        for row in cursor.fetchall():
            name = row[0]  # ดึงค่าจาก column แรก
            result = name.split("\\")[-1]
            username.append(result)
        print(f"จำนวนผู้ที่ลาออก: {len(username)}")
        cursor.close()
        conn.close()
        return username

    # Query User Tableau by withdraw username
    def get_tableau_user(user):
        print("ค้นหา User Tableau ของคนที่ลาออก")
        placeholders = ', '.join(['%s'] * len(user))

        query_user = f"select su.id,su.name,u.luid,u.id FROM public.system_users su inner join public.users u on su.id = u.system_user_id where su.name in ({placeholders})"
        # print(query_user,user)
        cur.execute(query_user,user)
        tableau_username = cur.fetchall()
        # ดึง ID เพื่อไป Search ใน Owner_id
        for i in tableau_username:
            tableau_user_luid.append(i[2])
            tableau_user_id.append(i[3])
        print(tableau_user_id)

        return tableau_username,tableau_user_id

    # Query Workbook of withdraw user
    def get_workbooks(owner_id):
        print("ค้นหา Workbook ของคนที่ลาออก")
        placeholders = ', '.join(['%s'] * len(owner_id))
        query_workbook = f"SELECT w.luid,w.name,w.site_id,su.name username FROM public.workbooks w inner join public.users u on w.owner_id = u.id and w.site_id = u.site_id inner join public.system_users su on su.id = u.system_user_id where w.owner_id in ({placeholders})"
        cur.execute(query_workbook,owner_id)
        # print(query_workbook)
        workbooks = cur.fetchall()

        if len(workbooks)>0:
            for i in workbooks:
                workbook_id.append(i[0])
                workbook_name.append(i[1])
            print(f"จำนวน Workbook: {len(workbook_name)}")
            # print(workbook_name)
            
        else:
            print("ไม่มี Workbook ที่เป็นเจ้าของ")
        return workbooks,workbook_id,workbook_name
    # Query Project of withdraw user
    def get_projects(owner_id):
        print("ค้นหา Project ของคนที่ลาออก")
        placeholders = ', '.join(['%s'] * len(owner_id))
        query_project  = f"SELECT p.luid,p.name,p.site_id,su.name username FROM public.projects p inner join public.users u on p.owner_id = u.id and p.site_id = u.site_id inner join public.system_users su on su.id = u.system_user_id where p.owner_id in  ({placeholders})"
        cur.execute(query_project,owner_id)
        projects = cur.fetchall()
        
        if len(projects)>0:
            print(f"จำนวน Project: {len(projects)}")
            
        else:
            print("ไม่มี Project ที่เป็นเจ้าของ")
        return projects
    # Query Datasource of withdraw user
    def get_datasources(owner_id):
        print("ค้นหา Datasource ของคนที่ลาออก")
        placeholders = ', '.join(['%s'] * len(owner_id))
        query_datasource  = f"SELECT d.luid,d.name,d.site_id,su.name username, d.parent_workbook_id, w.name workbook_name FROM public.datasources d inner join public.users u on d.owner_id = u.id and d.site_id = u.site_id inner join public.system_users su on su.id = u.system_user_id inner join workbooks w on d.parent_workbook_id = w.id where d.owner_id in ({placeholders})"
        cur.execute(query_datasource,owner_id)
        datasources = cur.fetchall()
        if len(datasources)>0:
            print(f"จำนวน Datasource: {len(datasources)}")
            
        else:
            print("ไม่มี Datasource ที่เป็นเจ้าของ")

        return datasources

    # Query View of withdraw user
    def get_views(owner_id):
        print("ค้นหา View ของคนที่ลาออก")
        placeholders = ', '.join(['%s'] * len(owner_id))
        query_view  = f"select v.luid,v.name,v.site_id,su.name username ,v.workbook_id, w.name workbook_name from views v inner join public.users u on v.owner_id = u.id and v.site_id = u.site_id inner join public.system_users su on su.id = u.system_user_id inner join workbooks w on v.workbook_id = w.id where v.owner_id in ({placeholders})"
        cur.execute(query_view,owner_id)
        views = cur.fetchall()
        if len(views)>0:
            print(f"จำนวน View: {len(views)}")
        else:
            print("ไม่มี View ที่เป็นเจ้าของ")

        return views


    # Query New Owner Workbook
    def get_site_luid(username):
        
        query_site_luid  = f"select u.id,u.luid,u.site_id,su.name username from public.users u inner join public.system_users su on u.system_user_id = su.id and su.name = '{username}'"
        cur.execute(query_site_luid)
        site_luid = cur.fetchall()

        if len(site_luid)==0:
            print("ไม่เจอผู้ใช้งาน")
        
        return site_luid


    # Update owner workbook
    def update_workbook(workbook_ids,new_owner_id):
        workbook = server.workbooks.get_by_id(workbook_ids)
        workbook.owner_id = new_owner_id
        workbook = server.workbooks.update(workbook)

    # Delete User tableau and insert 
    def delete_tableau_user(tableau_user_luid):
        conn = pyodbc.connect(da_dev)
        cursor = conn.cursor()
        insert_query = '''
        INSERT INTO [da_dev].[Tableau_Delete_User_Logs] (username, detail)
        VALUES (?, ?)
        '''
        for i in tableau_user_luid:
            users = server.users.get_by_id(i)
            log_txt = f"Delete User ID: {users.id} , Username: {users.name} , Role: {users.site_role}"
            print(log_txt)
            data = (users.name,log_txt)
            cursor.execute(insert_query, data)
            conn.commit()
            server.users.remove(i)
        cursor.close()
        conn.close()



    print("เริ่มทำงานโดยเริ่มจากดึงข้อมูลผู้ที่ลาออก")
    try:
        username = get_withdraw_users()
        print(username)
    except Exception as e:
        print("Error Function ดึงข้อมูลผู้ที่ลาออก")
        print(e)
    if len(username) > 0 :
        cur = conn.cursor()
        try:
            tableau_username,tableau_user_id = get_tableau_user(username)
        except Exception as e:
            print("Error function ดึงข้อมูลจาก Tableau User")
            print(e)


        if len(tableau_user_id) > 0 :
            try:
                workbooks,workbook_id,workbook_name = get_workbooks(tableau_user_id)
                # Update workbook จริง
                if len(workbooks) > 0 :

                    while True:
                        try:
                            ad_username = input("ใส่รหัส AD เพื่อเปลี่ยนเจ้าของ Workbook: ")
                            site_luid = get_site_luid(ad_username)
                            print("Username ที่เป็นเจ้าของใหม่ Workbook: " + ad_username)
                            if len(site_luid) == 0 :
                                print("ไม่พบ Username ใน Tableau กรุณากรอกใหม่")
                                print(len(site_luid))
                                continue
                            break
                        except Exception as e:
                            print(e)


                    with server.auth.sign_in(tableau_auth):
                        f = open(f"workbook.txt","a",encoding="utf-8")
                        f.write(f"วันเวลา: {datetime.now()}\n")
                        for i in workbooks:
                            for j in site_luid:
                                # print(j[2])
                                if i[2] == j[2]:
                                    update_workbook(i[0],j[1])
                                    log_txt = f"อัปเดต Workbook ID: {i[0]} , Workbook: {i[1]} เปลี่ยนเจ้าของจาก {i[3]} เป็น {j[3]}\n"
                                    print(log_txt)
                                    f.write(log_txt)
                        f.close()
                        print("----------------------------------")
            except Exception as e:
                print("Error function เปลี่ยน Owner Workbook")
                print(e)


            try:
                projects = get_projects(tableau_user_id)
                if len(projects)>0:
                    f = open(f"project.txt","a",encoding="utf-8")
                    f.write(f"วันเวลา: {datetime.now()}\n")
                    for i in projects:
                        log_txt = f"Project ID: {i[0]} , Project Name: {i[1]} , Owner: {i[3]}\n"
                        print(log_txt)
                        f.write(log_txt)
                    f.close
                    print("----------------------------------")
            except Exception as e:
                print("Error function List Project")
                print(e)



            try:    
                datasources = get_datasources(tableau_user_id)
                if len(datasources)>0 :
                    f = open(f"datasource.txt","a",encoding="utf-8")
                    f.write(f"วันเวลา: {datetime.now()}\n")
                    for i in datasources:
                        log_txt = f"Datasource ID: {i[0]} , Datasource Name: {i[1]} , Workbook Name: {i[5]} , Owner: {i[3]}\n"
                        print(log_txt)
                        f.write(log_txt)
                    f.close
                    print("----------------------------------")
            except Exception as e:
                print("Error function List Datasource")
                print(e)

            try:  
                views = get_views(tableau_user_id)
                if len(views)>0:
                    f = open(f"view.txt","a",encoding="utf-8")
                    f.write(f"วันเวลา: {datetime.now()}\n")
                    for i in views:
                        log_txt = f"View ID: {i[0]} , View Name: {i[1]} , Workbook: {i[5]} , Owner: {i[3]}\n"
                        print(log_txt)
                        f.write(log_txt)
                    f.close()
                    print("----------------------------------")
            except Exception as e:
                print("Error function List views")
                print(e)
            cur.close()
            conn.close()


            # คำสั่งลบ
            try:
                with server.auth.sign_in(tableau_auth):
                    delete_tableau_user(tableau_user_luid)
            except Exception as e:
                print("Error Function Delete")
                print(e)


        else:
            print("คนที่ลาออกไม่ได้มี User ใน Tableau")
            

    else:
        print("ไม่มีคนที่ลาออก")

def run_on_1_and_16():
    today = datetime.today()
    if today.day == 1 or today.day == 16:
        job()

schedule.every().day.at("23:00").do(run_on_1_and_16)
while True:
    schedule.run_pending()
    time.sleep(1)
