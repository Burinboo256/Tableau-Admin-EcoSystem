# Main V3
import pyodbc
import pandas as pd
from datetime import date,timedelta,datetime
import tableauserverclient as TSC
import psycopg2
import schedule
import time
import os
from dotenv import load_dotenv
import requests
import json

ad_username = 'agentserveradmin'
# dryrun =True Test ไม่ส่งผลกับระบบ
# dryrun =False  ทำจริง
dryrun = True
debuging = True
debugad = 'test.del'
backdate = 30


# โหลดตัวแปรจากไฟล์ .env
load_dotenv()


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
workbook_count = 0
datasource_count=0
delete_users = 0
project_name = []
project_owner_luid = []
tableau_site_name  = os.getenv("tableau_site_name").split(",")
tableau_site_id  = os.getenv("tableau_site_id").split(",")


# Current Date Between - 30 day and now
current_date = date.today()
start_date = current_date - timedelta(days=backdate)
alert_text = f"วันเที่ดำเนินการ: {current_date}"



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


def get_withdraw_users():
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # real query
    query = f"SELECT DomainUser FROM [dbo].[M_HR_Employee_Edoc] where EmploymentCode = '0' and WithdrawnOrInactiveDate between '{start_date}' and '{current_date}'"
    
    # for test
    # query = f"SELECT DomainUser FROM [dbo].[M_HR_Employee_Edoc] where DomainUser like '%pawaris.kon%' "
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

def get_tableau_user(user,tableau_site_id):
    print("ค้นหา User Tableau ของคนที่ลาออก")
    tableau_user_luid = []
    tableau_user_id = []
    placeholders = ', '.join(['%s'] * len(user))

    query_user = f"select su.id,su.name,u.luid,u.id,u.site_role_id,u.site_id FROM public.system_users su inner join public.users u on su.id = u.system_user_id where u.site_id = {tableau_site_id} and su.name in ({placeholders})"
    # print(query_user,user)
    cur.execute(query_user,user)
    tableau_username = cur.fetchall()
    # ดึง ID เพื่อไป Search ใน Owner_id
    for i in tableau_username:
        tableau_user_luid.append(i[2])
        tableau_user_id.append(i[3])

    return tableau_username,tableau_user_id,tableau_user_luid

# Query Workbook of withdraw user
def get_workbooks(owner_id,tableau_site_id):
    print("ค้นหา Workbook ของคนที่ลาออก")
    workbook_id = []
    workbook_name = []
    placeholders = ', '.join(['%s'] * len(owner_id))
    query_workbook = f"SELECT w.luid,w.name,w.site_id,su.name username FROM public.workbooks w inner join public.users u on w.owner_id = u.id and w.site_id = u.site_id inner join public.system_users su on su.id = u.system_user_id where w.site_id = {tableau_site_id} and w.owner_id in ({placeholders})  "
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
def get_projects(owner_id,tableau_site_id):
    print("ค้นหา Project ของคนที่ลาออก")
    project_name = []
    project_owner_luid = []
    placeholders = ', '.join(['%s'] * len(owner_id))
    query_project  = f"SELECT p.luid,p.name,p.site_id,su.name username,u.luid FROM public.projects p inner join public.users u on p.owner_id = u.id and p.site_id = u.site_id inner join public.system_users su on su.id = u.system_user_id where p.site_id = {tableau_site_id} and p.owner_id in  ({placeholders}) "
    
    cur.execute(query_project,owner_id)
    projects = cur.fetchall()
    
    if len(projects)>0:
        print(f"จำนวน Project: {len(projects)}")
        for i in projects:
            project_name.append(i[0])
            project_owner_luid.append([4])
        
    else:
        print("ไม่มี Project ที่เป็นเจ้าของ")

    return projects,project_owner_luid,project_name
    

# Query Datasource of withdraw user
def get_datasources(owner_id,tableau_site_id):
    print("ค้นหา Datasource ของคนที่ลาออก")
    placeholders = ', '.join(['%s'] * len(owner_id))
    query_datasource  = f"SELECT d.luid,d.name,d.site_id,su.name username, d.parent_workbook_id FROM public.datasources d inner join public.users u on d.owner_id = u.id and d.site_id = u.site_id inner join public.system_users su on su.id = u.system_user_id where d.connectable is true and d.site_id = {tableau_site_id} and d.owner_id in ({placeholders})"
    cur.execute(query_datasource,owner_id)
    datasources = cur.fetchall()
    if len(datasources)>0:
        print(f"จำนวน Datasource: {len(datasources)}")
        
    else:
        print("ไม่มี Datasource ที่เป็นเจ้าของ")

    return datasources



# Query New Owner Workbook
def get_site_luid(ad_username,tableau_site_id):
    cur = conn.cursor()
    query_site_luid  = f"select u.id,u.luid,u.site_id,su.name username,s.name site_name from public.users u inner join public.system_users su on u.system_user_id = su.id and su.name = '{ad_username}' inner join sites s on s.id = u.site_id where u.site_id = {tableau_site_id} "
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

# Update owner Datasource
def update_datasource(datasource_ids,new_owner_id):
    datasource = server.datasources.get_by_id(datasource_ids)
    datasource.owner_id = new_owner_id
    datasource = server.datasources.update(datasource)

def project_list_logs(project_name,owner_name,site_name,detail):
    conn = pyodbc.connect(da_dev)
    cursor = conn.cursor()
    insert_query = '''
    INSERT INTO [da_dev].[Tableau_List_Project_logs] (project_name, owner_name, site_name, detail)
    VALUES (?, ?, ?, ?)
    '''
    
    data = (project_name,owner_name,site_name,detail)
    cursor.execute(insert_query, data)
    conn.commit()



def prepare_delete(tableau_user_luid,project_owner_luid):
    if project_owner_luid is None:
        return tableau_user_luid
    
    owner_id=list(set(project_owner_luid))
    for i in owner_id:
        tableau_user_luid.remove(i)
    return tableau_user_luid


# Delete User tableau and insert
# log จะมี 2 record ต่อ 1 user id อันนึงเป็น get มาว่าจะลบอะไรอันนึงเป็น result หลังลบแล้วว่า success มั้ย 
def delete_tableau_user(tableau_user_luid,site_name):
    delete_users = 0 
    conn = pyodbc.connect(da_dev)
    cursor = conn.cursor()
    insert_query = '''
    INSERT INTO [da_dev].[Tableau_Delete_User_Logs] (username, detail,site_name)
    VALUES (?, ?, ?)
    '''
    for i in tableau_user_luid:
        
        users = server.users.get_by_id(i)
        
        if dryrun:
            print(f"Dry Run Delete User ID: {users.id} , Username: {users.name} , Role: {users.site_role} , Site: {site_name} ")
            delete_users +=1
            print(delete_users)
        else:
            server.users.remove(i)
            log_txt = f"Delete User ID: {users.id} , Username: {users.name} , Role: {users.site_role} , Site: {site_name}"
            print(log_txt)
            data = (users.name,log_txt,site_name)
            cursor.execute(insert_query, data)
            conn.commit()
            delete_users +=1
            print(delete_users)
    cursor.close()
    conn.close()
    return delete_users

# เพิ่มวันที่การ Run ในข้อความแต่ละครั้ง
def alert_message(alert_text):
    if dryrun:
        print("Dry run Alert Message")
    else:
        webhook_url = os.getenv("webhook_url")
        message = {
            'text': alert_text
        }

        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.post(webhook_url, data=json.dumps(message), headers=headers)

        if response.status_code == 200:
            print('ส่งข้อความสำเร็จ')
        else:
            print('ส่งข้อความล้มเหลว')

print("เริ่มทำงานโดยเริ่มจากดึงข้อมูลผู้ที่ลาออก")
try:

    if debuging:
        username.append(debugad)
    else:
        username = get_withdraw_users()

    alert_text = alert_text +"\n"+ f"จำนวนผู้ที่ลาออก: {len(username)} คน"
    print(username)


except Exception as e:
    alert_text = alert_text +"\n"+ f"Error Function ดึงข้อมูลผู้ที่ลาออก {e}"
    print(alert_text)
    print(e)


if len(username) > 0 :
    
    for site_id,site_name in zip(tableau_site_id,tableau_site_name):
        site_luid = get_site_luid(ad_username,site_id)
        tableau_auth = TSC.PersonalAccessTokenAuth(os.getenv("tableau_TOKEN_NAME"), os.getenv("tableau_TOKEN_VALUE"), site_id=site_name)
        server = TSC.Server(os.getenv("tableau_server_url"), use_server_version=True)

        print(f"ตรวจสอบข้อมูล Tableau Site: {site_name}")
        alert_text = alert_text +"\n"+ "----------------------" + "\n"+f"Tableau Site: {site_name}"
        cur = conn.cursor()
        try:
            tableau_username,tableau_user_id,tableau_user_luid = get_tableau_user(username,site_id)
            print(tableau_username)
            
            alert_text = alert_text +"\n"+ f"จำนวน User ใน Tableau: {len(tableau_user_luid)} บัญชี"

        except Exception as e:
            print("Error function ดึงข้อมูลจาก Tableau User")
            alert_text = alert_text +"\n"+"Error function ดึงข้อมูลจาก Tableau User: {e}"
            print(e)


        if len(tableau_user_id) > 0 :

            try:
                workbooks,workbook_id,workbook_name = get_workbooks(tableau_user_id,site_id)
                if len(workbooks) > 0 :
                    with server.auth.sign_in(tableau_auth):
                        f = open(f"workbook.txt","a",encoding="utf-8")

                        for i in workbooks:
                            for j in site_luid:
                                if i[2] == j[2]:
                                    if dryrun:
                                        print(f"Dry run Workbook วันที่: {current_date} , ID: {i[0]} , Workbook: {i[1]} เปลี่ยนเจ้าของจาก {i[3]} เป็น {j[3]}\n")
                                    else:
                                        update_workbook(i[0],j[1])
                                        print(f"Workbook วันที่: {current_date} , ID: {i[0]} , Workbook: {i[1]} เปลี่ยนเจ้าของจาก {i[3]} เป็น {j[3]}\n")
                                        log_txt = f"อัปเดต Workbook วันที่: {current_date} , ID: {i[0]} , Workbook: {i[1]} เปลี่ยนเจ้าของจาก {i[3]} เป็น {j[3]}\n"
                                        workbook_count+=1
                                        print(log_txt)
                                        f.write(log_txt)
                        f.close()
                        print(workbooks)
                        print("----------------------------------")
                    alert_text = alert_text +"\n"+ f"จำนวน Workbook : {len(workbooks)}"+"\n"+f"Workbook ที่เปลี่ยนเจ้าของสำเร็จ: {workbook_count}"
                    
            except Exception as e:
                print("Error function เปลี่ยน Owner Workbook")
                print(e)
                alert_text = alert_text +"\n"+ f"Error function เปลี่ยน Owner Workbook: {e}"



            # เอา join parent_workbook_id ออก
            try:    
                datasources = get_datasources(tableau_user_id,site_id)
                if len(datasources)>0 :
                    f = open(f"datasource.txt","a",encoding="utf-8")
                    for i in datasources:
                        for j in site_luid:
                            if i[2] == j[2]:
                                if dryrun:
                                    print(f"Dry run update วันที่: {current_date} , Datasource ID: {i[0]} , Datasource Name: {i[1]}  , Owner: {i[3]}\n")
                                else:
                                    update_datasource(i[0],j[1])
                                    log_txt = f"Datasource update วันที่: {current_date} , ID: {i[0]} , Datasource Name: {i[1]}  , Owner: {i[3]}\n"                                
                                    datasource_count += 1
                                    
                        print(log_txt)
                        f.write(log_txt)
                    f.close
                    print("----------------------------------")
                    alert_text = alert_text +"\n"+ f"จำนวน Datasource: {len(datasources)}"+"\n"+f"จำนวน Datasource ที่เปลี่ยนเจ้าของสำเร็จ: {datasource_count}"
                    print(datasources)
            except Exception as e:
                print("Error function List Datasource")
                print(e)
                alert_text = alert_text +"\n"+f"Error function List Datasource: {e}"

            try:
                projects,project_owner_luid,project_name = get_projects(tableau_user_id,site_id)
                if len(projects)>0:
                    f = open(f"project.txt","a",encoding="utf-8")
                    for i in projects:
                        if dryrun:
                            print(f"Dry run Project วันที่: {current_date} , ID: {i[0]} , Project Name: {i[1]} , Owner: {i[3]}\n")
                        else:
                            log_txt = f"List Project วันที่: {current_date} , ID: {i[0]} , Project Name: {i[1]} , Owner: {i[3]}\n"
                            project_list_logs(i[1],i[3],site_name,log_txt)
                        print(log_txt)
                        f.write(log_txt)
                    f.close
                    print("----------------------------------")
                    alert_text = alert_text +"\n"+ f"จำนวน Project: {len(projects)}"
                    tableau_user_luid = prepare_delete(tableau_user_luid,project_owner_luid)
                print(projects)
            except Exception as e:
                print("Error function List Project")
                print(e)
                alert_text = alert_text +"\n"+f"Error function List Project: {e}"

            # คำสั่งลบ
            if len(tableau_user_luid)>0:
                try:

                    with server.auth.sign_in(tableau_auth):
                        print(tableau_user_id)
                        delete_users = delete_tableau_user(tableau_user_luid,site_name)
                        
                        alert_text = alert_text +"\n"+ f"จำนวน Tableau User ที่ถูกลบออก: {delete_users} บัญชี"
                        print(f"จำนวน Tableau User ที่ถูกลบออก: {delete_users} บัญชี")
                except Exception as e:
                    print("Error Function Delete")
                    print(e)
                    alert_text = alert_text +"\n"+f"Error Function Delete: {e}"

        else:
            alert_text = alert_text+"\n"+"คนที่ลาออกไม่ได้มี User ใน Tableau หรือ Site นี้"
            print("คนที่ลาออกไม่ได้มี User ใน Tableau หรือ Site นี้")

        print("------------------------")
else:
    print("ไม่มีคนที่ลาออก")
    alert_text = alert_text+"\n"+"ไม่มีคนที่ลาออก"

alert_message(alert_text)

