# รวมไฟล์
# นำเข้า lib
import tableauserverclient as TSC
import pyodbc
import os
from dotenv import load_dotenv
import requests
import json
from datetime import date,timedelta,datetime


# โหลดตัวแปรจากไฟล์ .env
load_dotenv()
datasource_connection  = os.getenv("datasource_connection").split(",")

debug_workbook = '355d8077-28a5-470c-b8a6-639f779da6f7'
dabug_datasource = '8c5f4164-a915-4ee8-bcf9-66d1794640da'
# dryrun =True Test ไม่ส่งผลกับระบบ
# dryrun =False  ทำจริง
dryrun =False
debuging = True
workbook_id = []
datasource_id = []
workbook_name = []
datasource_name = []
detail = []
old_data = []


tableau_auth = TSC.PersonalAccessTokenAuth(os.getenv("tableau_TOKEN_NAME"), os.getenv("tableau_TOKEN_VALUE"), site_id=os.getenv("site"))
server = TSC.Server(os.getenv("tableau_server_url"), use_server_version=True)

# connection sql server ที่จะ save log
conn_str = (
    f'DRIVER={{ODBC Driver 18 for SQL Server}};'
    f'SERVER={os.getenv("da_dev_server")};'
    f'DATABASE={os.getenv("da_dev_database")};'
    f'UID={os.getenv("da_dev_username")};'
    f'PWD={os.getenv("da_dev_password")};'
    'Encrypt=yes;'
    'TrustServerCertificate=yes'
)
# (1) Main Get Datasource 

# Function Get all Datasources in all Workbooks
def get_embed_datasource():
    with server.auth.sign_in(tableau_auth):
        page_size = 100
        req_option = TSC.RequestOptions(pagesize=page_size)
        all_workbooks = []
        # datasource_id = []
        # workbook_id = []
        # old_data = []
        index = 1
        # workbook_name = []
        # datasource_name = []
        
        while True:
            workbooks, pagination_item = server.workbooks.get(req_option)
            all_workbooks.extend(workbooks)
            
            if pagination_item.total_available > len(all_workbooks):
                req_option.pagenumber += 1
            else:
                break
        
        print('Searching datasource in Workboook')
        try:    
            for i in all_workbooks:
                workbook = server.workbooks.get_by_id(i.id)
                server.workbooks.populate_connections(workbook)

                for connection in workbook.connections:
                    if connection.embed_password is False:
                        if connection.connection_type == 'sqlserver':
                            if workbook.project_name is not None:
                                # หา extract 
                                datasource = server.datasources.get_by_id(connection.datasource_id)
                                server.datasources.populate_connections(datasource)
                                # if datasource.has_extracts is False:
                                if connection.server_address.lower() in datasource_connection: 
                                    print(f"Datasource Number: {index}")
                                    print(f"Workbook ID: {workbook.id}")
                                    print(f"Workbook name: {workbook.name}")
                                    print(f"Project ID: {workbook.project_id}")
                                    print(f"Project name: {workbook.project_name}")
                                    print(f"id: {connection.id}")
                                    print(f"Datasource ID: {connection.datasource_id}")
                                    print(f"Datasource name: {connection.datasource_name}")
                                    print(f"Type: {connection.connection_type}")
                                    print(f"Server Address: {connection.server_address}")
                                    print(f"Embed Password: {connection.embed_password}")
                                    print(f"Has Extracts: {datasource.has_extracts}")
                                    print('---------------------------------------')
                                    workbook_id.append(workbook.id)
                                    datasource_id.append(connection.datasource_id)
                                    old_data.append(f"<Old Data = Workbook ID: '{workbook.id}', Workbook name: '{workbook.name}', Project ID: '{workbook.project_id}', Project name: '{workbook.project_name}', id: '{connection.id}', Datasource ID: '{connection.datasource_id}', Datasource name: '{connection.datasource_name}',  Type: '{connection.connection_type}', Server Address: '{connection.server_address}', Embed Status: '{connection.embed_password}', Embed Password: '{connection.embed_password}', Has Extracts: '{datasource.has_extracts}'>")
                                    workbook_name.append(workbook.name)
                                    datasource_name.append(connection.datasource_name)
                                    index+=1
                                    # else:
                                    #     pass
                                else:
                                    pass
                            else:
                                pass

                        else:
                            pass   
                    else:
                        pass
            print(f"Total Datasource is {index-1}")
        except ValueError as e:
            print("Finish Get All Datasource (1)")
            print(f"Function Get Datasource from Workbook")
            print(f"Workbook ID: {workbook.id}")
            print(f"Workbook name: {workbook.name}")
            print(f"⚠️ Error fetching Datasource ID {connection.datasource_id}: {e}")
            print(f"Datasource name: {connection.datasource_name}")
        except Exception as e:
            print("Finish Get All Datasource (1)")
            print(f"Function Get Datasource from Workbook")
            print(f"Workbook ID: {workbook.id}")
            print(f"Workbook name: {workbook.name}")
            print(f"⚠️ Unexpected Error for Datasource ID {connection.datasource_id}: {e}")
            print(f"Datasource name: {connection.datasource_name}")
            print('---------------------------------------')

    # ออกจากระบบ
    print("Finish Get All Datasource")
    server.auth.sign_out()
    return workbook_id,datasource_id,old_data,workbook_name,datasource_name



# (2) Change Embed Password Status เอา datasource_id จาก (1) มา Update
def change_embed_status(datasource_id):
    f = open(f"change_embed_logs.log","a",encoding="utf-8")

    try:
        with server.auth.sign_in(tableau_auth):
            dt = server.datasources.get_by_id(datasource_id)
            server.datasources.populate_connections(dt)
            connection = dt.connections[0]            
            connection.server_address = os.getenv("panda_server_address")
            connection.username =os.getenv("panda_username")
            connection.password = os.getenv("panda_password")
            connection.embed_password = True

            new_data = server.datasources.update_connection(dt, connection)
            print(f"New Data: {new_data}")
            new_status = new_data.embed_password
            if new_status ==True:
                new_status = 1
            else:
                new_status = 0
            log_txt = f"{datetime.now()}, Datasource ID: {datasource_id}, Datasource Name: {dt.name}, Updated connection: {connection.server_address}:{connection.server_port} with username {connection.username} Embed: {new_status}"
            print(log_txt)
            f.write(log_txt+'\n')
    except Exception as e:
        print("Error Function change_embed_status(2)")
        print(e)
        print('---------------------------------------')
        log_txt = f"{datetime.now()}, Datasource ID: {datasource_id}, Datasource Name: {dt.name}, Updated connection: {connection.server_address}:{connection.server_port} with username {connection.username} Error: {e}"
        f.write({datetime.now()} + str(e) + '\n')
        new_status = 0

    f.close
    return new_status
    

# (3) Insert log to SQL server
def insert_data(workbook_name,datasource_name,status,detail):
    f = open(f"insert_logs.log","a",encoding="utf-8")
    # insert data
    insert_query = '''
    INSERT INTO [da_dev].[Tableau_Change_Embed_Logs] (workbook_name, datasource_name, status, detail)
    VALUES (?, ?, ?,?)
    '''
    try:
        data = (workbook_name, datasource_name, status,detail)
        # Execute the insert statement
        cursor.execute(insert_query, data)

        # Commit the transaction
        conn.commit()
        print(f"Insert Data Workbook Name:{workbook_name}, Data Source: {datasource_name}, Status: {status}, Detail:{detail}")
    except Exception as e:

        print("Error function insert_data(4)")
        print(e)
        f.write({datetime.now()} + str(e) + '\n')

    f.close

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


# เริ่มทำงาน

# (1) Main Get Datasource 
# Function Get all Datasources in all Workbooks
if debuging:
    print('Dry run Get all datasources')
    workbook_id.append(debug_workbook)
    datasource_id.append(dabug_datasource)
    workbook_name.append('test_embed_password(Pep)')
    datasource_name.append('Test_embeded_password')
    old_data.append(f"Dry run Workbook: {workbook_id}, Workbook Name: {workbook_name}, Datasource: {datasource_id}, Datasource Name: {datasource_name} ")
    detail.append('Dry run test')
else:
    workbook_id,datasource_id,old_data,workbook_name,datasource_name = get_embed_datasource()


conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

for check_workbook_id,check_datasource_id,check_old_data,check_workbook_name,check_datasource_name in zip(workbook_id,datasource_id,old_data,workbook_name,datasource_name):
    
    # (2) Change Embed Password Status
    if dryrun:
        print("Dry run test change_embed_status(2)")
    else:
        new_status = change_embed_status(check_datasource_id)
    
    # (4) Insert log to SQL server
    insert_data(check_workbook_name,check_datasource_name,new_status,check_old_data)


cursor.close()
conn.close()
alert_text = f"วันที่ดำเนินการ: {datetime.now()}\nดำเนินการปรับสถานะ Datasource ให้เป็น Embeded เสร็จสิ้น\nจำนวน Datasource: {len(datasource_id)}\n"
alert_message(alert_text)
