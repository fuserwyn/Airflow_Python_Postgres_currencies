# import psycopg2

# # with open("database.env", 'r') as file:
# #     lists = file.readlines()
# #     user = lists[0]
# #     password = lists[1]
# #     db_name = lists[2]
# connection = None    
# try:
#     # connect to exist database
#     connection = psycopg2.connect(
#         host="127.0.0.1",
#         # port = '5432',
#         user='airflow',
#         password='airflow',
#         database='airflow'    
#     )
#     connection.autocommit = True
    
#     # the cursor for perfoming database operations
#     # cursor = connection.cursor()
    
#     with connection.cursor() as cursor:
#         cursor.execute(
#             "SELECT version();"
#         )
        
#         print(f"Server version: {cursor.fetchone()}")

# except Exception as _ex:
#     print("[INFO] Error while working with PostgreSQL", _ex)
# finally:
#     if connection:
#         # cursor.close()
#         connection.close()
#         print("[INFO] PostgreSQL connection closed")