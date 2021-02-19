import Mansion_Properties_Data_Server
import traceback

try:
    server = Mansion_Properties_Data_Server.MAIN_SERVER("properties_database.db")
    server.main_flow()
except:
    with open("error_log.txt", 'w') as f:
        traceback.print_exc(file=f)