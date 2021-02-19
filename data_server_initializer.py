import Mansion_Properties_Data_Server
import traceback

try:
    server = Mansion_Properties_Data_Server.SERVER_INIT("properties_database.db")
    server.main_flow()
except KeyboardInterrupt:
    pass
except:
    with open("error_log.txt", 'w') as f:
        traceback.print_exc(file=f)
    input("Bugs found! >")