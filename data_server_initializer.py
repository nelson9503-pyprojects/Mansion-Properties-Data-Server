from Mansion_Properties_Data_Server.midland_manager import MidlandManager
import Mansion_Properties_Data_Server
import traceback

try:
    db_path = "properties_database.db"
    manage = MidlandManager(db_path)
    manage.update_mode1()
    
except KeyboardInterrupt:
    pass
except:
    with open("error_log.txt", 'w') as f:
        traceback.print_exc(file=f)
    input("Bugs found! >")