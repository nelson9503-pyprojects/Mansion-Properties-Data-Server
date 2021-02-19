from .hse28_manager import Hse28Manager
from .hse730_manager import Hse730Manager
from .asiaxpat_manager import AsiaXPatManger
from .midland_manager import MidlandManager
from datetime import datetime
import time

class MAIN_SERVER:

    def __init__(self, db_path: str):
        print("initializing... please wait.")
        self.hse28 = Hse28Manager(db_path)
        self.hse730 = Hse730Manager(db_path)
        self.asiaxpat = AsiaXPatManger(db_path)
        self.midland = MidlandManager(db_path)
    
    def main_flow(self):
        while True:

            now = datetime.now()

            if now.hour == 0 and now.weekday() in [0, 1, 2, 3, 4, 5]:
                self.hse28.update_mode1()
            
            elif now.hour == 6 and now.weekday() in [0, 1, 2, 3, 4, 5]:
                self.hse28.update_mode2()
            
            elif now.hour == 12 and now.weekday() in [0, 1, 2, 3, 4, 5]:
                self.hse730.update_buy()
                self.hse730.update_rent()
            
            elif now.hour == 18 and now.weekday() in [0, 1, 2, 3, 4, 5]:
                self.asiaxpat.update()
            
            elif now.hour == 0 and now.weekday() == 6:
                self.hse28.update_mode3()
            
            elif now.hour == 6 and now.weekday() == 6:
                self.midland.update_mode1()
            
            elif now.hour == 18 and now.weekday() == 6:
                self.midland.update_mode2()
            
            time.sleep(1)