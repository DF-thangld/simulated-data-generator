import schedule
import time
import json

import generators.base_generator as base_generator
from generators.generator_errors import *

def job(number=0):
    print("I'm working...", number)

#schedule.every(10).minutes.do(job)
#schedule.every().hour.do(job)
#schedule.every().day.at("10:30").do(job)
#schedule.every().monday.do(job)
#schedule.every().wednesday.at("13:15").do(job)
#schedule.every().minute.at(":17").do(job)

abc = None
try:
    abc = base_generator.BaseGenerator()
except ConfigurationError as e:
    print(e)