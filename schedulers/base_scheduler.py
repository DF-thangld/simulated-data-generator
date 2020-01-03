# -*- coding: utf-8 -*-

import uuid
import datetime
from multiprocessing import Process

from errors import *

class BaseScheduler:

    def __init__(self, scheduler_id=None, schedule={}, generator=None):

        # the process to manage the schedule
        self.process = None

        if scheduler_id is not None:
            self.scheduler_id = scheduler_id
        else:
            self.task_id = str(uuid.uuid4())

        if generator is not None:
            self.generator = generator
        else:
            raise Exception("Must set a generator to scheduler")

        if 'type' not in schedule:
            raise SchedulingError('Scheduling type not found')
        if 'status' not in schedule:
            raise SchedulingError("Schedule parameter 'status' not found")
        elif schedule['status'] not in ['Active', 'Inactive']:
            raise SchedulingError("Schedule status only take the following values: 'Active', 'Inactive'")

        if 'valid_from' in schedule:
            try:
                valid_from = datetime.datetime.strptime(schedule['valid_from'], '%Y-%m-%d %H:%M:%S')
            except:
                raise SchedulingError("Value {} not valid for parameter 'valid_from'. Must be in '%Y-%m-%d %H:%M:%S' format".format(schedule['valid_from']))

        if 'valid_to' in schedule:
            try:
                valid_to = datetime.datetime.strptime(schedule['valid_to'], '%Y-%m-%d %H:%M:%S')
            except:
                raise SchedulingError("Value {} not valid for parameter 'valid_to'. Must be in '%Y-%m-%d %H:%M:%S' format".format(schedule['valid_to']))

        self.schedule = schedule

    def run(self):
        # To be implemented by children class
        raise SchedulingError("Function run() in class BaseScheduler should not be executed")
    def terminate(self):
        # To be implemented by children class
        raise SchedulingError("Function terminate() in class BaseScheduler should not be executed")

    def start(self):
        self.schedule['status'] = 'Active'
        self.process = Process(target=self.run)
        print('Start batch schedule {}'.format(self.scheduler_id))
        self.process.start()

    def stop(self):
        self.schedule['status'] = 'Inactive'
        print('Terminate batch schedule {}'.format(self.scheduler_id))
        self.process.terminate()
        self.terminate()
