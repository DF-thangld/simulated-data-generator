# -*- coding: utf-8 -*-

import json
import os
import datetime
import time

from schedulers.base_scheduler import BaseScheduler
from errors import *

class BatchScheduler(BaseScheduler):
    """
        Schedule the generating job on a batchly manner
        """

    def __init__(self, scheduler_id=None, schedule={}, generator=None):
        """
        Needed parameters for batch scheduler:
            type: batch
            generating_time: number
            status: 'Active', 'Inactive'
            interval: 'annually', 'monthly', 'daily', 'hourly', 'per_minute', 'once'
            detail:
                'once': No need to have detail parameter
                'per_minute': No need to have detail parameter
                'hourly': {'minute': 30}
                'daily': {'hour': 3, 'minute': 0}
                'monthly': {'day': 18, 'hour': 14, minute: 56} note that some months don't have 29, 30 or 31 days
                'annually': {'month': 1, 'day': 18, 'hour': 14, minute: 56}
            'valid_from': time in 'YYYY-MM-DD HH24:MI:SS' format or not needed. This parameter also triggers 'once' interval
            'valid_to': time in 'YYYY-MM-DD HH24:MI:SS' format or not needed
        """
        super().__init__(scheduler_id, schedule, generator)

        # validate schedule based on configuration
        if schedule['type'] != 'batch':
            raise SchedulingError('Batch scheduling only!')
        if 'generating_time' not in schedule:
            raise SchedulingError("Schedule parameter 'generating_time' not found")
        elif isinstance(schedule['generating_time'], int) or schedule['generating_time'] <= 0:
            raise SchedulingError("Schedule parameter 'generating_time' must be a positive number")

        if 'interval' not in schedule:
            raise SchedulingError("Schedule parameter 'interval' not found")
        elif schedule['interval'] not in ['annually', 'monthly', 'daily', 'hourly', 'per_minute', 'once']:
            raise SchedulingError("Schedule interval only take the following values: 'annually', 'monthly', 'daily', 'hourly', 'per_minute', 'once'")

        if schedule['interval'] in ['annually', 'monthly', 'daily', 'hourly'] and 'detail' not in schedule:
            raise SchedulingError("Interval {} requires 'detail' parameter".format(schedule['interval']))
        if schedule['interval'] in ['annually', 'monthly', 'daily', 'hourly'] and 'minute' not in schedule['detail']:
            raise SchedulingError("Interval {} requires 'minute' parameter in detail field".format(schedule['interval']))
        elif schedule['interval'] in ['annually', 'monthly', 'daily', 'hourly'] and (not isinstance(schedule['detail']['minute'], int) or (schedule['detail']['minute'] < 0 or schedule['detail']['minute'] > 59)):
            raise SchedulingError("Value {} not valid for parameter 'minute'".format(schedule['detail']['minute']))

        if schedule['interval'] in ['annually', 'monthly', 'daily'] and 'hour' not in schedule['detail']:
            raise SchedulingError("Interval {} requires 'hour' parameter in detail field".format(schedule['interval']))
        elif schedule['interval'] in ['annually', 'monthly', 'daily'] and (not isinstance(schedule['detail']['hour'], int) or (schedule['detail']['hour'] < 0 or schedule['detail']['hour'] > 23)):
            raise SchedulingError("Value {} not valid for parameter 'hour'".format(schedule['detail']['hour']))

        if schedule['interval'] in ['annually', 'monthly'] and 'day' not in schedule['detail']:
            raise SchedulingError("Interval {} requires 'day' parameter in detail field".format(schedule['interval']))
        elif schedule['interval'] in ['annually', 'monthly'] and (not isinstance(schedule['detail']['day'], int) or (schedule['detail']['day'] < 0 or schedule['detail']['day'] > 31)):
            raise SchedulingError("Value {} not valid for parameter 'day'".format(schedule['detail']['day']))

        if schedule['interval'] in ['annually'] and 'month' not in schedule['detail']:
            raise SchedulingError("Interval {} requires 'month' parameter in detail field".format(schedule['interval']))
        elif schedule['interval'] in ['annually'] and (not isinstance(schedule['detail']['month'], int) or (schedule['detail']['month'] < 0 or schedule['detail']['month'] > 12)):
            raise SchedulingError("Value {} not valid for parameter 'month'".format(schedule['detail']['month']))

    def __process_once(self):
        if 'valid_from' in self.schedule:
            valid_from = datetime.datetime.strptime(self.schedule['valid_from'], '%Y-%m-%d %H:%M:%S')
            now = datetime.datetime.now()
            if valid_from > now:
                time.sleep(int((valid_from - now).seconds))

        self.generator.generate(self.schedule['generating_time'])

    def __process_interval(self):
        # sleep process to 'valid_from' time
        now = datetime.datetime.now()
        valid_from = datetime.datetime.strptime(self.schedule['valid_from'], '%Y-%m-%d %H:%M:%S')
        valid_to = datetime.datetime.strptime(self.schedule['valid_to'], '%Y-%m-%d %H:%M:%S')
        if valid_from > now:
            time.sleep(int((valid_from - now).seconds))

        # TODO: calculate next run
        while now < valid_to:

            now = datetime.datetime.now()
            next_run = None
            if self.schedule['interval'] == 'per_minute':
                next_run = now + datetime.timedelta(0, 60)
                next_run = next_run.replace(second=0)

            elif self.schedule['interval'] == 'hourly':
                if self.schedule['detail']['minute'] < now.minute:
                    next_run = datetime.datetime.now()
                    next_run = next_run.replace(minute=self.schedule['detail']['minute'], second=0)
                else:
                    next_run = now + datetime.timedelta(0, 3600)
                    next_run = next_run.replace(minute=self.schedule['detail']['minute'], second=0)
            elif self.schedule['interval'] == 'daily':
                if (self.schedule['detail']['hour'] < now.hour) or (self.schedule['detail']['hour'] == now.hour and self.schedule['detail']['minute'] < now.minute):
                    next_run = datetime.datetime.now()
                    next_run = next_run.replace(hour=self.schedule['detail']['hour'], minute=self.schedule['detail']['minute'], second=0)
                else:
                    next_run = now + datetime.timedelta(1)
                    next_run = next_run.replace(hour=self.schedule['detail']['hour'], minute=self.schedule['detail']['minute'], second=0)

            elif self.schedule['interval'] == 'monthly':
                if (self.schedule['detail']['day'] < now.day) \
                        or (self.schedule['detail']['day'] == now.day and self.schedule['detail']['hour'] < now.hour)\
                        or (self.schedule['detail']['day'] == now.day and self.schedule['detail']['hour'] == now.hour and self.schedule['detail']['minute'] < now.minute):
                    next_run = datetime.datetime.now()
                    next_run = next_run.replace(day=self.schedule['detail']['day'], hour=self.schedule['detail']['hour'], minute=self.schedule['detail']['minute'], second=0)
                else:
                    next_run = datetime.datetime.now()
                    if now.month == 12:
                        next_run = next_run.replace(year=now.year+1, month=1, day=self.schedule['detail']['day'],
                                                    hour=self.schedule['detail']['hour'], minute=self.schedule['detail']['minute'],
                                                    second=0)
                    else:
                        next_run = next_run.replace(month=now.month+1, day=self.schedule['detail']['day'],
                                                    hour=self.schedule['detail']['hour'],
                                                    minute=self.schedule['detail']['minute'], second=0)
            elif self.schedule['interval'] == 'annually':
                if (self.schedule['detail']['month'] < now.month) \
                        or (self.schedule['detail']['month'] == now.month and self.schedule['detail']['day'] < now.day) \
                        or (self.schedule['detail']['month'] == now.month and self.schedule['detail']['day'] == now.day and self.schedule['detail']['hour'] < now.hour) \
                        or (self.schedule['detail']['month'] == now.month and self.schedule['detail']['day'] == now.day and self.schedule['detail']['hour'] == now.hour and self.schedule['detail']['minute'] < now.minute):
                    next_run = datetime.datetime.now()
                    next_run = next_run.replace(month=self.schedule['detail']['month'],
                                                day=self.schedule['detail']['day'],
                                                hour=self.schedule['detail']['hour'],
                                                minute=self.schedule['detail']['minute'],
                                                second=0)
                else:
                    next_run = datetime.datetime.now()
                    next_run = next_run.replace(year=next_run.year,
                                                month=self.schedule['detail']['month'],
                                                day=self.schedule['detail']['day'],
                                                hour=self.schedule['detail']['hour'],
                                                minute=self.schedule['detail']['minute'],
                                                second=0)

            if next_run >= valid_to:
                break
            time.sleep(int((next_run - now).seconds))
            self.generator.generate(self.schedule['generating_time'])

    def run(self):
        if self.schedule['interval'] == 'once':
            self.__process_once()
            self.stop()
            return
        else:
            self.__process_interval()


    def terminate(self):
        print('Terminate batch schedule {}'.format(self.scheduler_id))
