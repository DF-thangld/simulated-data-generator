# -*- coding: utf-8 -*-

import json
import uuid
import random
import math
import datetime

from errors import *


class BaseGenerator:

    sample = '''
    {
        "variables": {
            "object-10": {"type": "string", "length": 100},
            "object-1": ["1", "2", "3"],
            "object-2": "some constant string",
            "object-3": 6,
            "object-4": 3.1412,
            "object-5": {"type": "numeric", "lower-bound": 0, "upper-bound": 10},
            "object-6": {"type": "float", "lower-bound": 0, "upper-bound": 10},
            "object-7": {"type": "string", "length": 10},
            "object-8": {"type": "datetime", "lower-bound": "YEAR-100", "upper-bound": "YEAR+100"}
        },
        "objects": [
            {
                "__probability": 0.1,
                "__delay": {"probability": 0.01, "delay_time": "MINUTE+5"},
                "customer_id": {"type": "variable", "id": "object-7"},
                "birthdate": {"type": "datetime", "lower-bound": "YEAR-60", "upper-bound": "YEAR-20"}
            },
            {
                "__probability": 1,
                "__delay": {"probability": 0.01, "delay_time": "MINUTE+5"},
                "customer_id": {"type": "variable", "id": "object-7"},
                "birthdate": {"type": "datetime", "lower-bound": "YEAR-60", "upper-bound": "YEAR-20"}
            }
        ]

    }
    '''

    def __init__(self, task_id=None, job_config='{}'):
        self.validated_job_config = None
        if task_id is not None:
            self.task_id = task_id
        else:
            self.task_id = str(uuid.uuid4())
        self.validate_job_config(job_config)

    def validate_job_config(self, job_config):

        if job_config is not dict:
            try:
                job_config = json.loads(job_config)
            except:
                raise ConfigurationError("Configuration is not in JSON format")

        if 'variables' not in job_config:
            job_config['variables'] = {}
        if 'objects' not in job_config or not isinstance(job_config['objects'], list) or len(
                job_config['objects']) == 0:
            raise ConfigurationError("Generation job must have at least 1 object")
        for object_config in job_config['objects']:
            if '__probability' not in object_config:
                object_config['__probability'] = 1
            if '__delay' not in object_config:
                object_config['__delay'] = {"probability": 0, "delay_time": "NOW"}

        self.job_config = job_config
        self.validated_job_config = dict(job_config)
        for key in self.validated_job_config['variables'].keys():
            self.validated_job_config['variables'][key] = self.__validate_data_config(key, self.validated_job_config['variables'][key])
        for object_config in self.validated_job_config['objects']:
            for key in object_config:
                if not key.startswith('__'):
                    object_config[key] = self.__validate_data_config(key, object_config[key])

    def __validate_data_config(self, key, config):
        if isinstance(config, dict):
            if 'type' not in config:
                raise ExporterError("Data field or variable '{}' does not have paramter 'type'".format(key))
            if config['type'] in ['numeric', 'float', 'datetime']:
                if 'lower-bound' not in config:
                    raise ExporterError("Data type 'numeric' in data field or variable '{}' should have parameter 'lower-bound'".format(key))
                if 'upper-bound' not in config:
                    raise ExporterError("Data type 'numeric' in data field or variable '{}' should have parameter 'upper-bound'".format(key))
                if config['type'] != 'datetime':
                    if config['upper-bound'] < config['lower-bound']:
                        raise ExporterError("Data type 'numeric' in data field or variable '{}' should have parameter 'upper-bound' greater than paramter 'lower-bound'".format(key))
                else:  # datetime
                    config['lower-bound'] = self.__validate_time_config(config['lower-bound'])
                    config['upper-bound'] = self.__validate_time_config(config['upper-bound'])
            elif config['type'] == 'string':
                if 'length' not in config:
                    raise ExporterError("Data type 'string' in data field or variable '{}' should have parameter 'length'".format(key))
                if config['length'] <= 0:
                    raise ExporterError("Data type 'string' in data field or variable '{}' should have length greater than 0".format(key))
                if config['length'] > 32:
                    generating_time = math.ceil(config['length']/32)
                else:
                    generating_time = 1
                config['generating_time'] = generating_time
            elif config['type'] == 'variable':
                if 'id' not in config:
                    raise ExporterError("Data type 'variable' in data field or variable '{}' should have parameter 'id'".format(key))

        return config

    def __validate_time_config(self, time_config):
        time_config_org = time_config
        time_delta_direction = 1
        time_config = time_config.split('+')
        if len(time_config) == 1:
            time_config = time_config[0].split('-')
            time_delta_direction = -1
        if len(time_config) == 1:
            time_delta_direction = 1
            time_delta = 0
        else:
            time_delta = int(time_config[1])
        time_unit = time_config[0]

        return {'__org': time_config_org, 'direction': time_delta_direction, 'delta': time_delta, 'unit': time_unit}

    def __get_time_bound(self, time_config):
        time_config_org = time_config['__org']
        time_delta_direction = time_config['direction']
        time_unit = time_config['unit']
        time_delta = time_config['delta']

        value = datetime.datetime.now()
        if time_unit == 'NOW':
            pass  # already take datetime.datetime.now() in the previous 2 lines
        elif time_unit == 'SECOND':
            value = value + datetime.timedelta(0, time_delta*time_delta_direction)
        elif time_unit == 'MINUTE':
            value = value.replace(second=0)
            value = value + datetime.timedelta(0, time_delta * 60 * time_delta_direction)
        elif time_unit == 'HOUR':
            value = value.replace(minute=0, second=0)
            value = value + datetime.timedelta(0, time_delta * 3600 * time_delta_direction)
        elif time_unit == 'DAY':
            value = value.replace(hour=0, minute=0, second=0)
            value = value + datetime.timedelta(time_delta * time_delta_direction)
        elif time_unit == 'MONTH':
            # TODO: should solve the case a month have more or less than 30 days for absolute correction
            # TO BE CONSIDERED: should it be that much accuracy?
            value = value.replace(hour=0, minute=0, second=0)
            value = value + datetime.timedelta(time_delta * 30 * time_delta_direction)
        elif time_unit == 'YEAR':
            # TODO: should solve the case a year have more than 365 days for absolute correction
            # TO BE CONSIDERED: should it be that much accuracy?
            value = value.replace(hour=0, minute=0, second=0)
            value = value + datetime.timedelta(time_delta * 365 * time_delta_direction)
        else:
            raise ExporterError("Time parameter '{}' not valid".format(time_config_org))

        return value

    def __generate_value(self, config, predefined_variables=[]):
        # if the config is a list of several values => choose a random value in that list
        if isinstance(config, list):
            index = random.randint(0, len(config)-1)
            return config[index]
        if isinstance(config, dict):
            if config['type'] == 'numeric':
                value = random.randint(config['lower-bound'], config['upper-bound'])
            elif config['type'] == 'float':
                value = config['lower-bound'] + (config['upper-bound'] - config['lower-bound'])*random.random()
            elif config['type'] == 'string':
                value = ''
                for i in range(0, config['generating_time']):
                    value += str(uuid.uuid4())
                value = value.replace('-', '')
                value = value[32*config['generating_time'] - config['length']:]
            elif config['type'] == 'datetime':
                if config['lower-bound'] == config['upper-bound'] == 'NOW':
                    value = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                else:
                    lower_bound = self.__get_time_bound(config['lower-bound'])
                    upper_bound = self.__get_time_bound(config['upper-bound'])
                    value = lower_bound + (datetime.timedelta(0, random.randint(0, int((upper_bound - lower_bound).total_seconds()))))
                    value = value.strftime('%Y-%m-%d %H:%M:%S')
            elif config['type'] == 'variable':
                return predefined_variables[config['id']]
            else:
                raise ExporterError("Export data type {} not supported".format(config['type']))

            return value

        # otherwise, the variable is a static value => return that value
        return config

    def generate(self, generate_time=1):
        exported_object = []

        for i in range (0, generate_time):
            variables = {}
            variable_config = self.validated_job_config['variables']
            for key in variable_config:
                variables[key] = self.__generate_value(variable_config[key])
            #print(variables)
            for object_config in self.validated_job_config['objects']:
                simulated_object = {}
                appear_probability = random.random()
                if appear_probability > object_config['__probability']:
                    continue
                delayed_probability = random.random()
                if delayed_probability > object_config['__delay']['probability']:
                    #TODO: create delayed situation
                    pass
                for key in object_config.keys():
                    if not key.startswith('__'):
                        simulated_object[key] = self.__generate_value(object_config[key], predefined_variables=variables)
                exported_object.append(simulated_object)

        return exported_object
