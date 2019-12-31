# -*- coding: utf-8 -*-

import json
import uuid
import random
import math
import datetime

from generators.generator_errors import *


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
            "object-8": {"type": "datetime", "lower-bound": "DAY-1", "upper-bound": "DAY"}
        },
        "objects": [
            {
                "__probability": 1,
                "__delay": {"probability": 0.99, "delay_time": "MINUTE+5"},
                "customer_id": {"type": "variable", "id": "object-7"},
                "birthdate": {"type": "datetime", "lower-bound": "YEAR-60", "upper-bound": "YEAR-20"}
            },
            {
                "__probability": 1,
                "__delay": {"probability": 0.99, "delay_time": "MINUTE+5"},
                "customer_id": {"type": "variable", "id": "object-7"},
                "birthdate": {"type": "datetime", "lower-bound": "YEAR-60", "upper-bound": "YEAR-20"}
            }
        ]

    }
    '''

    def __init__(self, task_id=None, job_config='{}'):
        #TODO: validate job_config extensively during this phase so the generating process doesn't need to do so
        if task_id is not None:
            self.task_id = task_id
        else:
            self.task_id = str(uuid.uuid4())

        if job_config is not dict:
            try:
                job_config = json.loads(job_config)
            except:
                raise ConfigurationError("Configuration is not in JSON format")

        if 'variables' not in job_config:
            job_config['variables'] = {}
        if 'objects' not in job_config or not isinstance(job_config['objects'], list) or len(job_config['objects']) == 0:
            raise ConfigurationError("Generation job must have at least 1 object")

        self.job_config = job_config

    def __get_time_bound(self, time_config):
        pass

    def __generate_value(self, config):
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
                generating_time = 1
                if config['length'] > 32:
                    generating_time = math.ceil(config['length']/32)
                for i in range(0, generating_time):
                    value += str(uuid.uuid4())
                value = value.replace('-', '')
                value = value[:config['length']]
            elif config['type'] == 'datetime':
                if config['lower-bound'] == config['upper-bound'] == 'NOW':
                    value = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                #value = datetime.datetime.now
            else:
                raise ExporterError("Export data type {} not supported".format(config['type']))

            return value

        # otherwise, the variable is a static value => return that value
        return config

    def generate(self, export_type="string"):
        if export_type not in ("string", 'json'):
            raise ExporterError("Export type only on 'string' or 'json' format")
        exported_object = {}

        #TODO: code to generate data goes here
        variables = {}
        variable_config = self.job_config['variables']
        for key in variable_config:
            variables[key] = self.__generate_value(variable_config[key])
        print(variables)

        if export_type == 'string':
            exported_object = json.dumps(exported_object)
        return exported_object
