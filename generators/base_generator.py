# -*- coding: utf-8 -*-

import json
import uuid

from generators.generator_errors import *


class BaseGenerator:

    """
    {
        variables: [
            'object-1': ['1', '2', '3'],
            'object-2': 'some constant string',
            'object-3': 6,
            'object-4': 3.1412,
            'object-5': {'type': 'numeric', 'lower-bound': 0, 'upper-bound': 10},
            'object-6': {'type': 'float', 'lower-bound': 0, 'upper-bound': 10},
            'object-7': {'type': 'string', 'length': 10},
            'object-8': {'type': 'datetime', 'lower-bound': 'T-1', 'upper-bound': 'T'}
        ],
        objects: [
            'customer': {
                '__probability': 1,
                '__delay': {'probability': 0.99, 'delay_time': 'M+5'},
                'customer_id': {}
            }
        ]

    }
    """

    def __init__(self, task_id=None, job_config='{}'):
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
            job_config['variables'] = []
        if 'objects' not in job_config:
            raise ConfigurationError("Generation job must have at least 1 object")
        self.job_config = job_config

    def generate(self, export_type="string"):
        if export_type not in ("string", 'json'):
            raise ExporterError("Export type only on 'string' or 'json' format")
        exported_object = {}

        #TODO: code to generate data goes here

        if export_type == 'string':
            exported_object = json.dumps(exported_object)
        return exported_object
