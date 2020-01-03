# -*- coding: utf-8 -*-

import json
import os

from generators.base_generator import BaseGenerator

class FileGenerator(BaseGenerator):
    """
    Export the generated objects to a JSON file
    Because there may have several types of objects, only semi-structured file format like JSON (or XML) can be used
    """

    def __init__(self, task_id=None, job_config='{}', directory=None, filename='exported_file'):
        super().__init__(task_id, job_config)
        if directory is None:
            directory = os.getcwd()
        self.directory = directory
        self.filename = filename

    def generate(self, generating_time=1, append=True, filename=None):
        # dump the generated objects to a json file
        if filename is None:
            filename = self.filename
        generated_objects = super().generate(generating_time)
        full_filename = os.path.join(self.directory, filename)
        if filename.find(".json") < 0 or filename.find(".json") != len(filename):
            full_filename += ".json"

        with open(full_filename, 'w') as fp:
            json.dump(generated_objects, fp)

