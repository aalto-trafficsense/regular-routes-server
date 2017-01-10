#!/usr/bin/env python

# A helper to return configuration values to different modules

# mikko.rinne@aalto.fi 18.11.2016

import os
from flask import Flask

APPLICATION_NAME = 'TrafficSense'
SETTINGS_FILE_ENV_VAR = 'REGULARROUTES_SETTINGS'

app = Flask(__name__)

env_var_value = os.getenv(SETTINGS_FILE_ENV_VAR, None)
if env_var_value is not None:
    print 'loading settings from: "' + str(env_var_value) + '"'
    app.config.from_envvar(SETTINGS_FILE_ENV_VAR)
else:
    print 'Environment variable "SETTINGS_FILE_ENV_VAR" was not defined -> using debug mode'
    # assume debug environment
    app.config.from_pyfile('../regularroutes.cfg')
    app.debug = True


def get_config(varname):
    return app.config[varname]

