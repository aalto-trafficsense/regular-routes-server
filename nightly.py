#!/usr/bin/env python

import json
import hashlib
import geoalchemy2 as ga2
from datetime import date, timedelta
from flask import Flask, abort, jsonify, request, render_template, Response
from flask.ext.sqlalchemy import SQLAlchemy
from oauth2client.client import *
from oauth2client.crypt import AppIdentityError
from sqlalchemy import MetaData, Table, Column, ForeignKey, Enum, BigInteger, Integer, String, Index, UniqueConstraint
from sqlalchemy.dialects.postgres import DOUBLE_PRECISION, TIMESTAMP, UUID
from sqlalchemy.exc import DataError
from sqlalchemy.sql import text, func, column, table, select
from uuid import uuid4

def make_clusters(db):
    return "test"
#dat = array(c.execute('SELECT lon,lat,t,d_id FROM data_averaged WHERE d_id=?', (45,)).fetchall(),dtype={'names':['lon', 'lat', 't', 'd_id'], 'formats':['f4','f4','f4','i4']})
#    with open('sql/learningtest.sql', 'r') as sql_file:
#        sql = sql_file.read()
#        result = db.engine.execute(text(sql))
