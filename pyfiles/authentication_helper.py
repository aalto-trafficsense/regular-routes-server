#!/usr/bin/env python

import hashlib
import httplib2

from flask import abort, jsonify
from flask import make_response
from oauth2client.client import *
from oauth2client.crypt import AppIdentityError

# from simplekv.memory import DictStore

import logging
logging.basicConfig()

def user_hash(account_google_id):
    return str(hashlib.sha256(str(account_google_id).encode('utf-8')).hexdigest()).upper()

def authenticate_with_google_oauth(redirect_uri, client_secret_file, client_id, one_time_token):
    """
        doc: https://developers.google.com/api-client-library/python/guide/aaa_oauth
    :param one_time_token: one time token acquired from Google with mobile client
    :return: dictionary with data used in authorization
    """
    # step1 to acquire one_time_token is done in android client
    try:
        flow = flow_from_clientsecrets(client_secret_file,
                                   scope='profile',
                                   redirect_uri=redirect_uri)
        credentials = flow.step2_exchange(one_time_token)
    except FlowExchangeError as err:
        response = make_response(
            json.dumps('Failed to upgrade the authorization code.'), 401)
        response.headers['Content-Type'] = 'application/json'
        # invalid token
        print 'invalid token: ' + one_time_token + ". error: " + err.message
        return response

    return verify_and_get_account_id(client_id, credentials)


def verify_and_read_id_token(id_token, client_id):
    data = {}
    if id_token is not None:
        # Check that the ID Token is valid.
        try:
            # Client library can verify the ID token.
            jwt = verify_id_token(jsonify(id_token), client_id)

            data['valid_id_token'] = True
            data['google_id'] = jwt['sub']

        except AppIdentityError as error:
            print 'verify: AppIdentityError: ' + error.message
            data['valid_id_token'] = False
    else:
        print 'verify: credentials.id_token is None'

    return data


def verify_id_token_values(true_client_id, id_token):
    # true_client_id = json.loads(open(CLIENT_SECRET_FILE, 'r').read())['web']['client_id']
    server_client_id = str(id_token['aud'])
    if server_client_id != true_client_id:
        print 'invalid server client id returned'
        abort(403)


def verify_and_get_account_id(client_id, credentials):
    """Verify an ID Token or an Access Token."""

    verify_id_token_values(client_id, credentials.id_token)

    data = {}
    data['google_id'] = str(credentials.id_token['sub'])
    data['access_token'] = credentials.access_token
    data['valid_access_token'] = True

    if credentials.refresh_token is not None:
        data['refresh_token'] = credentials.refresh_token

    http = httplib2.Http()
    credentials.authorize(http)

    return data

