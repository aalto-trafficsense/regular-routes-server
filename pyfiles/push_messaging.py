# -*- coding: utf-8 -*-
#
# Push messages to terminals
#
# Created by: mikko.rinne@aalto.fi 10.1.2017
#
import json
import requests
import datetime

from pyfiles.config_helper import get_config
from tzlocal import get_localzone


header = {'Content-Type': 'application/json',
          'Authorization': 'key=' + get_config('FIREBASE_KEY')}


def push_ptp_pubtrans(device_alert):
    try:
        device_alert_msg = {'NOTIFICATION_TITLE': 'HRT / TrafficSense Public Transport Disruption Info',
                            'NOTIFICATION_MESSAGE': device_alert["en_text"],
                            'NOTIFICATION_URI': device_alert["en_uri"],
                            'NOTIFICATION_TITLE_FI': 'HSL / TrafficSense joukkoliikenteen häiriötieto',
                            'NOTIFICATION_MESSAGE_FI': device_alert["fi_text"],
                            'NOTIFICATION_URI_FI': device_alert["fi_uri"],
                            'PTP_ALERT_PUBTRANS': '1',
                            'PTP_ALERT_END': device_alert["alert_end"].strftime("%Y-%m-%d %H:%M:%S"),
                            'PTP_ALERT_TYPE': device_alert["alert_type"]}
        delta_sec = int((device_alert["alert_end"] - datetime.datetime.now()).total_seconds())
        ttl = 3600  # seconds = 1 hour default
        if (delta_sec > 0) and (delta_sec < 43200):  # Between positive and 12 hours
            ttl = delta_sec
        fire_msg = json.dumps({'to': device_alert["messaging_token"],
                               'data': device_alert_msg,
                               'time_to_live': ttl})
        firebase_request(fire_msg)
    except Exception as e:
        print "push_ptp_pubtrans / exception: ", e


def push_ptp_traffic(device_alert):
    try:
        device_alert_msg = {'NOTIFICATION_TITLE': 'DigiTraffic / TrafficSense Disruption Info',
                            'NOTIFICATION_MESSAGE': device_alert["en_text"],
                            'NOTIFICATION_TITLE_FI': 'DigiTraffic / TrafficSense liikenteen häiriötieto',
                            'NOTIFICATION_MESSAGE_FI': device_alert["fi_text"],
                            'PTP_ALERT_TRAFFIC': '1',
                            'PTP_ALERT_END': device_alert["alert_end"].strftime("%Y-%m-%d %H:%M:%S"),
                            'PTP_ALERT_TYPE': device_alert["alert_type"]}
        delta_sec = int((device_alert["alert_end"] - datetime.datetime.now(get_localzone())).total_seconds())
        ttl = 3600  # seconds = 1 hour default
        if (delta_sec > 0) and (delta_sec < 43200):  # Between positive and 12 hours
            ttl = delta_sec
        fire_msg = json.dumps({'to': device_alert["messaging_token"],
                               'data': device_alert_msg,
                               'time_to_live': ttl})
        firebase_request(fire_msg)
    except Exception as e:
        print "push_ptp_traffic / exception: ", e


def firebase_request(fire_msg):
    try:
        response = requests.post('https://fcm.googleapis.com/fcm/send', headers=header, data=fire_msg, verify=True)
        json_response = response.json()
        response.close()
        if json_response["success"]<1:
            print "firebase_request appears not to have succeeded: ", fire_msg
            print "response: ", json_response
    except requests.exceptions.ConnectionError as e:
        print "firebase_request / requests.exceptions.ConnectionError: ", e
    except Exception as e:
        print "firebase_request / exception: ", e


# Test data - comment out for releases

# sample_dev_alert = {'messaging_token': get_config('TEST_MSG_TOKEN'),
#                     'alert_end': datetime.datetime.strptime("2017-01-11 15:01:00", "%Y-%m-%d %H:%M:%S"),
#                     'alert_type': "FERRY",
#                     'fi_text': "Seutuliikenteen linjat: 510 Martinlaaksosta ja 550 Itäkeskuksesta, myöhästyy. Syy: ruuhka. Paikka: Kehä I Leppävaara. Arvioitu kesto: 08:16 - 08:55.",
#                     'fi_uri': "https://www.reittiopas.fi/disruptions.php",
#                     'en_text': "Regional traffic, lines: 510 from Martinlaakso and 550 from Itäkeskus, will be delayed. Cause: traffic jam. Location: Kehä I Leppävaara. Estimated time: 08:16 - 08:55.",
#                     'en_uri': "https://www.reittiopas.fi/en/disruptions.php"}
#
# push_ptp_pubtrans(sample_dev_alert)
