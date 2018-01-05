# -*- coding: utf-8 -*- {{{
# vim: set fenc=utf-8 ft=python sw=4 ts=4 sts=4 et:

# Copyright (c) 2017, SLAC National Laboratory / Kisensum Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation
# are those of the authors and should not be interpreted as representing
# official policies, either expressed or implied, of the FreeBSD
# Project.
#
# This material was prepared as an account of work sponsored by an
# agency of the United States Government.  Neither the United States
# Government nor the United States Department of Energy, nor SLAC / Kisensum,
# nor any of their employees, nor any jurisdiction or organization that
# has cooperated in the development of these materials, makes any
# warranty, express or implied, or assumes any legal liability or
# responsibility for the accuracy, completeness, or usefulness or any
# information, apparatus, product, software, or process disclosed, or
# represents that its use would not infringe privately owned rights.
#
# Reference herein to any specific commercial product, process, or
# service by trade name, trademark, manufacturer, or otherwise does not
# necessarily constitute or imply its endorsement, recommendation, or
# favoring by the United States Government or any agency thereof, or
# SLAC / Kisensum. The views and opinions of authors
# expressed herein do not necessarily state or reflect those of the
# United States Government or any agency thereof.
#
# }}}

"""
This file provides helper functions for Influxdb database operation
used in:

:py:class:`services.core.InfluxdbHistorian.influx.historian.InfluxdbHistorian`
"""

import logging
import json
from dateutil import parser
from requests.exceptions import ConnectionError

from influxdb import InfluxDBClient, SeriesHelper
from influxdb.exceptions import InfluxDBClientError

from volttron.platform.agent.utils import parse_timestamp_string, format_timestamp

_log = logging.getLogger(__name__)
__version__ = '0.1'


# class DataHistorianSeriesHelper(SeriesHelper):
#     """ Meta class stores time series helper configuration.
#     """
#     class Meta:
#         # The client should be an instance of InfluxDBClient.
#         client = 'dummy'
#         # The series name must be a string. Add dependent fields/tags in curly brackets.
#         series_name = 'data'
#         # Defines all the fields in this time series.
#         fields = ['value']
#         # Defines all the tags for the series.
#         tags = ['source', 'topic']
#         # Defines the number of data points to store prior to writing on the wire.
#         bulk_size = 10
#         # autocommit must be set to True when using bulk_size
#         autocommit = True
#
#     def __init__(self, client=None, **kwargs):
#         cls = self.__class__
#         if client:
#             cls._client = client
#
#         if kwargs:
#             super(DataHistorianSeriesHelper, self).__init__(**kwargs)
#
#
# class MetaHistorianSeriesHelper(SeriesHelper):
#     """ Meta class stores time series helper configuration.
#     """
#     class Meta:
#         # The client should be an instance of InfluxDBClient.
#         client = 'dummy'
#         # The series name must be a string. Add dependent fields/tags in curly brackets.
#         series_name = 'meta'
#         # Defines all the fields in this time series.
#         fields = ['meta_dict', 'last_updated']
#         # Defines all the tags for the series.
#         tags = ['topic']
#         # Defines the number of data points to store prior to writing on the wire.
#         bulk_size = 10
#         # autocommit must be set to True when using bulk_size
#         autocommit = True
#
#     def __init__(self, client=None, **kwargs):
#         cls = self.__class__
#         if client:
#             cls._client = client
#
#         if kwargs:
#             super(MetaHistorianSeriesHelper, self).__init__(**kwargs)


def get_client(connection_params):
    """
    Connect to InfluxDB client

    :param connection_params: This is what would be provided in the config file
    :return: an instance of InfluxDBClient
    """
    db = connection_params['database']
    host = connection_params['host']
    port = connection_params['port']
    user = connection_params.get('user', None)
    passwd = connection_params.get('passwd', None)

    try:
        client = InfluxDBClient(host, port, user, passwd, db)
        dbs = client.get_list_database()
        if {"name": db} not in dbs:
            _log.error("Database {} does not exist.".format(db))
            return None
    except ConnectionError, err:
        _log.error("Cannot connect to host {}. {}".format(host, err))
        return None
    except InfluxDBClientError, err:
        _log.error(err)
        return None

    return client


# def publish_to_client(client, data_list):
#     """
#     Execute queries to insert data into influxdb client.
#
#     If all data in data_list are done inserted, the function should return
#     the maximum index in the list, which should be len(data_list) - 1.
#
#
#     If not all the data in data_list are inserted, return the index
#     in the data_list of the last data point inserted into InfluxDB database
#
#     :param client: Influxdb client we connected in historian_setup method.
#     :param data_list: list of data points to be inserted
#     :return: the index of the last data point inserted
#     """
#     num_dps = 0
#     DataHistorianSeriesHelper(client=client)
#     MetaHistorianSeriesHelper(client=client)
#
#     for i, point in enumerate(data_list):
#         try:
#             data = point['data']
#             meta = point['meta']
#             num_dps += 1
#             DataHistorianSeriesHelper(**data)
#             MetaHistorianSeriesHelper(**meta)
#         except NameError as e:
#             _log.error(e)
#             num_dps -= 1
#             break
#
#     already_stored_count = 0
#     try:
#         if num_dps > 1:
#             _log.info("Commited {} measurements to data".format(num_dps))
#             DataHistorianSeriesHelper.commit()
#             MetaHistorianSeriesHelper.commit()
#             already_stored_count = num_dps
#         else:
#             _log.info("No new measurements")
#     except InfluxDBClientError, e:
#         _log.error(e)
#         # Query data from first timestamp in data_list to last timestamp in data_list
#         begin_ts = parse_timestamp_string(data_list[0]['data']['time'])
#         end_ts = parse_timestamp_string(data_list[len(data_list) - 1]['data']['time'])
#         begin_epoch = begin_ts.strftime('%s') + '0' * 9
#         end_epoch = end_ts.strftime('%s') + '0' * 9
#         query = 'SELECT value FROM data WHERE time >= %s and time <= %s' % (begin_epoch, end_epoch)
#
#         rs = client.query(query)
#         rs = list(rs.get_points())
#
#         if len(rs) > 0:
#             # Get last stored data index in data_list
#             for i, point in enumerate(rs):
#                 time_in_historian = parse_timestamp_string(data_list[i]['data']['time'])
#                 time_in_influx = parser.parse(point['time'])
#                 if time_in_historian == time_in_influx:
#                     continue
#                 else:
#                     already_stored_count = i + 1
#                     break
#         else:
#             already_stored_count = 0
#
#     return already_stored_count


def get_all_topics(client):
    """
    Execute query to return the topic list we stored.
    This information should take from 'meta' measurement in the InfluxDB database.

    :param client: Influxdb client we connected in historian_setup method.
    :return: a list of all unique topics published.
    """
    topic_list = []

    query = 'SELECT meta_dict, topic FROM meta'
    rs = client.query(query)
    rs = list(rs.get_points())

    for point in rs:
        topic_list.append(point['topic'])

    # raise ValueError(topic_list)
    return topic_list


def get_topic_values(client, topic, start, end, skip, count, order):
    """
    This is a helper for function query_historian in InfluxdbHistorian class.
    Execute query to return a list of values of specific topic(s)
    The information should take from 'data' measurement in the in the InfluxDB database.

    Please see
    :py:meth:`volttron.platform.agent.base_historian.BaseQueryHistorianAgent.query_historian`
    for input parameters
    """
    query = 'SELECT value FROM data WHERE topic=\'%s\'' % topic

    if start:
        start_time = format_timestamp(start).replace("+00:00", "Z")
        query += ' AND time >= \'%s\'' % start_time
    if end:
        end_time = format_timestamp(end).replace("+00:00", "Z")
        query += ' AND time <= \'%s\'' % end_time
    if order == "LAST_TO_FIRST":
        query += ' ORDER BY time DESC'

    query += ' LIMIT %d' % count
    if skip:
        query += ' OFFSET %d' % skip

    rs = client.query(query)
    rs = list(rs.get_points())

    values = []
    for point in rs:
        ts = point['time']
        # Influxdb uses nanosecond-precision Unix time.
        # However, datetime objects don't support anything more fine than microseconds
        if len(point['time']) > 27:
            micro = int(round(float('0.' + ts[20:-1]) * (10 ** 6)))
            micro = '%06d' % micro
            ts = ts[0:20] + micro + '+00:00'
        else:
            ts = point['time'].replace("Z", "+00:00")

        ts = format_timestamp(parse_timestamp_string(ts))
        values.append((ts, point['value']))

    return values


def get_topic_meta(client, topic):
    """
    Execute query to return the meta dictionary of a specific topic
    This information should take from 'meta' measurement in the InfluxDB database.
    """
    query = 'SELECT meta_dict FROM meta WHERE topic=\'%s\'' % topic
    rs = client.query(query)
    rs = list(rs.get_points())
    meta = rs[0]['meta_dict'].replace("u'", "\"").replace("'", "\"")
    return json.loads(meta)


def get_all_topic_meta(client):
    """
    Execute query to return meta dict for all topics.
    This information should take from 'meta' measurement in the InfluxDB database.

    :param client: InfluxDB client connected in historian_setup method.
    :return: a dictionary that maps each topic to its metadata
    """
    meta_dicts = {}

    query = 'SELECT meta_dict, topic FROM meta'
    rs = client.query(query)
    rs = list(rs.get_points())

    for point in rs:
        meta = point['meta_dict'].replace("u'", "\"").replace("'", "\"")
        meta_dicts[point['topic']] = json.loads(meta)

    return meta_dicts


def insert_meta(client, topic, meta, last_updated):
    """
    Insert or update metadata dictionary of a specific topic into the database.
    It will insert into 'meta' table

    :param client: InfluxDB client connected in historian_setup method.
    :param topic: topic that holds the metadata
    :param meta: metadata dictionary need to be inserted into database
    :param last_updated: timestamp that the metadata is inserted into database
    """

    json_body = [
        {
            "measurement": "meta",
            "tags": {
                "topic": topic
            },
            "time": 0,
            "fields": {
                "meta_dict": str(meta),
                "last_updated": last_updated
            }
        }
    ]

    client.write_points(json_body)


def insert_data_point(client, time, topic, source, value):
    """
    Insert one data point of a specific topic into the database.
    It will insert into 'data' table
    """

    json_body = [
        {
            "measurement": "data",
            "tags": {
                "topic": topic,
                "source": source
            },
            "time": time,
            "fields": {
                "value": value
            }
        }
    ]

    client.write_points(json_body)
