import re
import os
import sys
import math
import time
import uwsgi
from pathlib import Path
from flask import Flask, send_file, request, Response
import prometheus_client as prometheus
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily
from prometheus_client.exposition import choose_encoder

# if mule is not seen in more than 1h we declare ourselves as not alive
MULE_ALIVE_THRESHOLD = 3600

def formatDuration(seconds, suffix='ago'):
    """ Utility function returning duration humanly """

    time = { 'year': 31536000, 'day': 86400, 'hour': 3600, 'minute': 60, 'second': 1 }
    res = []
    if seconds == 0:
        return 'now'
    for key in time.keys():
        if seconds >= time[key]:
            val = math.floor(seconds / time[key])
            res.append(f'{val}{ f" {key}s" if val > 1 else f" {key}"}')
            seconds = seconds % time[key]
    return (re.sub(',([^,]*)$', r' and\1', ', '.join(res)) if len(res) > 1 else res[0]) + suffix

class PrometheusCollector(object):
    """ Collects statistics for prometheus"""

    def __init__(self, cache):
        self.cache = cache

    def collect(self):
        # names of the keys turned to gadgets
        keys = []

        # maps key to gadget
        gadgets_map = {}

        # collects prometheus gadgets during stats traversal
        mule = 1
        while True:
            # check end of the mules
            if not uwsgi.cache_exists(f'mule.{mule}.keys', self.cache):
                break
            # print(uwsgi.cache_get(f'mule.{mule}.keys', self.cache).decode('utf-8').split(','))
            # iterating mule.x.keys which holds all keys exported by mule x
            for key in uwsgi.cache_get(f'mule.{mule}.keys', self.cache).decode('utf-8').split(','):
                # globally unique key
                ukey = f'mule.{mule}.{key}'

                # each key have subkeys 'type', 'value' and 'help'
                metric_type = uwsgi.cache_get(f'{ukey}.type', self.cache).decode('utf-8')
                assert metric_type in ['inc', 'set']

                # one prometheus gadget for the same keys from all the mules
                metric_value = uwsgi.cache_get(f'{ukey}.value', self.cache).decode('utf-8')
                metric_help = uwsgi.cache_get(f'{ukey}.help', self.cache).decode('utf-8')
                prometheus_key = key.replace('.', '_').replace('-', '_')
                if mule > 1:
                    metric_help = ''
                if metric_type == 'inc':
                    gadget = CounterMetricFamily(prometheus_key, metric_help, labels=[f'mule'])
                else:
                    gadget = GaugeMetricFamily(prometheus_key, metric_help, labels=[f'mule'])
                gadget.add_metric([f'{mule}'], metric_value)
                # append unique key formatted for sorting the labeled gadgets
                keys.append(f'{key}_{mule}')
                gadgets_map[f'{key}_{mule}'] = gadget
            mule += 1
        for key in sorted(keys):
            yield gadgets_map[key]

def create_app():
    # create and configure the app
    app = Flask(__name__)
    app_dir = Path(app.root_path).parent.absolute()
    build_info = f'{app_dir}/BUILD-INFO.json'

    opt_cache2 = dict(item.split('=') for item in uwsgi.opt['cache2'].decode('utf-8').split(','))
    cache = opt_cache2['name']

    # register stats collector from uwsgi cache
    prometheus.REGISTRY.register(PrometheusCollector(cache))

    # unregister auto-generated stats
    prometheus.REGISTRY.unregister(prometheus.PROCESS_COLLECTOR)
    prometheus.REGISTRY.unregister(prometheus.PLATFORM_COLLECTOR)
    prometheus.REGISTRY.unregister(prometheus.GC_COLLECTOR)

    # handle ops requests
    @app.route("/ops/version")
    def version():
        return send_file(build_info, mimetype="application/json")

    @app.route("/ops/heartbeat")
    def heartbeat():
        start_time = int(float(uwsgi.cache_get('mule.1.started_on.value', cache).decode('utf-8')))
        up_time = int(float(uwsgi.cache_get('mule.1.up_time.value', cache).decode('utf-8')))
        last_seen = start_time + up_time
        long_ago = int(time.time() - last_seen)
        status = 200
        message = f'A mule last seen {formatDuration(long_ago, " ago")}.'
        if long_ago > MULE_ALIVE_THRESHOLD:
            status = 500
            message += f' That\'s over {MULE_ALIVE_THRESHOLD}, so it\'s probably dead.'
        else:
            message += f' The mule is considered alive!'
        return Response(message, status=status)

    # handle prometheus request
    @app.route("/metrics")
    def metrics():
        accept_header = request.headers.get("Accept")
        generate_latest, content_type = choose_encoder(accept_header)
        generated_content = generate_latest(prometheus.REGISTRY).decode('utf-8')
        return Response(response=generated_content, content_type=content_type)
    return app
