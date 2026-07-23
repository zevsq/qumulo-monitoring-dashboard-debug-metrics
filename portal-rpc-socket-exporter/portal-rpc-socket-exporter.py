#!/usr/bin/env python3

import argparse
import json
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Sequence, Union
from urllib.parse import urlencode

from prometheus_client import REGISTRY, start_http_server
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily


MEASUREMENT_NAME = 'portal_rpc_socket'

# metrics/socket_metrics.c:158-190 in the qfsd source tree - field type source of truth.
GAUGE_FIELDS: Sequence[str] = (
    'duration_since_data_sent_ms',
    'duration_since_data_received_ms',
    'duration_since_ack_received_ms',
    'read_queue_size_bytes',
    'write_queue_size_bytes',
    'retransmit_timer_ms',
    'round_trip_time_us',
    'round_trip_time_variance_us',
    'send_slow_start_threshold',
    'send_congestion_window',
    'current_retransmits',
)
COUNTER_FIELDS: Sequence[str] = (
    'total_retransmits',
    'bytes_acked',
    'bytes_received',
    'segments_out',
    'segments_in',
)

# metrics/socket_metrics.c:101-119 - tags emitted for SOCKET_METRIC_CONNECTION_PORTAL_RPC, plus
# cluster/node_id, which we add ourselves since /v1/debug/metrics/ is scoped to one node at a time.
TAG_NAMES: Sequence[str] = ('type', 'remote_node', 'peer_cluster_uuid', 'channel', 'fs_id')
LABEL_NAMES: Sequence[str] = ('cluster', 'node_id') + tuple(TAG_NAMES)

MetricFamily = Union[GaugeMetricFamily, CounterMetricFamily]


@dataclass
class ClusterConfig:
    name: str
    host: str
    port: int
    access_token: Optional[str]
    insecure: bool
    timeout: int


def load_cluster_configs(path: str) -> List[ClusterConfig]:
    with open(path) as f:
        raw = json.load(f)
    configs = []
    for entry in raw['clusters']:
        configs.append(
            ClusterConfig(
                name=entry.get('name', entry['host']),
                host=entry['host'],
                port=entry.get('port', 8000),
                access_token=entry.get('access_token'),
                insecure=bool(entry.get('insecure', False)),
                timeout=int(entry.get('timeout', 20)),
            )
        )
    return configs


# ----------------- Raw REST client (no qumulo/qinternal import needed) -----------------
#
# /v1/debug/metrics/ and /v1/cluster/nodes/ are ordinary bearer-token-authenticated REST endpoints


def _build_url(config: ClusterConfig, path: str, params: Optional[Dict[str, Any]] = None) -> str:
    url = f'https://{config.host}:{config.port}{path}'
    if params:
        url = f'{url}?{urlencode(params)}'
    return url


def api_get_json(config: ClusterConfig, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    if not config.access_token:
        raise ValueError(f'cluster {config.name!r} needs an access_token')

    url = _build_url(config, path, params)
    req = urllib.request.Request(
        url, headers={'Authorization': f'Bearer {config.access_token}'}, method='GET'
    )
    context = ssl._create_unverified_context() if config.insecure else ssl.create_default_context()

    with urllib.request.urlopen(req, timeout=config.timeout, context=context) as resp:
        raw = resp.read()
        return json.loads(raw.decode('utf-8'))


def list_node_ids(config: ClusterConfig) -> List[int]:
    data = api_get_json(config, '/v1/cluster/nodes/')
    return [int(node['id']) for node in data]


def get_portal_rpc_socket_measurements(
    config: ClusterConfig, node_id: int
) -> List['dict[str, object]']:
    data = api_get_json(config, '/v1/debug/metrics/', params={'node-id': str(node_id)})
    return [m for m in data if m['measurement'] == MEASUREMENT_NAME]


def probe_cluster(config: ClusterConfig) -> None:
    # Same rationale as the original exporter's eager list_nodes() call: fail fast at startup on
    # an unreachable host or bad token, rather than the container silently never serving real data.
    list_node_ids(config)


class PortalRpcSocketCollector:
    def __init__(self, clusters: Dict[str, ClusterConfig]) -> None:
        self._clusters = clusters

    def collect(self) -> Iterator[MetricFamily]:
        gauges = {
            field: GaugeMetricFamily(
                f'qfsd_portal_rpc_socket_{field}',
                f'QFSD portal_rpc_socket {field} (gauge)',
                labels=LABEL_NAMES,
            )
            for field in GAUGE_FIELDS
        }
        counters = {
            field: CounterMetricFamily(
                f'qfsd_portal_rpc_socket_{field}',
                f'QFSD portal_rpc_socket {field} (counter)',
                labels=LABEL_NAMES,
            )
            for field in COUNTER_FIELDS
        }

        for cluster_name, config in self._clusters.items():
            for node_id in self._list_node_ids(config):
                for measurement in self._get_node_measurements(config, node_id):
                    label_values = [cluster_name, str(node_id)] + [
                        measurement['tags'].get(tag, '') for tag in TAG_NAMES
                    ]
                    for field in GAUGE_FIELDS:
                        if field in measurement['fields']:
                            gauges[field].add_metric(
                                label_values, float(measurement['fields'][field])
                            )
                    for field in COUNTER_FIELDS:
                        if field in measurement['fields']:
                            counters[field].add_metric(
                                label_values, float(measurement['fields'][field])
                            )

        yield from gauges.values()
        yield from counters.values()

    def _list_node_ids(self, config: ClusterConfig) -> List[int]:
        try:
            return list_node_ids(config)
        except (urllib.error.URLError, OSError, ValueError, json.JSONDecodeError) as e:
            print(f'ERROR: [{config.name}] failed to list cluster nodes: {e}', file=sys.stderr)
            return []

    def _get_node_measurements(
        self, config: ClusterConfig, node_id: int
    ) -> List['dict[str, object]']:
        try:
            return get_portal_rpc_socket_measurements(config, node_id)
        except (urllib.error.URLError, OSError, ValueError, json.JSONDecodeError) as e:
            print(
                f'ERROR: [{config.name}] failed to fetch debug metrics from node {node_id}: {e}',
                file=sys.stderr,
            )
            return []


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Prometheus exporter for QFSD portal_rpc_socket debug metrics (standalone, no source checkout needed)'
    )
    parser.add_argument(
        '--config', required=True, help='JSON file listing clusters to poll (see clusters.example.json)'
    )
    parser.add_argument(
        '--listen-address', default='0.0.0.0', help='Exporter bind address (default: 0.0.0.0)'
    )
    parser.add_argument(
        '--listen-port', type=int, default=9840, help='Exporter /metrics port (default: 9840)'
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    configs = load_cluster_configs(args.config)

    clusters: Dict[str, ClusterConfig] = {}
    for config in configs:
        try:
            probe_cluster(config)
            clusters[config.name] = config
        except (urllib.error.URLError, OSError, ValueError, json.JSONDecodeError) as e:
            print(f'ERROR: failed to connect to cluster {config.name!r}: {e}', file=sys.stderr)

    if not clusters:
        print('ERROR: no clusters connected successfully, exiting', file=sys.stderr)
        return 1

    REGISTRY.register(PortalRpcSocketCollector(clusters))
    start_http_server(args.listen_port, addr=args.listen_address)
    print(
        f'Serving /metrics for {len(clusters)} cluster(s) on {args.listen_address}:{args.listen_port}',
        file=sys.stderr,
    )

    while True:
        time.sleep(3600)


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        pass
