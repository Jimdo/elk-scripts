#!/usr/bin/env python
import argparse
import requests

from requests.exceptions import ConnectionError
from time import sleep, time
from fabric.api import run, sudo, env
from fabric.decorators import task
from fabric.colors import red, yellow, green
from fabric.tasks import execute
from fabric.utils import abort
from fabric.contrib.console import confirm


def get_data_nodes():
    req = requests.get(
        'http://{0}:{1}/_nodes'.format(env.hosts[0], env.es_port)
    )
    nodes = req.json()['nodes']
    data_node_ips = [
        nodes[node_id]['ip']
        for node_id in nodes
        if (not nodes[node_id].get('attributes')
            or nodes[node_id]['attributes']['data'] == 'true')
    ]
    return data_node_ips


def setup_fabric():
    # disable SSH strict host key checking
    env.disable_known_hosts = True
    env.use_ssh_config = True

    if len(env.hosts) == 1:
        # get all data nodes
        env.hosts = get_data_nodes()

    env.parallel = False
    print 'Performing rolling restart on {0}'.format(', '.join(env.hosts))


def data_node_count():
    """
    return: number of data nodes in cluster
    """
    try:
        req = requests.get(
            'http://{0}:{1}/_cluster/health'.format(env.host, env.es_port))
    except ConnectionError:
        print red("Couldn't reach {0} to get data node count".format(env.host))
        return -1
    return req.json()['number_of_data_nodes']


def cluster_status():
    """
    Returns the status of the ES cluster
    """
    req = requests.get('http://{0}:{1}/_cluster/health'.format(env.host, env.es_port))
    status = req.json()['status']
    print 'Cluster status: ',
    if status == 'green':
        print green(status, bold=True)
    elif status == 'yellow':
        print yellow(status, bold=True)
    elif status == 'red':
        print red(status, bold=True)
    return status


def toggle_routing(enable=True):
    """
    Toggles the ES setting that controls shards being shuffled around
    on detection of node failure.

    @param enable: bool
    """
    mode = 'all'
    if not enable:
        mode = 'none'

    es_req_data = '{{"transient":{{"cluster.routing.allocation.enable":"{0}"}}}}'
    es_req = requests.put(
        'http://{0}:{1}/_cluster/settings'.format(env.host, env.es_port),
        data=es_req_data.format(mode)
    )

    if es_req.status_code != 200:
        print red('Error toggling routing allocation {0}'.format(es_req.text))
        return -1

    print 'Successfully set routing allocation to: {0}'.format(mode)


@task
def enable_puppet():
    # Check health of cluster before beginning
    if cluster_status() != 'green':
        abort('Cluster is not healthy, '
              'please ensure the cluster is healthy before continuing')

    run('! pgrep -fl "[p]uppet agent"')
    if env.act:
        sudo('/usr/sbin/puppetd --enable', shell=False)
    else:
        print 'would enable puppet'


@task
def disable_puppet():
    # Check health of cluster before beginning
    if cluster_status() != 'green':
        abort('Cluster is not healthy, '
              'please ensure the cluster is healthy before continuing')

    run('! pgrep -fl "[p]uppet agent"')
    if env.act:
        sudo('/usr/sbin/puppetd --disable', shell=False)
    else:
        print 'would disable puppet'


@task
def run_puppet():
    start = time()
    expected_data_nodes = data_node_count()

    # Check health of cluster before beginning
    if cluster_status() != 'green':
        abort(
            'Cluster is not healthy, please ensure the cluster is healthy before continuing')

    if env.act:
        # Disable Routing
        if toggle_routing(enable=False) == -1:
            abort('Cluster is not healthy, '
                'please ensure the cluster is healthy before continuing')

        # run puppet
        sudo('/usr/sbin/puppetd --enable', shell=False)
        result = sudo('/usr/sbin/puppetd --test --environment={0}'.format(env.environment), shell=False, warn_only=True)
        if result.return_code not in (0, 2):
            abort('puppet failed!')
        sudo('/usr/sbin/puppetd --disable', shell=False)

        while data_node_count() != expected_data_nodes:
            # Wait for node to start back up
            print 'Sleeping 15 seconds and waiting for node to come back online'
            sleep(15)

        # Re-enable allocation
        if toggle_routing(enable=True) == -1:
            abort('Aborting due to error enabling routing allocation')

        # Wait for cluster to fully heal
        while cluster_status() != 'green':
            print 'Cluster not green yet, sleeping 60 seconds...'
            sleep(60)
        end = time()
        print 'Node restart took: {0} seconds'.format(end - start)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--hosts', '--host', type=str, required=True,
        help='Comma-separated hosts or ips to act on. '
             'If only one is given, all data nodes in cluster are acted upon.'
    )
    parser.add_argument('--service-name', type=str, required=True)
    parser.add_argument('--port', type=int, default=9200)
    parser.add_argument('--environment', type=str, default='production')
    parser.add_argument('--act', action='store_true')

    args = parser.parse_args()

    env.hosts = [host for host in args.hosts.split(',')]
    env.environment = args.environment

    if not args.act:
        print red('Performing DRY RUN')

    env.es_port = args.port
    env.service_name = args.service_name
    env.act = args.act
    setup_fabric()
    if confirm('This process can take a long time, you should run this within a screen session, do you wish to continue?'):
        execute(disable_puppet)
        execute(run_puppet)
        execute(enable_puppet)

if __name__ == '__main__':
    main()
