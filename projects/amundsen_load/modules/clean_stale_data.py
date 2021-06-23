from pyhocon import ConfigFactory
from databuilder.job.job import DefaultJob
from databuilder.task.task import DefaultTask
from databuilder.task.neo4j_staleness_removal_task import Neo4jStalenessRemovalTask

import logging
import sys
import os

import argparse

LOGGER = logging.getLogger("mainModule")
handler = logging.StreamHandler(sys.stdout)
LOGGER.addHandler(handler)


def create_metadata_stale_remover_job(neo4j_endpoint, neo4j_user, neo4j_password, job_last_update):

    task = Neo4jStalenessRemovalTask()

    job_config = ConfigFactory.from_dict({
        f'job.identifier': 'remove_stale_data_job',
        f'task.remove_stale_data.neo4j_endpoint': neo4j_endpoint,
        f'task.remove_stale_data.neo4j_user': neo4j_user,
        f'task.remove_stale_data.neo4j_password': neo4j_password,
        f'task.remove_stale_data.staleness_max_pct': 90,
        f'task.remove_stale_data.target_relations': ['READ', 'READ_BY'],
        f'task.remove_stale_data.milliseconds_to_expire': job_last_update
    })

    job_config = ConfigFactory.from_dict(job_config)
    job = DefaultJob(conf=job_config, task=task)
    job.launch()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='')
    parser.add_argument("--days_since_update", metavar='days', type=float, default=3,
                        help="Days since last update to retain")

    args = parser.parse_args()

    neo_endpoint = os.environ['NEO4J_ENDPOINT']
    neo_user = os.environ['NEO4J_USER']
    neo_password = os.environ['NEO4J_PASSWORD']

    # 86400000 is a day
    job_last_update = 86400000 * args.days

    create_metadata_stale_remover_job(neo_endpoint, neo_user, neo_password, job_last_update)