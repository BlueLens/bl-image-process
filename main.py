from __future__ import print_function

import os
import uuid
from multiprocessing import Process
import time
from bluelens_spawning_pool import spawning_pool
from stylelens_product.products import Products
import redis
import pickle

from bluelens_log import Logging

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
RELEASE_MODE = os.environ['RELEASE_MODE']
DB_PRODUCT_HOST = os.environ['DB_PRODUCT_HOST']
DB_PRODUCT_PORT = os.environ['DB_PRODUCT_PORT']
DB_PRODUCT_USER = os.environ['DB_PRODUCT_USER']
DB_PRODUCT_PASSWORD = os.environ['DB_PRODUCT_PASSWORD']
DB_PRODUCT_NAME = os.environ['DB_PRODUCT_NAME']
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY'].replace('"', '')
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY'].replace('"', '')

REDIS_HOST_CLASSIFY_QUEUE = 'bl:host:classify:queue'
REDIS_PRODUCT_IMAGE_PROCESS_QUEUE = 'bl:product:image:process:queue'
REDIS_CRAWL_VERSION = 'bl:crawl:version'
REDIS_CRAWL_VERSION_LATEST = 'latest'

SPAWNING_CRITERIA = 100
PROCESSING_TERM = 60

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-image-process')
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

def get_latest_crawl_version():
  value = rconn.hget(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST)
  log.debug(value)
  try:
    version_id = value.decode("utf-8")
  except Exception as e:
    log.error(str(e))
    version_id = None
  return version_id

def cleanup_products(host_code, version_id):
  global product_api
  try:
    res = product_api.delete_products_by_hostcode_and_version_id(host_code, version_id,
                                                                 except_version=True)
    log.debug(res)
  except Exception as e:
    log.error(e)


def push_product_to_queue(product):
  rconn.lpush(REDIS_PRODUCT_IMAGE_PROCESS_QUEUE, pickle.dumps(product))

def query(host_code, version_id):
  global product_api
  log.info('start query: ' + host_code)

  spawn_counter = 0

  q_offset = 0
  q_limit = 500

  try:
    while True:
      res = product_api.get_products_by_hostcode_and_version_id(host_code, version_id,
                                                                is_processed=False,
                                                                offset=q_offset, limit=q_limit)
      for p in res:
        push_product_to_queue(p)

      if len(res) == 0:
        break
      else:
        q_offset = q_offset + q_limit
        if spawn_counter < SPAWNING_CRITERIA:
          spawn_image_processor(str(uuid.uuid4()))
        spawn_counter = spawn_counter + 1

      time.sleep(60)
  except Exception as e:
    log.error(str(e) + ':' + host_code)


def spawn_image_processor(uuid):
  log.debug('RELEASE_MODE:' + RELEASE_MODE)

  pool = spawning_pool.SpawningPool()

  project_name = 'bl-image-processor-' + uuid
  log.debug('spawn_image-processor: ' + project_name)

  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setApiVersion('v1')
  pool.setKind('Pod')
  pool.setMetadataName(project_name)
  pool.setMetadataNamespace(RELEASE_MODE)
  pool.addMetadataLabel('name', project_name)
  pool.addMetadataLabel('group', 'bl-image-processor')
  pool.addMetadataLabel('SPAWN_ID', uuid)
  container = pool.createContainer()
  pool.setContainerName(container, project_name)
  pool.addContainerEnv(container, 'AWS_ACCESS_KEY', AWS_ACCESS_KEY)
  pool.addContainerEnv(container, 'AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY)
  pool.addContainerEnv(container, 'REDIS_SERVER', REDIS_SERVER)
  pool.addContainerEnv(container, 'REDIS_PASSWORD', REDIS_PASSWORD)
  pool.addContainerEnv(container, 'SPAWN_ID', uuid)
  pool.addContainerEnv(container, 'RELEASE_MODE', RELEASE_MODE)
  pool.addContainerEnv(container, 'DB_PRODUCT_HOST', DB_PRODUCT_HOST)
  pool.addContainerEnv(container, 'DB_PRODUCT_PORT', DB_PRODUCT_PORT)
  pool.addContainerEnv(container, 'DB_PRODUCT_USER', DB_PRODUCT_USER)
  pool.addContainerEnv(container, 'DB_PRODUCT_PASSWORD', DB_PRODUCT_PASSWORD)
  pool.addContainerEnv(container, 'DB_PRODUCT_NAME', DB_PRODUCT_NAME)
  pool.setContainerImage(container, 'bluelens/bl-image-processor:' + RELEASE_MODE)
  pool.setContainerImagePullPolicy(container, 'Always')
  pool.addContainer(container)
  pool.setRestartPolicy('Never')
  pool.spawn()

def dispatch_query_job(rconn):
  global product_api
  product_api = Products()
  while True:
    key, value = rconn.blpop([REDIS_HOST_CLASSIFY_QUEUE])
    version_id = get_latest_crawl_version()
    if version_id is not None:
      query(value.decode('utf-8'), version_id)

if __name__ == '__main__':
  log.info('Start bl-image-process:3')
  Process(target=dispatch_query_job, args=(rconn,)).start()
