from __future__ import print_function

import os
import uuid
from multiprocessing import Process
import time
from bluelens_spawning_pool import spawning_pool
from stylelens_product.products import Products
from stylelens_product.crawls import Crawls
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

MAX_PROCESS_NUM = int(os.environ['MAX_PROCESS_NUM'])

REDIS_HOST_CLASSIFY_QUEUE = 'bl:host:classify:queue'
REDIS_PRODUCT_IMAGE_PROCESS_QUEUE = 'bl:product:image:process:queue'
REDIS_CRAWL_VERSION = 'bl:crawl:version'
REDIS_CRAWL_VERSION_LATEST = 'latest'

SPAWNING_CRITERIA = 50
PROCESSING_TERM = 60

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-image-process')
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

def get_latest_crawl_version(rconn):
  value = rconn.hget(REDIS_CRAWL_VERSION, REDIS_CRAWL_VERSION_LATEST)
  if value is None:
    return None

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

def clear_product_queue(rconn):
  rconn.delete(REDIS_PRODUCT_IMAGE_PROCESS_QUEUE)

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

  except Exception as e:
    log.error(str(e) + ':' + host_code)


def spawn(uuid):
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
  pool.addContainerEnv(container, 'MAX_PROCESS_NUM', str(MAX_PROCESS_NUM))
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

def dispatch(rconn, version_id):
  global product_api

  size = rconn.llen(REDIS_PRODUCT_IMAGE_PROCESS_QUEUE)

  if size > 0 and size < MAX_PROCESS_NUM:
    for i in range(10):
      spawn(str(uuid.uuid4()))
    time.sleep(60*60*2)

  if size >= MAX_PROCESS_NUM and size < MAX_PROCESS_NUM * 10:
    for i in range(100):
      spawn(str(uuid.uuid4()))
    time.sleep(60*60*5)

  elif size >= MAX_PROCESS_NUM * 100:
    for i in range(300):
      spawn(str(uuid.uuid4()))
    time.sleep(60*60*10)

def prepare_products(rconn, version_id):
  global product_api
  offset = 0
  limit = 200

  clear_product_queue(rconn)
  try:
    while True:

      res = product_api.get_products_by_version_id(version_id=version_id,
                                                   is_processed=False,
                                                   offset=offset,
                                                   limit=limit)

      log.debug("Got " + str(len(res)) + ' products')
      for product in res:
        push_product_to_queue(product)

      if len(res) == 0:
        break
      else:
        offset = offset + limit

  except Exception as e:
    log.error(str(e))

def check_condition_to_start(version_id):
  global product_api

  product_api = Products()
  crawl_api = Crawls()

  try:
    # Check Crawling process is done
    total_crawl_size = crawl_api.get_size_crawls(version_id)
    crawled_size = crawl_api.get_size_crawls(version_id, status='done')
    if total_crawl_size != crawled_size:
      return False

    # Check Crawling process is done
    total_product_size = product_api.get_size_products(version_id)
    processed_size = product_api.get_size_products(version_id, is_processed=True)
    if total_product_size == processed_size:
      return False

    # Check Image processing queue is empty
    queue_size = rconn.llen(REDIS_PRODUCT_IMAGE_PROCESS_QUEUE)
    if queue_size != 0:
      return False

  except Exception as e:
    log.error(str(e))

  return True

def start(rconn):
  while True:
    version_id = get_latest_crawl_version(rconn)
    if version_id is not None:
      ok = check_condition_to_start(version_id)
      if ok is True:
        prepare_products(rconn, version_id)
        dispatch(rconn, version_id)
    time.sleep(60*10)

if __name__ == '__main__':
  log.info('Start bl-image-process:1')
  try:
    Process(target=start, args=(rconn,)).start()
  except Exception as e:
    log.error(str(e))
