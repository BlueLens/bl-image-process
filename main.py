from __future__ import print_function

import os
import uuid
from multiprocessing import Process
import time
from bluelens_spawning_pool import spawning_pool
import redis

from bluelens_log import Logging

REDIS_SERVER = os.environ['REDIS_SERVER']
REDIS_PASSWORD = os.environ['REDIS_PASSWORD']
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY'].replace('"', '')
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY'].replace('"', '')

REDIS_PRODUCT_IMAGE_PROCESS_QUEUE = 'bl:product:image:process:queue'

SPAWNING_CRITERIA = 50

options = {
  'REDIS_SERVER': REDIS_SERVER,
  'REDIS_PASSWORD': REDIS_PASSWORD
}
log = Logging(options, tag='bl-image-process')
rconn = redis.StrictRedis(REDIS_SERVER, port=6379, password=REDIS_PASSWORD)

def spawn_image_processor(uuid):

  pool = spawning_pool.SpawningPool()

  project_name = 'bl-image-processor-' + uuid
  log.debug('spawn_image-processor: ' + project_name)

  pool.setServerUrl(REDIS_SERVER)
  pool.setServerPassword(REDIS_PASSWORD)
  pool.setApiVersion('v1')
  pool.setKind('Pod')
  pool.setMetadataName(project_name)
  pool.setMetadataNamespace('index')
  pool.addMetadataLabel('name', project_name)
  pool.addMetadataLabel('SPAWN_ID', uuid)
  container = pool.createContainer()
  pool.setContainerName(container, project_name)
  pool.addContainerEnv(container, 'AWS_ACCESS_KEY', AWS_ACCESS_KEY)
  pool.addContainerEnv(container, 'AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY)
  pool.addContainerEnv(container, 'REDIS_SERVER', REDIS_SERVER)
  pool.addContainerEnv(container, 'REDIS_PASSWORD', REDIS_PASSWORD)
  pool.addContainerEnv(container, 'SPAWN_ID', uuid)
  pool.setContainerImage(container, 'bluelens/bl-image-processor:latest')
  pool.addContainer(container)
  pool.setRestartPolicy('Never')
  pool.spawn()

def dispatch_image_processor(rconn):
  log.info('Start dispatch_image_processor')
  while True:
    len = rconn.llen(REDIS_PRODUCT_IMAGE_PROCESS_QUEUE)
    if len > SPAWNING_CRITERIA:
      spawn_image_processor(str(uuid.uuid4()))
    time.sleep(60)

if __name__ == '__main__':
  log.info('Start bl-image-process')
  Process(target=dispatch_image_processor, args=(rconn,)).start()
