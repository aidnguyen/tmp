import celery
import logging
import psutil
import yaml
import os
from datetime import timedelta
from raven.handlers.logging import SentryHandler
from raven.conf import setup_logging

import amz

TIME = 300
FORMAT = "%-30s %12s %12s %12s %7s %5s  %s"
logger = logging.getLogger(__name__)
# Setup Sentry handler
# sentry_handler = SentryHandler(amz.SENTRY_AI_TEST_PROJECT_DNS)
# sentry_handler.setLevel(logging.ERROR)
# setup_logging(sentry_handler)



@celery.decorators.periodic_task(run_every=timedelta(seconds=TIME))
def df():

    print(FORMAT % ("Device", "Size", "Used", "Avail", "Use% ", "Type", "Mount on"))
    for part in psutil.disk_partitions(all=False):
        if os.name == 'nt':
            if 'cdrom' in part.opts or part.fstype == '':
                # skip cd-rom drives with no disk in it; they may raise
                # ENOENT, pop-up a Windows GUI error for a non-ready
                # partition or just hang.
                continue
        usage = psutil.disk_usage(part.mountpoint)
        print(FORMAT % (
            part.device,
            usage.total,
            usage.used,
            usage.free,
            usage.percent,
            part.fstype,
            part.mountpoint))

df()