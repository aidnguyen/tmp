import celery
import logging
import psutil
import yaml
from raven.handlers.logging import SentryHandler
from raven.conf import setup_logging

import amz

TIME = 300
FORMAT = "%-30s %8s %8s %8s %5s%% %9s  %s"
logger = logging.getLogger(__name__)
# Setup Sentry handler
sentry_handler = SentryHandler(amz.SENTRY_AI_TEST_PROJECT_DNS)
sentry_handler.setLevel(logging.ERROR)
setup_logging(sentry_handler)


@celery.decorators.periodic_task(run_every=timedelta(seconds=TIME))
def df():

    print(FORMAT % ("Device", "Total", "Used", "Free", "Use ", "Type", "Mount"))
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
            bytes2human(usage.total),
            bytes2human(usage.used),
            bytes2human(usage.free),
            int(usage.percent),
            part.fstype,
            part.mountpoint))

df()