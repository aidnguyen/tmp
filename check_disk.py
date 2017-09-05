import celery
import logging
import psutil
import yaml
import os
from datetime import timedelta
from raven.handlers.logging import SentryHandler
from raven.conf import setup_logging

import amz

TIME = 20
FORMAT = "%-30s %12s %12s %12s %7s %5s  %s\n"
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
# Setup celery
app = celery.Celery(broker='pyamqp://guest@localhost//')

# Read config file
with open('disk_config.yaml', 'r') as f:
    config = yaml.load(f)
DEFAULT_THRESHOLD = config["default"]
MAIN_THRESHOLDS = config["main"]


@celery.decorators.periodic_task(run_every=timedelta(seconds=TIME))
def df():
    # Setup Sentry handler
    sentry_handler = SentryHandler(amz.SENTRY_AI_TEST_PROJECT_DNS)
    sentry_handler.setLevel(logging.ERROR)
    setup_logging(sentry_handler)
    
    title = FORMAT % ("Device", "Size", "Used", "Avail", "Use% ", "Type", "Mount on")
    msg = ''
    error_msg = ''
    error_title = 'Device(s) usage above threshold\n'
    for part in psutil.disk_partitions(all=False):
        if os.name == 'nt':
            if 'cdrom' in part.opts or part.fstype == '':
                # skip cd-rom drives with no disk in it; they may raise
                # ENOENT, pop-up a Windows GUI error for a non-ready
                # partition or just hang.
                continue
        device = part.mountpoint
        usage = psutil.disk_usage(device)
        msg += (FORMAT % (part.device,
                          usage.total,
                          usage.used,
                          usage.free,
                          usage.percent,
                          part.fstype,
                          part.mountpoint))
        if (device in MAIN_THRESHOLDS.keys() and usage.percent >= MAIN_THRESHOLDS[device]) or \
                (device not in MAIN_THRESHOLDS.keys() and usage.percent >= DEFAULT_THRESHOLD):
            error_msg += (FORMAT % (part.device,
                                    usage.total,
                                    usage.used,
                                    usage.free,
                                    usage.percent,
                                    part.fstype,
                                    part.mountpoint))
    logging.error(error_title + title + error_msg)
    logging.info(title + msg)
