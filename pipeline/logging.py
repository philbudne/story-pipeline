"""
Logging support: extend Python logging to add levels,
XXX emable sentry.io if configured

MUST be imported (by some file) before ANY calls to getLogger
"""

# MAYBE:
# add TRACE level (exists in log4j)?

import logging
import os

# PyPI:
try:
    import sentry_sdk
except ModuleNotFoundError:
    sentry_sdk = None

# add notice (missing syslog level)"
NOTICE = (logging.WARNING + logging.INFO) // 2
logging.addLevelName(NOTICE, 'NOTICE')

if sentry_sdk:
    # logging level mapping for breadcrumbs:
    sentry_sdk.integrations.logging.LOGGING_TO_EVENT_LEVEL[NOTICE] = "warning"

# XXX if using logging.handlers.SyslogHandler
#  .priority_names already has notice and alert (but not emerg)
#  BUT .priority_map (used by mapPriority method) needs to be extended

class Logger(logging.Logger):
    """subclass of logging.Logger with alert & notice methods"""

    def notice(self, msg, *args, **kwargs):
        """
        Log 'msg % args' with severity 'NOTICE'.

        NOTICE syslog level is higher than INFO, but lower than
        WARNING and is defined as "normal but significant condition"
        """
        if self.isEnabledFor(NOTICE):
            self._log(NOTICE, msg, args, **kwargs)

# have logging.getLogger return OUR Logger class
# MUST be before ANYONE calls logging.getLogger!!!
logging.setLoggerClass(Logger)

def sentry_setup() -> None:
    """
    see if sentry_sdk available and SENTRY_DSN set in environment.
    Call AFTER logging up and running!
    """

    dsn = os.environ.get('SENTRY_DSN')
    if dsn:
        if not sentry_sdk:
            print("sentry_sdk not available") # XXX log as error?
            return

        # XXX need environment (prod vs staging) and release (pkg version)
        print("call sentry_sdk.init")

if __name__ == '__main__':
    logging.basicConfig(level=NOTICE)
    sentry_setup()

    l = logging.getLogger(__name__)
    l.info("should not be seen")
    l.notice("notice this")
