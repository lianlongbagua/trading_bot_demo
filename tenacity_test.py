import logging
import sys

from tenacity import retry, stop_after_attempt, after_log

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(3), after=after_log(logger, logging.DEBUG))
def raise_my_exception():
    raise Exception("Fail")


raise_my_exception()
