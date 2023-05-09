"""
Pipeline Worker Definitions
"""

# possible to use tx_select to select transaction mode to get all/or nothing sends to multiple exchanges?

import argparse
import logging
import os
import pickle

# PyPI
import pika


class PipelineException(Exception):
    """base class for pipeline exceptions"""

logger = logging.getLogger(__name__)

# content types:
MIME_TYPE_PICKLE = 'application/python-pickle'

DEFAULT_ROUTING_KEY = 'default'

class Worker:
    """base class for AMQP/pika based pipeline worker"""
    def __init__(self, process_name: str, descr: str):
        self.process_name = process_name
        self.descr = descr
        self.args = None        # set by main

        # XXX maybe have command line options???
        # script/configure.py creates queues/exchanges with process-{in,out}
        # names based on pipeline.json file:
        self.input_queue_name = f"{self.process_name}-in"
        self.output_exchange_name = f"{self.process_name}-out"

    def define_options(self, ap: argparse.ArgumentParser):
        """
        subclass if additional options/argument needed by process;
        subclass methods _SHOULD_ call super() method!!
        """
        # environment variable automagically set in Dokku:
        default_url = os.environ.get('RABBITMQ_URL')  # set by Dokku
        ap.add_argument('--rabbitmq-url', '-U', dest='amqp_url',
                        default=default_url,
                        help="set RabbitMQ URL (default {default_url}")

    def main(self):
        # call before logargparser.my_parse_args:
        ap = argparse.ArgumentParser(self.process_name, self.descr)
        self.define_options(ap)
        self.args = ap.parse_args()

        print(dir(self.args))
        if not self.args.amqp_url:
            raise PipelineException('need RabbitMQ URL')
        parameters = pika.connection.URLParameters(self.args.amqp_url)

        with pika.BlockingConnection(parameters) as conn:
            logger.info("connected to rabbitmq: calling main_loop")
            self.main_loop(conn)

    def main_loop(self, conn: pika.BlockingConnection):
        raise PipelineException('must override main_loop!')

    def encode_message(self, data):
        # XXX allow control over encoding?
        # see ConsumerWorker decode_message!!!
        encoded = pickle.dumps(data)
        # return (content-type, content-encoding, body)
        return (MIME_TYPE_PICKLE, 'none', encoded)

    def send_message(self, chan, data, exchange=None,
                     routing_key : str = DEFAULT_ROUTING_KEY):
        # XXX wrap, and include message history?
        content_type, content_encoding, encoded = self.encode_message(data)
        chan.basic_publish(
            exchange or self.output_exchange_name,
            routing_key,
            encoded,            # body
            pika.BasicProperies(content_type=content_type))


class ConsumerWorker(Worker):
    def main_loop(self, conn: pika.BlockingConnection):
        """
        basic main_loop for a consumer.
        override for a producer!
        """
        chan = conn.channel()
        chan.basic_consume(self.input_queue_name, self.on_message)
        chan.start_consuming()

    def on_message(self, chan, method, properties, body):
        """basic_consume callback function"""

        decoded = self.decode_message(properties, body)
        self.process_message(chan, method, properties, decoded)

    def decode_message(self, properties, body):
        # XXX look at content-type to determine how to decode
        decoded = pickle.loads(body)  # decode payload
        # XXX extract & return message history?
        # XXX send stats on delay since last hop???
        return decoded

    def process_message(self, chan, method, properties, decoded):
        raise PipelineException("Worker.process_message not overridden")


class ListConsumerWorker(ConsumerWorker):
    """Pipeline worker that handles list of work items"""

    def process_message(self, chan, method, properties, decoded):
        results = []
        for item in decoded:
            # XXX return exchange name too???
            result = self.process_item(item)
            if result:
                # XXX have a "put" worker function to do splitting on demand?
                # (use transactions to get all-or-nothing???)
                results.append(result)
        self.send_results(chan, results)

    def process_item(self, item):
        raise PipelineException("ListWorker.process_item not overridden")

    def send_results(self, chan, items):
        # XXX split up into multiple msgs as needed!
        # XXX per-process default on items/msg?????
        self.send_message(chan, items)

def run(klass, *args, **kw):
    """
    run worker process, takes Worker subclass
    could, in theory create threads or asyncio tasks.
    """
    worker = klass(*args, **kw)
    worker.main()
