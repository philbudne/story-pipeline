"""
Pipeline Worker Definitions
"""

# possible to use tx_select to select transaction mode to get all/or nothing sends to multiple exchanges?

import argparse
import logging
import os
import pickle
import sys

# PyPI
import pika


class PipelineException(Exception):
    """base class for pipeline exceptions"""

logger = logging.getLogger(__name__)

# content types:
MIME_TYPE_PICKLE = 'application/python-pickle'

DEFAULT_ROUTING_KEY = 'default'

class Worker:
    """
    Base class for AMQP/pika based pipeline Worker.
    Producers (processes that have no input queue)
    should derive from this class
    """

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
        # XXX give env var name instead of value?
        ap.add_argument('--rabbitmq-url', '-U', dest='amqp_url',
                        default=default_url,
                        help="set RabbitMQ URL (default {default_url}")

    def main(self):
        # call before logargparser.my_parse_args:
        ap = argparse.ArgumentParser(self.process_name, self.descr)
        self.define_options(ap)
        self.args = ap.parse_args()

        if not self.args.amqp_url:
            raise PipelineException('need RabbitMQ URL')
        parameters = pika.connection.URLParameters(self.args.amqp_url)

        with pika.BlockingConnection(parameters) as conn:
            logger.info("connected to rabbitmq: calling main_loop")
            chan = conn.channel()
            self.main_loop(conn, chan)

    def main_loop(self, conn: pika.BlockingConnection, chan):
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
            pika.BasicProperties(content_type=content_type))


class ConsumerWorker(Worker):
    """Base class for Workers that consume messages"""

    # override this to allow enable input batching
    INPUT_BATCH = 1

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.input_msgs = []

    def main_loop(self, conn: pika.BlockingConnection, chan):
        """
        basic main_loop for a consumer.
        override for a producer!
        """
        # XXX allow environment/cmd-line override??
        if self.INPUT_BATCH > 1:
            chan.basic_qos(self.INPUT_BATCH, global_qos=False)  # RabbitMQ ext.
        chan.tx_select()        # enter transaction mode
        chan.basic_consume(self.input_queue_name, self.on_message)
        chan.start_consuming()

    def on_message(self, chan, method, properties, body):
        """
        basic_consume callback function
        """

        self.input_msgs.append( (method, properties, body) )
        if len(self.input_msgs) < self.INPUT_BATCH:
            return

        # here with full batch: start processing
        for m, p, b in self.input_msgs:
            decoded = self.decode_message(p, b)
            self.process_message(chan, m, p, decoded)
            # XXX check processing status??

        self.flush_output(self, chan)
        chan.tx_commit()

        # ack last message only:
        chan.basic_ack(delivery_tag=method.delivery_tag)
        self.input_msgs = []

        sys.stdout.flush()      # for redirection, supervisord

    def decode_message(self, properties, body):
        # XXX look at content-type to determine how to decode
        decoded = pickle.loads(body)  # decode payload
        # XXX extract & return message history?
        # XXX send stats on delay since last hop???
        return decoded

    def process_message(self, chan, method, properties, decoded):
        raise PipelineException("Worker.process_message not overridden")


    def flush_output(self):
        """hook for ListConsumer"""

class ListConsumerWorker(ConsumerWorker):
    """Pipeline worker that handles list of work items"""

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.output_items = []

    def process_message(self, chan, method, properties, decoded):
        results = []
        for item in decoded:
            # XXX return exchange name too?
            result = self.process_item(item)
            if result:
                # XXX append to per-exchange list?
                self.output_items(result)

    def flush_output(self, chan):
        # XXX split up into multiple msgs as needed!
        # XXX per-process (OUTPUT_BATCH) for max items/msg?????
        # XXX iterate for dict of lists of items by dest exchange??
        self.send_message(chan, self.output_items)
        self.output_items = []

    def process_item(self, item):
        raise PipelineException(
            "ListConsumerWorker.process_item not overridden")

def run(klass, *args, **kw):
    """
    run worker process, takes Worker subclass
    could, in theory create threads or asyncio tasks.
    """
    worker = klass(*args, **kw)
    worker.main()
