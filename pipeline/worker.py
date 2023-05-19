"""
Pipeline Worker Definitions
"""

import argparse
import logging
import os
import pickle
import sys
import time

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

        # ~sigh~ wanted to avoid this, but "call_later" requires it
        self.connection = None

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
        ap = argparse.ArgumentParser(self.process_name, self.descr)
        self.define_options(ap)
        self.args = ap.parse_args()

        if not self.args.amqp_url:
            raise PipelineException('need RabbitMQ URL')
        parameters = pika.connection.URLParameters(self.args.amqp_url)

        self.connection = pika.BlockingConnection(parameters)
        logger.info(f"connected to {self.args.amqp_url}")
        chan = self.connection.channel()
        self.main_loop(self.connection, chan)

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

    # for generators:
    def send_items(self, chan, items):
        print("send_items", len(items))  # DEBUG logging?
        sys.stdout.flush()
        # XXX split up into multiple msgs as needed!
        # XXX per-process (OUTPUT_BATCH) for max items/msg?????
        # XXX take dest exchange??
        # XXX perform wrapping?
        self.send_message(chan, items)


class ConsumerWorker(Worker):
    """Base class for Workers that consume messages"""

    # XXX maybe allow command line args, environment overrides?
    # override this to allow enable input batching
    INPUT_BATCH_MSGS = 1

    # if INPUT_BATCH_MSGS > 1, wait no longer than INPUT_BATCH_SECS after 
    # first message, then process messages on hand:
    INPUT_BATCH_SECS = 120

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.input_msgs = []
        self.input_timer = None

    def main_loop(self, conn: pika.BlockingConnection, chan):
        """
        basic main_loop for a consumer.
        override for a producer!
        """
        chan.tx_select()        # enter transaction mode
        # set "prefetch" limit so messages get distributed among workers:
        chan.basic_qos(prefetch_count=self.INPUT_BATCH_MSGS*2)

        arguments = {}
        # if batching multiple input messages,
        # set consumer timeout accordingly
        if self.INPUT_BATCH_MSGS > 1 and self.INPUT_BATCH_SECS:
            # add a small grace period, convert to milliseconds
            ms = (self.INPUT_BATCH_SECS + 10) * 1000
            arguments['x-consumer-timeout'] = ms
        chan.basic_consume(self.input_queue_name, self.on_message,
                           arguments=arguments)

        chan.start_consuming()

    def on_message(self, chan, method, properties, body):
        """
        basic_consume callback function
        """

        print("on_message", method.delivery_tag)
        sys.stdout.flush()

        self.input_msgs.append( (method, properties, body) )

        if len(self.input_msgs) < self.INPUT_BATCH_MSGS:
            # here only when batching multiple msgs
            if self.input_timer is None and self.INPUT_BATCH_SECS:
                self.input_timer = \
                    self.connection.call_later(self.INPUT_BATCH_SECS,
                                               lambda : self._process_messages(chan))
            return

        # here with full batch: start processing
        if self.input_timer:
            self.connection.remove_timeout(self.input_timer)
            self.input_timer = None
        self._process_messages(chan)

    def _process_messages(self, chan):
        """
        Here w/ INPUT_BATCH_MSGS or
        INPUT_BATCH_SECS elapsed after first message
        """
        for m, p, b in self.input_msgs:
            # XXX wrap in try? reject bad msgs??
            decoded = self.decode_message(p, b)
            self.process_message(chan, m, p, decoded)
            # XXX check processing status?? reject bad msgs?
            # XXX increment counters based on status??

        self.end_of_batch(chan)
        self.flush_output(chan)  # generate message(s)
        chan.tx_commit()         # commit messages

        # ack last message only:
        multiple = len(self.input_msgs) > 1
        tag = self.input_msgs[-1][0].delivery_tag # tag from last message
        print("ack", tag, multiple)
        chan.basic_ack(delivery_tag=tag, multiple=multiple)
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

    def flush_output(self, chan):
        """hook for ListConsumer"""

    def end_of_batch(self, chan):
        """hook for batch processors (ie; write to database)"""

class ListConsumerWorker(ConsumerWorker):
    """Pipeline worker that handles list of work items"""

    def __init__(self, process_name: str, descr: str):
        super().__init__(process_name, descr)
        self.output_items = []

    def process_message(self, chan, method, properties, decoded):
        results = []
        print("process_message", len(decoded), "items")  # DEBUG logging?
        t0 = time.time()
        items = 0
        for item in decoded:
            # XXX return exchange name too?
            result = self.process_item(item)
            # XXX increment counters based on result??
            items += 1
            if result:
                # XXX append to per-exchange list?
                self.output_items.append(result)
        print(f"{os.getpid()} processed {items} items in {(time.time()-t0):.6g} sec")
        sys.stdout.flush()

    def flush_output(self, chan):
        # XXX iterate for dict of lists of items by dest exchange??
        if self.output_items:
            self.send_items(chan, self.output_items)
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
