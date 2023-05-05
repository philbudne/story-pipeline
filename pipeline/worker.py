"""
Pipeline Worker Definitions
"""

# possible to use tx_select to select transaction mode to get all/or nothing sends to multiple exchanges?

import logging
import os
import pickle

# PyPI
import pika

class PipelineException:
    """base class for pipeline exceptions"""

logger = logging.getLogger(__name__)

# content types:
TYPE_PICKLE = 'application/python-pickle'

class Worker:
    """base class for AMQP/pika based pipeline worker"""
    def __init__(self):
        self.url = None                  # Optional[str]
        self.input_queue_name = None     # Optional[str]
        self.output_exchange_name = None # Optional[str]

    def main(self):
        # default values:
        # XXX call method to add command line parser options

        # XXX process command line for logging/queue params, open log file??
        logging.basicConfig()
        # XXX init sentry???

        # environment variable automagically set in Dokku:
        self.url = os.environ.get('RABBITMQ_URL')
        self.main_loop()

    def main_loop(self):
        # _COULD_ launch multiple threads?!
        parameters = pika.connection.URLParameters(self.url)
        with pika.BlockingConnection(parameters) as conn:
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

    def encode_message(self, data):
        # XXX allow control over encoding?
        encoded = pickle.dumps(data)
        # return (content-type, content-encoding, body)
        return (TYPE_PICKLE, 'none', encoded)

    def send_message(self, chan, data, exchange=None, routing_key='default'):
        # XXX wrap, and include message history?
        content_type, content_encoding, encoded = self.encode_message(data)
        chan.basic_publish(
            exchange or self.output_exchange_name,
            routing_key,
            encoded,            # body
            pika.BasicProperies(content_type=content_type))

    def process_message(self, chan, method, properties, decoded):
        raise PipelineException("Worker.process_message not overridden")


class ListWorker(Worker):
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
        self.send_message(chan, items)
