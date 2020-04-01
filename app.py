import connexion
import yaml
import json
import dateutil.parser
import logging.config
from connexion import NoContent
from pykafka import KafkaClient
from flask_cors import CORS, cross_origin

with open('log_conf.yaml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

with open('app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())

logger = logging.getLogger('basicLogger')

def parse_datetime(datestring):
    return dateutil.parser.parse(datestring)

def get_scan_in_nth(n):
    """ get the nth record received """
    logger.info("Getting nth scan")
    client = KafkaClient(hosts='{}:{}'.format(app_config['kafka']['server'], app_config['kafka']['port']))
    topic = client.topics[app_config['kafka']['topic']]
    consumer = topic.get_simple_consumer(auto_commit_enable=True,
                                         auto_commit_interval_ms=1000,
                                         consumer_timeout_ms=100,
                                         reset_offset_on_start=True)
    current_n = 0

    for m in consumer:
        if m is not None:
            msg_str = m.value.decode('utf-8')
            msg = json.loads(msg_str)

            if msg["type"] == "ScanRecord":
                current_n += 1
                if current_n == n:
                    logger.debug(msg)
                    return msg, 200

def get_body_info_average(startDate, endDate):
    """ get the nth record received """

    client = KafkaClient(hosts='{}:{}'.format(app_config['kafka']['server'], app_config['kafka']['port']))
    topic = client.topics[app_config['kafka']['topic']]
    consumer = topic.get_simple_consumer(auto_commit_enable=True,
                                         auto_commit_interval_ms=1000,
                                         consumer_timeout_ms=100,
                                         reset_offset_on_start=True)
    logger.info("Getting average weight")
    num_of_records = 0
    weight_sum = 0

    start = parse_datetime(startDate)
    end = parse_datetime(endDate)

    for m in consumer:
        if m is not None:
            msg_str = m.value.decode('utf-8')
            msg = json.loads(msg_str)
            date = parse_datetime(msg["datetime"])
            if msg["type"] == "BodyInfoUpdate" and date >= start and date <= end:
                num_of_records += 1
                weight_sum += msg['payload']['body_info']['weight']

    response = {'average_weight': weight_sum/num_of_records}
    logger.debug(response)

    if num_of_records == 0:
        return NoContent, 404

    return response, 200



app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml")
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'

if __name__ == "__main__":
    app.run(port=8110)