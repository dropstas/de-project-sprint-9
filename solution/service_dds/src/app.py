import logging
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from datetime import datetime
from dds_loader.dds_message_processor_job import DdsMessageProcessor
from app_config import AppConfig
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository
from lib.pg import PgConnect

app = Flask(__name__)

cfg = AppConfig()


@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    
    app.logger.setLevel(logging.DEBUG)
    logging.basicConfig(filename='myapp.log', level=logging.INFO)

    kafka_consumer_instance = KafkaConsumer(host = cfg.kafka_host, port=cfg.kafka_port, user=cfg.kafka_consumer_username, password=cfg.kafka_consumer_password, topic=cfg.kafka_consumer_topic, group=cfg.kafka_consumer_group, cert_path=cfg.CERTIFICATE_PATH)
    pg_conn = PgConnect(cfg.pg_warehouse_host, cfg.pg_warehouse_port, cfg.pg_warehouse_dbname, cfg.pg_warehouse_user, cfg.pg_warehouse_password)
    dds_repository_instance = DdsRepository(pg_conn, datetime.utcnow(), "KFK")
    producer_instance = KafkaProducer(cfg.kafka_host,cfg.kafka_port,cfg.kafka_producer_username,cfg.kafka_producer_password,cfg.kafka_producer_topic,cfg.CERTIFICATE_PATH)
    logger = logging.getLogger(__name__)
    
    call = DdsMessageProcessor(kafka_consumer_instance,producer_instance,dds_repository_instance,None,logger)

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=call.run, trigger="interval", seconds=25)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
