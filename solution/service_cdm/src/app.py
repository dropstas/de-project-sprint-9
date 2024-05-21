import logging
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor
from cdm_loader.repository import CdmRepository
from lib.kafka_connect import KafkaConsumer
from datetime import datetime
from app_config import AppConfig
from lib.pg import PgConnect

app = Flask(__name__)

cfg = AppConfig()

@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    
    app.logger.setLevel(logging.DEBUG)
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename='myapp.log', level=logging.INFO)
    pg_conn = PgConnect(cfg.pg_warehouse_host, cfg.pg_warehouse_port, cfg.pg_warehouse_dbname, cfg.pg_warehouse_user, cfg.pg_warehouse_password)
    cdm_repository = CdmRepository(pg_conn)
    kafka_consumer_instance = KafkaConsumer(host = cfg.kafka_host, port=cfg.kafka_port, user=cfg.kafka_consumer_username, password=cfg.kafka_consumer_password, topic=cfg.kafka_consumer_topic, group=cfg.kafka_consumer_group, cert_path=cfg.CERTIFICATE_PATH)
    
    logger.info(f"{datetime.utcnow()}: START")

    call = CdmMessageProcessor(kafka_consumer_instance,cdm_repository,None,logger)

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=call.run, trigger="interval", seconds=25)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', port = '8080', use_reloader=False)
