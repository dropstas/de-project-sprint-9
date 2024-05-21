from datetime import datetime
import sys, os
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from cdm_loader.repository import CdmRepository
from lib.kafka_connect import KafkaConsumer


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: logging.Logger) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = 30
        self._logger = logger

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(250):
            msg = self._consumer.consume()
            if not msg:
                break
            self._logger.info(f"{datetime.utcnow()}: Kafka consumer number {_} done. Message: {msg}")
            
            if msg['cdm_object'] == 'user_product_counters':
                for ms in msg['value']:
                    self._cdm_repository.user_product_counters(self._vals(ms))


            if msg['cdm_object'] == 'user_category_counters':
                for ms in msg['value']:
                    self._cdm_repository.user_category_counters(self._vals(ms))
                    
            self._logger.info(f"{datetime.utcnow()}: END MARTS")

    def _vals(self, ms):
            user_id = ms['user_id']
            object_id = ms['object_id']
            object_name = ms['object_name']
            order_cnt = ms['order_cnt']
            return (user_id, object_id, object_name, order_cnt)
    
