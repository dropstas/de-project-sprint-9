from datetime import datetime
import sys, os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from cdm_loader.repository import CdmRepository, OrderCdmBuilder
from datetime import datetime
import logging
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

            builder = OrderCdmBuilder(msg)

            if msg['object_type'] == 'user_product_counters':
                for i in builder.user_product_counters():
                    self._cdm_repository.cdm_product_insert(i)

            if msg['object_type'] == 'user_category_counters':
                for i in builder.user_category_counters():
                    self._cdm_repository.cdm_category_insert(i)

                                
            self._logger.info(f"{datetime.utcnow()}: END MARTS")


