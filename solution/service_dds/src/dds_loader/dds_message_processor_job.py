import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from datetime import datetime
import logging
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository



class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: logging.Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = 30
        self._logger = logger



    def run(self) -> None:
        
        self._logger.info(f"{datetime.utcnow()}: START")

        load_dt = datetime.utcnow()
        load_src = "KFK"


        for _ in range(25):
            msg = self._consumer.consume()
            if not msg:
                break
            self._logger.info(f"{datetime.utcnow()}: Kafka consumer number {_} done. Message: {msg}")

            #h_user insert
            user_id = msg['payload']['user']['id']
            h_user_pk = self._hash_generate(user_id)
            self._dds_repository.h_user(user_id, h_user_pk)
            self._logger.info(f"{datetime.utcnow()}: User id = {user_id} has been inserted")

            #s_user_names insert
            username = msg['payload']['user']['name']
            userlogin = msg['payload']['user']['login']
            hk_user_names_hashdiff = self._hash_generate(h_user_pk, username, userlogin, load_dt, load_src)
            self._dds_repository.s_user_names(h_user_pk, username, userlogin, hk_user_names_hashdiff)
            self._logger.info(f"{datetime.utcnow()}: User name info has been inserted")

            #h_order insert
            order_id = msg['payload']['id']
            order_dt = msg['payload']['date']
            h_order_pk = self._hash_generate(order_id)
            self._dds_repository.h_order(order_id, order_dt, h_order_pk)
            self._logger.info(f"{datetime.utcnow()}: Order id = {order_id} has been inserted")

            #s_order_cost
            order_cost = msg['payload']['cost']
            order_payment = msg['payload']['payment']
            hk_order_cost_hashdiff = self._hash_generate(h_order_pk, order_cost, order_payment, load_dt, load_src)
            self._dds_repository.s_order_cost(h_order_pk, order_cost, order_payment, hk_order_cost_hashdiff)
            self._logger.info(f"{datetime.utcnow()}: Order info has been inserted")

            #s_order_status
            order_status = msg['payload']['status']
            hk_order_status_hashdiff = self._hash_generate(h_order_pk, order_status, load_dt, load_src)
            self._dds_repository.s_order_status(h_order_pk, order_status, hk_order_status_hashdiff)
            self._logger.info(f"{datetime.utcnow()}: Order status has been inserted")

            #h_restaurant insert
            restaurant_id = msg['payload']['restaurant']['id']
            h_restaurant_pk = self._hash_generate(restaurant_id)
            self._dds_repository.h_restaurant(restaurant_id, h_restaurant_pk)
            self._logger.info(f"{datetime.utcnow()}: Restaurant id = {restaurant_id} has been inserted")

            #s_restaurant_names
            restaurant_name = msg['payload']['restaurant']['name']
            hk_restaurant_names_hashdiff = self._hash_generate(h_restaurant_pk, restaurant_name, load_dt, load_src)
            self._dds_repository.s_restaurant_names(h_restaurant_pk, restaurant_name, hk_restaurant_names_hashdiff)
            self._logger.info(f"{datetime.utcnow()}: Restaurant name info has been inserted")

                    
            #l_order_user insert
            hk_order_user_pk = self._hash_generate(order_id, user_id)
            self._dds_repository.l_order_user(hk_order_user_pk, h_order_pk, h_user_pk)
            self._logger.info(f"{datetime.utcnow()}: link order_user has been inserted")

            for row in msg['payload']['products']:
                
                #h_product insert
                product_id = row['id']
                h_product_pk = self._hash_generate(product_id)
                self._dds_repository.h_product(product_id, h_product_pk)
                self._logger.info(f"{datetime.utcnow()}: Product id = {product_id} has been inserted")

                #s_product_names
                name = row['name']
                hk_product_names_hashdiff = self._hash_generate(h_product_pk, name, load_dt, load_src)
                self._dds_repository.s_product_names(h_product_pk, name, hk_product_names_hashdiff)
                self._logger.info(f"{datetime.utcnow()}: Product name info has been inserted")

            
                #h_category insert
                category_name = row['category']
                h_category_pk = self._hash_generate(category_name)
                self._dds_repository.h_category(category_name, h_category_pk)
                self._logger.info(f"{datetime.utcnow()}: Category name = {category_name} has been inserted")

                #l_order_product insert
                hk_order_product_pk = self._hash_generate(order_id, product_id)
                self._dds_repository.l_order_product(hk_order_product_pk, h_order_pk, h_product_pk)
                self._logger.info(f"{datetime.utcnow()}: link order_product has been inserted")

                #l_product_restaurant insert
                hk_product_restaurant_pk = self._hash_generate(restaurant_id, product_id)
                self._dds_repository.l_product_restaurant(hk_product_restaurant_pk, h_restaurant_pk, h_product_pk)
                self._logger.info(f"{datetime.utcnow()}: link product_restaurant has been inserted")

                #l_product_category insert
                hk_product_category_pk = self._hash_generate(category_name, product_id)
                self._dds_repository.l_product_category(hk_product_category_pk, h_category_pk, h_product_pk)
                self._logger.info(f"{datetime.utcnow()}: link product_category has been inserted")


        user_product_counters_data = self._dds_repository.user_product_counters_prep()
        user_category_counters_data = self._dds_repository.user_category_counters_prep()

        self._producer.produce(self._cdm_val(user_product_counters_data, 'user_product_counters'))
        self._producer.produce(self._cdm_val(user_category_counters_data, 'user_category_counters'))


        self._logger.info(f"{datetime.utcnow()}: Kafka prepare data for marts has been loaded")


    def _cdm_val(self, prep_data, mart_name):
        values = []
        for row in prep_data:
                data = {
                        "user_id": str(row[0]),
                        "object_id": str(row[1]),
                        "object_name": str(row[2]),
                        "order_cnt": row[3]
                                    }
                values.append(data)
        dst_msg = {"cdm_object": mart_name,
            "value": values}
        return dst_msg
        
    def _hash_generate(self, text1, text2 = '', text3 = '', text4 = '', text5 = ''):
        text1 = str(text1)
        text2 = str(text2)
        text3 = str(text3)
        text4 = str(text4)
        text5 = str(text5)
        text = text1 + text2 + text3 + text4 + text5
        hash=0
        for ch in text:
            hash = ( hash*281  ^ ord(ch)*997) & 0xFFFFFFFF
        return hex(hash)[2:].upper().zfill(8) + '-' + hex(hash)[2:6].upper().zfill(4) + '-' + hex(hash)[2:6].upper().zfill(4)  + '-' + hex(hash)[2:6].upper().zfill(4)   + '-' + hex(hash)[2:].upper().zfill(12)
