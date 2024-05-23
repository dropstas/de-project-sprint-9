import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel


class CDM_user_category_counters(BaseModel):
    user_id: uuid.UUID
    category_id: uuid.UUID
    category_name: str
    order_cnt: int


class CDM_user_product_counters(BaseModel):
    user_id: uuid.UUID
    product_id: uuid.UUID
    product_name: str
    order_cnt: int


class OrderCdmBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict

    def user_category_counters(self) -> List[CDM_user_category_counters]:
        category_cnt = []
        for i in self._dict['value']:
            user_id = i['user_id']
            category_id = i['object_id']
            category_name = i['object_name']
            order_cnt = i['order_cnt']
            category_cnt.append(
                CDM_user_category_counters(
                    user_id=user_id,
                    category_id=category_id,
                    category_name = category_name,
                    order_cnt=order_cnt
                        )
                    )
            return category_cnt
            
    
    def user_product_counters(self) -> List[CDM_user_product_counters]:
        product_cnt = []
        for i in self._dict['value']:
            user_id = i['user_id']
            product_id = i['object_id']
            product_name = i['object_name']
            order_cnt = i['order_cnt']
            product_cnt.append(
                CDM_user_product_counters(
                    user_id=user_id,
                    product_id=product_id,
                    product_name=product_name,
                    order_cnt=order_cnt
                        )
                    )
            return product_cnt





class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def cdm_product_insert(self, obj: CDM_user_product_counters) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters(
                            user_id,
                            product_id,
                            product_name,
                            order_cnt
                        )
                        VALUES(
                            %(user_id)s,
                            %(product_id)s,
                            %(product_name)s,
                            %(order_cnt)s
                        )
                        ON CONFLICT (user_id, product_id) DO UPDATE
                                    SET
                                    product_name = EXCLUDED.product_name,
                                    order_cnt = EXCLUDED.order_cnt;                   
                        """,
                    {
                        'user_id': obj.user_id,
                        'product_id': obj.product_id,
                        'product_name': obj.product_name,
                        'order_cnt': obj.order_cnt
                    }
                )

    def cdm_category_insert(self, obj: CDM_user_category_counters) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters(
                            user_id,
                            category_id,
                            category_name,
                            order_cnt
                        )
                        VALUES(
                            %(user_id)s,
                            %(category_id)s,
                            %(category_name)s,
                            %(order_cnt)s
                        )
                        ON CONFLICT (user_id, category_id) DO UPDATE
                                    SET
                                    category_name = EXCLUDED.category_name,
                                    order_cnt = EXCLUDED.order_cnt;                   
                        """,
                    {
                        'user_id': obj.user_id,
                        'category_id': obj.category_id,
                        'category_name': obj.category_name,
                        'order_cnt': obj.order_cnt
                    }
                )
