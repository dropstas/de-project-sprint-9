from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
    
    def user_product_counters(self, vals):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters(user_id, product_id,product_name, order_cnt)
                        VALUES (%(user_id)s, %(object_id)s, %(object_name)s, %(order_cnt)s)
                        ON CONFLICT (user_id, product_id) DO UPDATE
                                    SET
                                    product_name = EXCLUDED.product_name,
                                    order_cnt = EXCLUDED.order_cnt;""",
                    {
                        'user_id': vals[0],
                        'object_id': vals[1],
                        'object_name': vals[2],
                        'order_cnt': vals[3]
                    }
                )

    def user_category_counters(self, vals):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters(user_id, category_id,category_name, order_cnt)
                        VALUES (%(user_id)s, %(object_id)s, %(object_name)s, %(order_cnt)s)
                        ON CONFLICT (user_id, category_id) DO UPDATE
                                    SET
                                    category_name = EXCLUDED.category_name,
                                    order_cnt = EXCLUDED.order_cnt;""",
                    {
                        'user_id': vals[0],
                        'object_id': vals[1],
                        'object_name': vals[2],
                        'order_cnt': vals[3]
                    }
                )