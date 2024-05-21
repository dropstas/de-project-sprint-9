from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect, load_dt, load_src) -> None:
        self._db = db
        self._load_dt = load_dt
        self._load_src = load_src
    
    def h_user(self, user_id, h_user_pk):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user(h_user_pk, user_id,load_dt,load_src)
                        VALUES (%(h_user_pk)s, %(user_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_user_pk) DO UPDATE
                        SET
                        user_id = EXCLUDED.user_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                        'h_user_pk': h_user_pk,
                        'user_id': user_id,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src
                    }
                )

    def h_product(self, product_id, h_product_pk):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product(h_product_pk, product_id,load_dt,load_src)
                        VALUES (%(h_product_pk)s, %(product_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_product_pk) DO UPDATE
                        SET
                        product_id = EXCLUDED.product_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {         
                        'h_product_pk': h_product_pk,
                        'product_id': product_id,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src
                    }
                )

    def h_category(self, category_name, h_category_pk):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category(h_category_pk, category_name,load_dt,load_src)
                        VALUES (%(h_category_pk)s, %(category_name)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_category_pk) DO UPDATE
                        SET
                        category_name = EXCLUDED.category_name,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                        'h_category_pk': h_category_pk,
                        'category_name': category_name,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src
                    }
                )

    def h_restaurant(self, restaurant_id, h_restaurant_pk):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(h_restaurant_pk, restaurant_id,load_dt,load_src)
                        VALUES (%(h_restaurant_pk)s, %(restaurant_id)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_restaurant_pk) DO UPDATE
                        SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                        'h_restaurant_pk': h_restaurant_pk,
                        'restaurant_id': restaurant_id,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src
                    }
                )

    def h_order(self, order_id, order_dt, h_order_pk):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order(h_order_pk, order_id, order_dt,load_dt,load_src)
                        VALUES (%(h_order_pk)s, %(order_id)s, %(order_dt)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (h_order_pk) DO UPDATE
                        SET
                        order_id = EXCLUDED.order_id,
                        order_dt = EXCLUDED.order_dt,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'order_id': order_id,
                        'order_dt': order_dt,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src
                    }
                )
    
    def l_order_product(self, hk_order_product_pk, h_order_pk, h_product_pk):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product(hk_order_product_pk, h_order_pk, h_product_pk,load_dt,load_src)
                        VALUES (%(hk_order_product_pk)s, %(h_order_pk)s, %(h_product_pk)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (hk_order_product_pk) DO UPDATE
                        SET
                        h_order_pk = EXCLUDED.h_order_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                        'hk_order_product_pk': hk_order_product_pk,
                        'h_order_pk': h_order_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src
                    }
                )
    
    def l_product_restaurant(self, hk_product_restaurant_pk, h_restaurant_pk, h_product_pk):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant(hk_product_restaurant_pk, h_restaurant_pk, h_product_pk,load_dt,load_src)
                        VALUES (%(hk_product_restaurant_pk)s, %(h_restaurant_pk)s, %(h_product_pk)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (hk_product_restaurant_pk) DO UPDATE
                        SET
                        h_restaurant_pk = EXCLUDED.h_restaurant_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                        'hk_product_restaurant_pk': hk_product_restaurant_pk,
                        'h_restaurant_pk': h_restaurant_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src
                    }
                )

    def l_product_category(self, hk_product_category_pk, h_category_pk, h_product_pk):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category(hk_product_category_pk, h_category_pk, h_product_pk,load_dt,load_src)
                        VALUES (%(hk_product_category_pk)s, %(h_category_pk)s, %(h_product_pk)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (hk_product_category_pk) DO UPDATE
                        SET
                        h_category_pk = EXCLUDED.h_category_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                        'hk_product_category_pk': hk_product_category_pk,
                        'h_category_pk': h_category_pk,
                        'h_product_pk': h_product_pk,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src
                    }
                )

    def l_order_user(self, hk_order_user_pk, h_order_pk, h_user_pk):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user(hk_order_user_pk, h_order_pk, h_user_pk,load_dt,load_src)
                        VALUES (%(hk_order_user_pk)s, %(h_order_pk)s, %(h_user_pk)s, %(load_dt)s, %(load_src)s)
                        ON CONFLICT (hk_order_user_pk) DO UPDATE
                        SET
                        h_order_pk = EXCLUDED.h_order_pk,
                        h_user_pk = EXCLUDED.h_user_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                    """,
                    {
                        'hk_order_user_pk': hk_order_user_pk,
                        'h_order_pk': h_order_pk,
                        'h_user_pk': h_user_pk,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src
                    }
                )

    def s_user_names(self, h_user_pk, username, userlogin, hk_user_names_hashdiff):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names(h_user_pk, username, userlogin,load_dt,load_src, hk_user_names_hashdiff)
                        VALUES (%(h_user_pk)s, %(username)s, %(userlogin)s, %(load_dt)s, %(load_src)s, %(hk_user_names_hashdiff)s)
                        ON CONFLICT (h_user_pk, load_dt) DO UPDATE
                        SET
                        username = EXCLUDED.username,
                        userlogin = EXCLUDED.userlogin,
                        load_src = EXCLUDED.load_src,
                        hk_user_names_hashdiff = EXCLUDED.hk_user_names_hashdiff;
                    """,
                    {
                        'h_user_pk': h_user_pk,
                        'username': username,
                        'userlogin': userlogin,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src,
                        'hk_user_names_hashdiff': hk_user_names_hashdiff
                    }
                )

    def s_product_names(self, h_product_pk, name, hk_product_names_hashdiff):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names(h_product_pk, name,load_dt,load_src, hk_product_names_hashdiff)
                        VALUES (%(h_product_pk)s, %(name)s, %(load_dt)s, %(load_src)s, %(hk_product_names_hashdiff)s)
                        ON CONFLICT (h_product_pk, load_dt) DO UPDATE
                        SET
                        name = EXCLUDED.name,
                        load_src = EXCLUDED.load_src,
                        hk_product_names_hashdiff = EXCLUDED.hk_product_names_hashdiff;
                    """,
                    {
                        'h_product_pk': h_product_pk,
                        'name': name,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src,
                        'hk_product_names_hashdiff': hk_product_names_hashdiff
                    }
                )

    def s_restaurant_names(self, h_restaurant_pk, name, hk_restaurant_names_hashdiff):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names(h_restaurant_pk, name,load_dt,load_src, hk_restaurant_names_hashdiff)
                        VALUES (%(h_restaurant_pk)s, %(name)s, %(load_dt)s, %(load_src)s, %(hk_restaurant_names_hashdiff)s)
                        ON CONFLICT (h_restaurant_pk, load_dt) DO UPDATE
                        SET
                        name = EXCLUDED.name,
                        load_src = EXCLUDED.load_src,
                        hk_restaurant_names_hashdiff = EXCLUDED.hk_restaurant_names_hashdiff;
                    """,
                    {
                        'h_restaurant_pk': h_restaurant_pk,
                        'name': name,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src,
                        'hk_restaurant_names_hashdiff': hk_restaurant_names_hashdiff
                    }
                )

    def s_order_cost(self, h_order_pk, order_cost, order_payment, hk_order_cost_hashdiff):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost(h_order_pk, cost, payment,load_dt,load_src, hk_order_cost_hashdiff)
                        VALUES (%(h_order_pk)s, %(order_cost)s, %(order_payment)s, %(load_dt)s, %(load_src)s, %(hk_order_cost_hashdiff)s)
                        ON CONFLICT (h_order_pk, load_dt) DO UPDATE
                        SET
                        cost = EXCLUDED.cost,
                        payment = EXCLUDED.payment,
                        load_src = EXCLUDED.load_src,
                        hk_order_cost_hashdiff = EXCLUDED.hk_order_cost_hashdiff;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'order_cost': order_cost,
                        'order_payment': order_payment,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src,
                        'hk_order_cost_hashdiff': hk_order_cost_hashdiff
                    }
                )

    def s_order_status(self, h_order_pk, status, hk_order_status_hashdiff):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(h_order_pk, status,load_dt,load_src, hk_order_status_hashdiff)
                        VALUES (%(h_order_pk)s, %(status)s, %(load_dt)s, %(load_src)s, %(hk_order_status_hashdiff)s)
                        ON CONFLICT (h_order_pk, load_dt) DO UPDATE
                        SET
                        status = EXCLUDED.status,
                        load_src = EXCLUDED.load_src,
                        hk_order_status_hashdiff = EXCLUDED.hk_order_status_hashdiff;
                    """,
                    {
                        'h_order_pk': h_order_pk,
                        'status': status,
                        'load_dt': self._load_dt,
                        'load_src': self._load_src,
                        'hk_order_status_hashdiff': hk_order_status_hashdiff
                    }
                )

    def user_product_counters_prep(self):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select hu.user_id, hp.product_id, spn.name as product_name, count(ho.order_id) from dds.h_user hu 
                    left join dds.l_order_user lou on hu.h_user_pk = lou.h_user_pk 
                    left join dds.l_order_product lop on lop.h_order_pk = lou.h_order_pk 
                    left join dds.h_product hp on hp.h_product_pk = lop.h_product_pk 
                    left join (select distinct h_product_pk , name from dds.s_product_names) spn on spn.h_product_pk = hp.h_product_pk 
                    left join dds.l_product_category lpc on lpc.h_product_pk = lop.h_product_pk 
                    left join dds.h_category hc on hc.h_category_pk = lpc.h_category_pk 
                    left join dds.h_order ho on ho.h_order_pk =lou.h_order_pk 
                    group by hu.user_id, hp.product_id, spn.name 
                    """
                )
                ans =cur.fetchall()
        return ans
    
    def user_category_counters_prep(self):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select hu.user_id, hc.h_category_pk as category_id , hc.category_name , count(ho.order_id) from dds.h_user hu 
                    left join dds.l_order_user lou on hu.h_user_pk = lou.h_user_pk 
                    left join dds.l_order_product lop on lop.h_order_pk = lou.h_order_pk 
                    left join dds.h_product hp on hp.h_product_pk = lop.h_product_pk 
                    left join (select distinct h_product_pk , name from dds.s_product_names) spn on spn.h_product_pk = hp.h_product_pk 
                    left join dds.l_product_category lpc on lpc.h_product_pk = lop.h_product_pk 
                    left join dds.h_category hc on hc.h_category_pk = lpc.h_category_pk 
                    left join dds.h_order ho on ho.h_order_pk =lou.h_order_pk 
                    group by hu.user_id, hc.h_category_pk, hc.category_name 
                    """
                )
                ans =cur.fetchall()
        return ans