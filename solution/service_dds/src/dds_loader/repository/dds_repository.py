from lib.pg import PgConnect
from datetime import datetime
import uuid
from pydantic import BaseModel
from typing import Dict, Any, List


class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str


class H_Product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str 


class H_Category(BaseModel):
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str 

class H_Restaurant(BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str 

class H_Order(BaseModel):
    h_order_pk: uuid.UUID
    order_id: int
    order_dt: datetime
    load_dt: datetime
    load_src: str 

class L_Order_Product(BaseModel):
    hk_order_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str 

class L_Product_Restaurant(BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str 

class L_Product_Category(BaseModel):
    hk_product_category_pk: uuid.UUID
    h_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str 

class L_Order_User(BaseModel):
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str 

class S_User_Names(BaseModel):
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str 
    hk_user_names_hashdiff: uuid.UUID 

class S_Product_Names(BaseModel):
    h_product_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str 
    hk_product_names_hashdiff: uuid.UUID 

class S_Restaurant_Names(BaseModel):
    h_restaurant_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str 
    hk_restaurant_names_hashdiff: uuid.UUID 

class S_Order_Cost(BaseModel):
    h_order_pk: uuid.UUID
    cost: int
    payment: int
    load_dt: datetime
    load_src: str 
    hk_order_cost_hashdiff: uuid.UUID 

class S_Order_Status(BaseModel):
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str 
    hk_order_status_hashdiff: uuid.UUID 



class OrderDdsBuilder:
    def __init__(self, dict: Dict) -> None:
        self._dict = dict
        self.source_system = "X"
        self.order_ns_uuid = uuid.NAMESPACE_OID

    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))
    
    def h_user(self) -> H_User:
        user_id = self._dict['user']['id']
        return H_User(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def s_user_names(self) -> S_User_Names:
        user_id = self._dict['user']['id']
        username = self._dict['user']['name']
        userlogin = self._dict['user']['login']
        return S_User_Names(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            username=username,
            userlogin=userlogin,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_user_names_hashdiff=self._uuid(str(user_id)+str(username)+str(userlogin)+str(datetime.utcnow())+str(self.source_system))
        )

    def h_order(self) -> H_Order:
        order_id = self._dict['id']
        order_dt = self._dict['date']
        return H_Order(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            order_dt=order_dt,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )   

    def s_order_cost(self) -> S_Order_Cost:
        order_id = self._dict['id']
        order_cost = self._dict['cost']
        order_payment = self._dict['payment']
        return S_Order_Cost(
            h_order_pk=self._uuid(order_id),
            cost=order_cost,
            payment=order_payment,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_cost_hashdiff=self._uuid(str(order_id)+str(order_cost)+str(order_payment)+str(datetime.utcnow())+str(self.source_system))
        )  
    
    def s_order_status(self) -> S_Order_Status:
        order_id = self._dict['id']
        order_status = self._dict['status']
        return S_Order_Status(
            h_order_pk=self._uuid(order_id),
            status=order_status,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_status_hashdiff=self._uuid(str(order_id)+str(order_status)+str(datetime.utcnow())+str(self.source_system))
        )  

    def h_restaurant(self) -> H_Restaurant:
        restaurant_id = self._dict['restaurant']['id']
        return H_Restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )  

    def s_restaurant_names(self) -> S_Restaurant_Names:
        restaurant_id = self._dict['restaurant']['id']
        restaurant_name = self._dict['restaurant']['name']
        return S_Restaurant_Names(
            h_restaurant_pk=self._uuid(restaurant_id),
            name=restaurant_name,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_restaurant_names_hashdiff=self._uuid(str(restaurant_id)+str(restaurant_name)+str(datetime.utcnow())+str(self.source_system))
        )

    def l_order_user(self) -> L_Order_User:
        order_id = self._dict['id']
        user_id = self._dict['user']['id']
        return L_Order_User(
            hk_order_user_pk=self._uuid(str(order_id)+str(user_id)),
            h_order_pk=self._uuid(order_id),
            h_user_pk=self._uuid(user_id),
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def h_product(self) -> List[H_Product]:
        products = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                H_Product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=prod_id,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return products 
    
    def s_product_names(self) -> List[S_Product_Names]:
        product_names = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            prod_name = prod_dict['name']
            product_names.append(
                S_Product_Names(
                    h_product_pk=self._uuid(prod_id),
                    name=prod_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                    hk_product_names_hashdiff=self._uuid(str(prod_id)+str(prod_name)+str(datetime.utcnow())+str(self.source_system))
                )
            )

        return product_names 

    def h_category(self) -> List[H_Category]:
        category = []

        for cat_dict in self._dict['products']:
            cat_nm = cat_dict['category']
            category.append(
                H_Category(
                    h_category_pk=self._uuid(cat_nm),
                    category_name=cat_nm,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return category 

    def l_order_product(self) -> List[L_Order_Product]:
        order_product = []
        order_id = self._dict['id']

        for op in self._dict['products']:
            prod_id = op['id']
            order_product.append(
                L_Order_Product(
                    hk_order_product_pk=self._uuid(str(prod_id)+str(order_id)),
                    h_product_pk=self._uuid(prod_id),
                    h_order_pk=self._uuid(order_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return order_product 
    
    def l_product_restaurant(self) -> List[L_Product_Restaurant]:
        product_restaurant = []
        restaurant_id = self._dict['restaurant']['id']

        for pr in self._dict['products']:
            prod_id = pr['id']
            product_restaurant.append(
                L_Product_Restaurant(
                    hk_product_restaurant_pk=self._uuid(str(prod_id)+str(restaurant_id)),
                    h_product_pk=self._uuid(prod_id),
                    h_restaurant_pk=self._uuid(restaurant_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return product_restaurant 
    
    def l_product_category(self) -> List[L_Product_Category]:
        product_category = []

        for pc in self._dict['products']:
            cat_nm = pc['category']
            prod_id = pc['id']
            product_category.append(
                L_Product_Category(
                    hk_product_category_pk=self._uuid(str(prod_id)+str(cat_nm)),
                    h_product_pk=self._uuid(prod_id),
                    h_category_pk=self._uuid(cat_nm),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                )
            )

        return product_category 
    







class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert(self, obj: H_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user(
                            h_user_pk,
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(user_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_user_pk) DO UPDATE
                        SET
                        user_id = EXCLUDED.user_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;                    
                        """,
                    {
                        'h_user_pk': obj.h_user_pk,
                        'user_id': obj.user_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )
                

    def s_user_names_insert(self, obj: S_User_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names(
                            h_user_pk,
                            username,
                            userlogin,
                            load_dt,
                            load_src,
                            hk_user_names_hashdiff
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(username)s,
                            %(userlogin)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_user_names_hashdiff)s
                        )
                        ON CONFLICT (h_user_pk, load_dt) DO UPDATE
                        SET
                        username = EXCLUDED.username,
                        userlogin = EXCLUDED.userlogin,
                        load_src = EXCLUDED.load_src,
                        hk_user_names_hashdiff = EXCLUDED.hk_user_names_hashdiff;                   
                        """,
                    {
                        'h_user_pk': obj.h_user_pk,
                        'username': obj.username,
                        'userlogin': obj.userlogin,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_user_names_hashdiff': obj.hk_user_names_hashdiff
                    }
                )

    def h_order_insert(self, obj: H_Order) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order(
                            h_order_pk,
                            order_id,
                            order_dt,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(order_id)s,
                            %(order_dt)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO UPDATE
                        SET
                        order_id = EXCLUDED.order_id,
                        order_dt = EXCLUDED.order_dt,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;                    
                        """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'order_id': obj.order_id,
                        'order_dt': obj.order_dt,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def s_order_cost_insert(self, obj: S_Order_Cost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost(
                            h_order_pk,
                            cost,
                            payment,
                            load_dt,
                            load_src,
                            hk_order_cost_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(cost)s,
                            %(payment)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_cost_hashdiff)s
                        )
                        ON CONFLICT (h_order_pk, load_dt) DO UPDATE
                        SET
                        cost = EXCLUDED.cost,
                        payment = EXCLUDED.payment,
                        load_src = EXCLUDED.load_src,
                        hk_order_cost_hashdiff = EXCLUDED.hk_order_cost_hashdiff;                    
                        """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'cost': obj.cost,
                        'payment': obj.payment,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_order_cost_hashdiff': obj.hk_order_cost_hashdiff
                    }
                )

    def s_order_status_insert(self, obj: S_Order_Status) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(
                            h_order_pk,
                            status,
                            load_dt,
                            load_src,
                            hk_order_status_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(status)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_status_hashdiff)s
                        )
                        ON CONFLICT (h_order_pk, load_dt) DO UPDATE
                        SET
                        status = EXCLUDED.status,
                        load_src = EXCLUDED.load_src,
                        hk_order_status_hashdiff = EXCLUDED.hk_order_status_hashdiff;                   
                        """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'status': obj.status,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_order_status_hashdiff': obj.hk_order_status_hashdiff
                    }
                )

    def h_restaurant_insert(self, obj: H_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(
                            h_restaurant_pk,
                            restaurant_id,
                            load_dt,
                            load_src
                            )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(restaurant_id)s,
                            %(load_dt)s,
                            %(load_src)s
                            )
                        ON CONFLICT (h_restaurant_pk) DO UPDATE
                        SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                  """,
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'restaurant_id': obj.restaurant_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def s_restaurant_names_insert(self, obj: S_Restaurant_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names(
                            h_restaurant_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_restaurant_names_hashdiff
                            )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_restaurant_names_hashdiff)s
                            )
                        ON CONFLICT (h_restaurant_pk, load_dt) DO UPDATE
                        SET
                        name = EXCLUDED.name,
                        load_src = EXCLUDED.load_src,
                        hk_restaurant_names_hashdiff = EXCLUDED.hk_restaurant_names_hashdiff;
                  """,
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'name': obj.name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_restaurant_names_hashdiff': obj.hk_restaurant_names_hashdiff
                    }
                )

    def l_order_user_insert(self, obj: L_Order_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user(
                            hk_order_user_pk,
                            h_order_pk,
                            h_user_pk,
                            load_dt,
                            load_src
                            )
                        VALUES(
                            %(hk_order_user_pk)s,
                            %(h_order_pk)s,
                            %(h_user_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                            )
                        ON CONFLICT (hk_order_user_pk) DO UPDATE
                        SET
                        h_order_pk = EXCLUDED.h_order_pk,
                        h_user_pk = EXCLUDED.h_user_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;
                  """,
                    {
                        'hk_order_user_pk': obj.hk_order_user_pk,
                        'h_order_pk': obj.h_order_pk,                
                        'h_user_pk': obj.h_user_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                    }
                )

    def h_product_insert(self, obj: H_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product(
                            h_product_pk,
                            product_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(product_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_product_pk) DO UPDATE
                        SET
                        product_id = EXCLUDED.product_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;                    
                        """,
                    {
                        'h_product_pk': obj.h_product_pk,
                        'product_id': obj.product_id,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )
    
    def s_product_names_insert(self, obj: S_Product_Names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names(
                            h_product_pk,
                            name,
                            load_dt,
                            load_src,
                            hk_product_names_hashdiff
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_product_names_hashdiff)s
                        )
                        ON CONFLICT (h_product_pk, load_dt) DO UPDATE
                        SET
                        name = EXCLUDED.name,
                        load_src = EXCLUDED.load_src,
                        hk_product_names_hashdiff = EXCLUDED.hk_product_names_hashdiff;                   
                        """,
                    {
                        'h_product_pk': obj.h_product_pk,
                        'name': obj.name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_product_names_hashdiff': obj.hk_product_names_hashdiff
                    }
                )

    def h_category_insert(self, obj: H_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category(
                            h_category_pk,
                            category_name,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_category_pk)s,
                            %(category_name)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_category_pk) DO UPDATE
                        SET
                        category_name = EXCLUDED.category_name,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;                   
                        """,
                    {
                        'h_category_pk': obj.h_category_pk,
                        'category_name': obj.category_name,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )
    
    def l_order_product_insert(self, obj: L_Order_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product(
                            hk_order_product_pk,
                            h_product_pk,
                            h_order_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_product_pk)s,
                            %(h_product_pk)s,
                            %(h_order_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_product_pk) DO UPDATE
                        SET
                        h_order_pk = EXCLUDED.h_order_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;                   
                        """,
                    {
                        'hk_order_product_pk': obj.hk_order_product_pk,
                        'h_order_pk': obj.h_order_pk,
                        'h_product_pk': obj.h_product_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def l_product_restaurant_insert(self, obj: L_Product_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant(
                            hk_product_restaurant_pk,
                            h_restaurant_pk,
                            h_product_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_restaurant_pk)s,
                            %(h_restaurant_pk)s,
                            %(h_product_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_restaurant_pk) DO UPDATE
                        SET
                        h_restaurant_pk = EXCLUDED.h_restaurant_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;                 
                        """,
                    {
                        'hk_product_restaurant_pk': obj.hk_product_restaurant_pk,
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'h_product_pk': obj.h_product_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def l_product_category_insert(self, obj: L_Product_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category(
                            hk_product_category_pk,
                            h_category_pk,
                            h_product_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_category_pk)s,
                            %(h_category_pk)s,
                            %(h_product_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_category_pk) DO UPDATE
                        SET
                        h_category_pk = EXCLUDED.h_category_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src;              
                        """,
                    {
                        'hk_product_category_pk': obj.hk_product_category_pk,
                        'h_category_pk': obj.h_category_pk,
                        'h_product_pk': obj.h_product_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def user_product_counters_prep(self):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select hu.h_user_pk as user_id, hp.h_product_pk as product_id, spn.name as product_name, count(ho.order_id) from dds.h_user hu 
                    left join dds.l_order_user lou on hu.h_user_pk = lou.h_user_pk 
                    left join dds.l_order_product lop on lop.h_order_pk = lou.h_order_pk 
                    left join dds.h_product hp on hp.h_product_pk = lop.h_product_pk 
                    left join (select h_product_pk , name, row_number() over(partition by h_product_pk , name order by load_dt desc) rn from dds.s_product_names) spn on spn.h_product_pk = hp.h_product_pk and rn = 1 
                    left join dds.l_product_category lpc on lpc.h_product_pk = lop.h_product_pk 
                    left join dds.h_category hc on hc.h_category_pk = lpc.h_category_pk 
                    left join dds.h_order ho on ho.h_order_pk =lou.h_order_pk 
                    group by hu.h_user_pk, hp.h_product_pk, spn.name 
                    """
                )
                ans =cur.fetchall()
        return ans

    def user_category_counters_prep(self):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select hu.h_user_pk as user_id, hc.h_category_pk as category_id , hc.category_name , count(ho.order_id) from dds.h_user hu 
                    left join dds.l_order_user lou on hu.h_user_pk = lou.h_user_pk 
                    left join dds.l_order_product lop on lop.h_order_pk = lou.h_order_pk 
                    left join dds.h_product hp on hp.h_product_pk = lop.h_product_pk 
                    left join (select h_product_pk , name, row_number() over(partition by h_product_pk , name order by load_dt desc) rn from dds.s_product_names) spn on spn.h_product_pk = hp.h_product_pk and rn = 1 
                    left join dds.l_product_category lpc on lpc.h_product_pk = lop.h_product_pk 
                    left join dds.h_category hc on hc.h_category_pk = lpc.h_category_pk 
                    left join dds.h_order ho on ho.h_order_pk =lou.h_order_pk 
                    group by hu.h_user_pk, hc.h_category_pk, hc.category_name 
                    """
                )
                ans =cur.fetchall()
        return ans
