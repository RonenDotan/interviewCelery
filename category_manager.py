import aiomysql


class CategoryManager:
    async def get_connection(self):
        self.conn = await aiomysql.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="password",
            db="interview",
            autocommit=True,
        )

    async def create_category(self, category_name, region, type):
        await self.get_connection()
        async with self.conn.cursor() as cur:
            await cur.execute(
                f'Insert into categories (category_name, region, type) VALUES ("{category_name}","{region}","{type}")'
            )
            new_category_id = cur.lastrowid
        self.conn.close()
        return new_category_id

    async def get_category(self, category_name):
        await self.get_connection()
        async with self.conn.cursor(aiomysql.DictCursor) as cur:
            ret_value = await cur.execute(
                f'SELECT * FROM categories WHERE category_name = "{category_name}"'
            )
            if ret_value != 0:
                r = await cur.fetchall()
                ret_id = r[0]["id"]
            else:
                self.conn.close()
                raise Exception("No categories with that name")
        self.conn.close()
        return ret_id

    async def get_categories(self, section_type=None, section_value=None):
        await self.get_connection()
        async with self.conn.cursor(aiomysql.DictCursor) as cur:
            if section_value and section_type:
                query = f'SELECT * \
                    FROM categories \
                    WHERE {section_type} = "{section_value}"'
            else:
                query = f"SELECT * \
                    FROM categories"

            ret_value = await cur.execute(query)
            if ret_value == 0:
                raise Exception(f"No {section_type} with value {section_value}")

            result = await cur.fetchall()
        self.conn.close()
        return result