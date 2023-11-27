import aiofiles
import os
import pandas as pd
from pathlib import Path


class Local_Storage:
    async def create_category(self, category_id):
        new_dir = f"./files/{category_id}"
        os.mkdir(new_dir)
        return True

    async def save_file(self, category_id, file_name, content):
        dest_dir = f"./files/{category_id}"
        file_type = await self.get_file_type(file_name)
        path_and_name = os.path.join(dest_dir, file_name)
        async with aiofiles.open(path_and_name, "wb") as out:
            await out.write(content)
            await out.flush()
        return True

    async def get_files_list_in_category(self, category_id):
        dest_dir = f"./files/{category_id}"
        pathlist = Path(dest_dir).glob("*")
        return list(pathlist)

    async def get_file_type(self, path):
        return Path(path).suffix

    async def get_file_content(self, path):
        file_type = await self.get_file_type(path)
        dest_dir = path
        async with aiofiles.open(dest_dir, "rb") as f:
            contents = await f.read()
            if file_type == ".xlsx":
                contents = pd.read_excel(contents, sheet_name=None)
            else:
                raise Exception(f'file {path} is of type {file_type}.This is not supported yet') 

        return contents