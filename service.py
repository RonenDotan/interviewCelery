import asyncio


class Service:
    def __init__(self, storage, managing_db):
        self.storage = storage
        self.managing_db = managing_db

    async def create_category(self, category_name: str, region: str, type: str):
        new_category_id = await self.managing_db.create_category(
            category_name, region, type
        )
        return_value = await self.storage.create_category(new_category_id)
        return return_value

    async def upload_file(self, category_name: str, file):
        category_id = await self.managing_db.get_category(category_name)
        file2storeEncoded = await file.read()
        succeeded = await self.storage.save_file(
            category_id=category_id, file_name=file.filename, content=file2storeEncoded
        )
        return succeeded

    async def get_files_list_in_categories(self, section_type=None, section_value=None):
        categories = await self.managing_db.get_categories(section_type, section_value)
        categories = list(map(lambda x: x["id"], categories))
        file_paths = await asyncio.gather(
            *(
                self.storage.get_files_list_in_category(category_id)
                for category_id in categories
            )
        )
        file_paths = ["./" + str(x) for row in file_paths for x in row]
        return file_paths

    async def sum_in_section(self, section_type: str, section_value: str):
        file_paths = await self.get_files_list_in_categories(
            section_type, section_value
        )
        tasks = []
        for file_path in file_paths:
            tasks.append(asyncio.create_task(self.sum_in_file(file_path)))
        results = await asyncio.gather(*tasks)
        return {
            "sum": sum(
                list(filter(lambda result: not isinstance(result, Exception), results))
            ),
            "Exceptions": (
                list(filter(lambda result: isinstance(result, Exception), results))
            ),
        }


    async def sum_in_file(self, path):
        try:
            contents = await self.storage.get_file_content(path=path)
            total_sum = 0
            for _, content in contents.items():
                total_sum += content.sum(numeric_only=True).sum()

            return total_sum
        except Exception as e:
            print(e)
            return e

    async def find_regions(self, search_term):
        categories = await self.managing_db.get_categories()
        categories = list(map(lambda x: {"id": x["id"], "region":x["region"]}, categories))
        file_paths = await asyncio.gather(
            *(
                self.storage.get_files_list_in_category(category["id"])
                for category in categories
            )
        )
        file_paths_with_region = []
        for i,paths in enumerate(file_paths):
            for path in paths:
                file_paths_with_region.append({"path" : str(path), "region": categories[i]["region"]})


        tasks = []
        regions = []
        exceptions = []
        for files_path_with_region in file_paths_with_region:
            tasks.append(
                asyncio.create_task(self.search_in_file(files_path_with_region, search_term, regions, exceptions))
            )
        results = await asyncio.gather(*tasks)
        return {"regions" : list(set(regions)), "Exceptions":exceptions}

    
    async def search_in_file(self, file_path_with_region, search_term, regions, exceptions):
        try:
            path = file_path_with_region['path']
            region = file_path_with_region['region']
            if region in regions:
                return True
            
            contents = await self.storage.get_file_content(path=path)
            if region in regions:
                return True
            for _, content in contents.items():
                for i in range(0, content.shape[0]):
                    if (len(list(filter(lambda x: search_term in str(x),content.iloc[i].tolist(),))) > 0):
                        if region in regions:
                            return True
                        else:
                            regions.append(region)
                            return True
            return False
        except Exception as e:
            print(e)
            exceptions.append(e)
            return False