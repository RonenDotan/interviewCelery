#from typing import Union
import uvicorn
from fastapi import FastAPI, File, UploadFile, HTTPException
import category_manager
import factory_storage
import service as be_service

app = FastAPI()


@app.get("/create_category/{category_name}")
async def create_category(category_name: str, region: str, type: str):
    """<p>
    Create A new category with region and type.\n
    In case The category exists - returns an exception.
    </p>"""
    try:
        ret_value = await service.create_category(category_name, region, type)
        return "Done"
    except Exception as e:
        print(e)
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/sum_type/{type}")
async def sum_type(type: str):
    """<p>
    Sums all numbers in Excel which are in a category of the requested type.\n
    If there are some files that arenot supported - disregard it. \n
    <b>An exception object will be returned with all the problematic files.</b>
    </p>"""    
    try:
        ret_value = await service.sum_in_section(
            section_type="type", section_value=type
        )
        if len(ret_value['Exceptions']) > 0:
            ret_value['Exceptions'] = list(map(str, ret_value['Exceptions']))
        else:
            del ret_value['Exceptions']
        return ret_value
    except Exception as e:
        print(e)
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/find_regions")
async def find_regions(search_term: str):
    """<p>
    All the regions which contains at least one Excel file containing the searched term\n
    <b>problematic files - none excel for the moment - would not raise an exception to the use but will be logged (in console for now).</b>
    </p>"""      
    try:
        ret_value = await service.find_regions(search_term=search_term)
        return ret_value
    except Exception as e:
        print(e)
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/uploadfile/{category_name}")
async def upload_file(category_name: str, file: UploadFile = File(...)):
    """<p>
    Uploading a file to the service\n
    <b>problematic files - none excel for the moment - would be saved (FFU?)</b>\n
    In case a file with the same name already xists in this category - will overwrite it.
    </p>"""          
    try:
        status = await service.upload_file(category_name, file)
        if status:
            return "SUCCEEDED"
        else:
            raise Exception("Error Uploading File")
    except Exception as e:
        print(e)
        raise HTTPException(status_code=404, detail=str(e))


if __name__ == "__main__":
    service = be_service.Service(
        storage=factory_storage.Factory("local"),
        managing_db=category_manager.CategoryManager(),
    )
    uvicorn.run(app, host="0.0.0.0", port=8000)
