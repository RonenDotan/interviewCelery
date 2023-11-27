import local_storage


def Factory(type="mysql"):
    """Factory Method"""
    storages = {
        "mysql": None,
        "S3": None,
        "local": local_storage.Local_Storage,
    }

    return storages[type]()
