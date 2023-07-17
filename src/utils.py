from pyspark.sql import SparkSession


def getSparkSession():
    return SparkSession.builder \
        .appName("Data Quality").enableHiveSupport() \
        .getOrCreate()



def read_file(file_path):
    keys = {}
    is_json_file = file_path.endswith(JSON_EXTENSION)
    is_properties_file = file_path.endswith(PROPERTIES_EXTENSION)
    global data

    if file_path.startswith(GOOGLE_STORAGE_PATH):
        data = gs_reader(file_path)

        if is_json_file:
            data = json.loads(data)

        if is_properties_file:

            for line in data.splitlines():
                if EQUAL_TO in line:
                    name, value = line.split(EQUAL_TO, 1)
                    keys[name.strip()] = value.strip()
            data = keys
    else:
        if is_json_file:
            with open(file_path) as f:
                data = json.loads(f.read())

        if is_properties_file:
            with open(file_path) as f:
                for line in f:
                    if EQUAL_TO in line:
                        name, value = line.split(EQUAL_TO, 1)
                        keys[name.strip()] = value.strip()
                data = keys

    return data


def gs_reader(path):
    length_of_google_storage_prefix = 5
    bucket_name = path[length_of_google_storage_prefix:].split(FORWARD_SLASH)[ZERO]
    file_name = path[6 + len(bucket_name):]
    bucket = Client().get_bucket(bucket_name)
    return bucket.get_blob(file_name).download_as_text(encoding="utf-8")
