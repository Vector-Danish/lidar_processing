import laspy
import time
import os
from botocore.config import Config
import boto3
from decouple import config

ACCESS_KEY = config('ACCESS_KEY')
SECRET_KEY = config('SECRET_KEY')
REGION_NAME = config('REGION_NAME')
BUCKET_NAME = config("BUCKET_NAME")
FILE_PATH = "big_laz/"

def breakLazFile(local_file_path, save_folder_name):
    if not os.path.exists(save_folder_name):
        os.makedirs(save_folder_name)
        print(f"Directory {save_folder_name} created.")
    else:
        print(f"Directory {save_folder_name} already exists.")

    with laspy.open(local_file_path) as f:

        INCREMENT = 1
        print(f"Total data in parent laz file: {f.header.point_count}")
        start = time.time()

        for laz in f.chunk_iterator(100000):
            header = laspy.LasHeader(point_format=3, version="1.2")

            data = laspy.LasData(header)
            data.x = laz.x
            data.y = laz.y
            data.z = laz.z
            data.red = laz.red
            data.green = laz.green
            data.blue = laz.blue

            filename = f"{INCREMENT}.laz"

            laz_name = filename
            data.write(os.path.join(save_folder_name, laz_name))

            INCREMENT += 1

        end = time.time()

    print(f"Total time taken to create chunk files: {end - start} seconds with {INCREMENT - 1} children")

    for file_name in os.listdir(save_folder_name):
        file_path = os.path.join(save_folder_name, file_name)
        os.remove(file_path)
    os.remove(local_file_path)

def downloadS3File(object_url):
    
    my_config = Config(
                    region_name = REGION_NAME,
                    signature_version = 'v4',
                    retries = {
                        'max_attempts': 10,
                        'mode': 'standard'
                    }
                )
    
    # print("objet url------------------",object_url)
    print(type(object_url))
    split_laz_url = object_url.split('/')
    print(split_laz_url)
    local_file_name = f"{FILE_PATH}"+split_laz_url[-1]
    # print(f"local file path: {local_file_name}")
    
    object_name = '/'.join(split_laz_url[-3:])
    print(f"object key: {object_name}")
    
    client = boto3.client('s3', config=my_config,aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)
    print(f"--------------------Laz file <{split_laz_url[-1]}> is Downloading------------------------------")
    start = time.time()
    client.download_file(BUCKET_NAME, object_name, local_file_name)
    end = time.time()
    print(f"laz file has downloaded in {end-start} seconds")
    breakLazFile(local_file_name,"small_laz")

# async def async_downloadS3File(object_url):
#     loop = asyncio.get_event_loop()
#     with ThreadPoolExecutor() as executor:
#         await loop.run_in_executor(executor, downloadS3File, object_url)

# async def main():
#     arr = ["adfsdf", "wfwerw", "werwerw"]
#     tasks = [async_downloadS3File(url) for url in arr]
#     await asyncio.gather(*tasks)

    
if __name__ == "__main__":

    Lidar_url = input("Please enter Lidar URL:  ")
    downloadS3File(Lidar_url)
    
    # asyncio.run(main())
