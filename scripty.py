import laspy
import time
import os
from botocore.config import Config
import boto3
from decouple import config

laz_file_path = config("FILE_PATH")
FILE_PATH = "big_laz/"

def breakLazFile(local_file_path, save_folder_name,bucket_data,lidar_salt):
    my_config = Config(
                    region_name = bucket_data[0]["region"],
                    signature_version = 'v4',
                    retries = {
                        'max_attempts': 10,
                        'mode': 'standard'
                    }
                )
    client = boto3.client('s3', config=my_config,aws_access_key_id=bucket_data[0]["access_key_id"],aws_secret_access_key=bucket_data[0]["secret_key"])
    print(bucket_data[0]["access_key_id"])
     
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

            filename = f"{local_file_path.split('/')[-1].split('.')[0]}_{INCREMENT}.laz"
            print(filename)

            laz_name = filename
            data.write(os.path.join(save_folder_name, laz_name))
            chunk_file_path = f"{laz_file_path}{filename}"
            
            print("chunk_file_path")
            print(chunk_file_path)
            print("bucket_dat")
            print(bucket_data[0]["name"])
            print("comple path")
            print(f"testLidar/{lidar_salt}")

            upload_url = client.upload_file(chunk_file_path, bucket_data[0]["name"], f"testLidar/{lidar_salt}/{filename}")
            print("upload return of s3-----------------",upload_url)

            INCREMENT += 1

        end = time.time()

    print(f"Total time taken to create chunk files: {end - start} seconds with {INCREMENT - 1} children")

    for file_name in os.listdir(save_folder_name):
        file_path = os.path.join(save_folder_name, file_name)
        os.remove(file_path)
    os.remove(local_file_path)

def downloadS3File(object_url,bucket_data, lidar_salt):

    
    my_config = Config(
                    region_name = bucket_data[0]["region"],
                    signature_version = 'v4',
                    retries = {
                        'max_attempts': 10,
                        'mode': 'standard'
                    }
                )
    
    # print("objet url------------------",object_url)
    # print(type(object_url))
    split_laz_url = object_url.split('/')
    print(split_laz_url)
    local_file_name = f"{FILE_PATH}"+split_laz_url[-1]
    # print(f"local file path: {local_file_name}")
    
    object_name = '/'.join(split_laz_url[-3:])
    print(f"object key: {object_name}")
    
    client = boto3.client('s3', config=my_config,aws_access_key_id=bucket_data[0]["access_key_id"],aws_secret_access_key=bucket_data[0]["secret_key"])
    print(f"--------------------Laz file <{split_laz_url[-1]}> is Downloading------------------------------")
    start = time.time()
    client.download_file(bucket_data[0]["name"], object_name, local_file_name)
    end = time.time()
    print(f"laz file has downloaded in {end-start} seconds")
    breakLazFile(local_file_name,"small_laz",bucket_data,lidar_salt)

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
