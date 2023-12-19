import mysql.connector
from decouple import config
from multiprocessing import Process, Manager
import scripty
import json

deserialized_bucket = None
lidar_salt = None

def process_lidar_file(lidar_file_tuple, shared_list):
    lidar_file = lidar_file_tuple[0]
    print(f"Processing lidar file: {lidar_file}")
    scripty.downloadS3File(lidar_file, deserialized_bucket, lidar_salt)
    shared_list.append(lidar_file)

def main():
    global deserialized_bucket
    global lidar_salt

    host = config('DB_HOST')
    port = config('DB_PORT')
    user = config('DB_USERNAME')
    password = config('DB_PASSWORD')
    database = config('DB_DATABASE')

    # Establish a connection to the RDS instance
    connection = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )

    # Read lidar_data_id from data.json
    with open('data.json', 'r') as json_file:
        data = json.load(json_file)
        lidar_data_id = data.get('tour_id')
    
    cursor = connection.cursor()
    update_query = "UPDATE lidar_data SET status = 'inprogress' WHERE uuid = %s"
    cursor.execute(update_query, (lidar_data_id,))
    connection.commit()

    cursor.execute("SELECT lidar_data.bucket, lidar_data.salt, lidar_data.id FROM lidar_data WHERE lidar_data.uuid = %s", (lidar_data_id,))
    bucket_data = cursor.fetchall()
    print("bucket data and lidar_id---------------------------------", bucket_data[0])
    lidar_id = bucket_data[0][-1]
    lidar_salt = bucket_data[0][-2]
    # print("lidar id --------------------------", lidar_id)

    # bucket data and convert it into list 
    bucket_data_strings = [row[0] for row in bucket_data]
    deserialized_bucket = [json.loads(data) for data in bucket_data_strings]
    # print(deserialized_bucket)
   
    # print("bucket name in data----------------------------",lidar_data_strings)

    update_query = "UPDATE lidar_chunks SET status = 'inprogress' WHERE lidar_chunks.lidar_id = %s"
    cursor.execute(update_query, (lidar_id,))
    connection.commit()

    # Fetch lidar_data from lidar_chunks based on id
    cursor.execute("SELECT lidar_chunks.lidar_data FROM lidar_chunks WHERE lidar_chunks.lidar_id = %s", (lidar_id,))
    lidar_chunks_data = cursor.fetchall()
    # print("lidar chunk data",lidar_chunks_data)   


    cursor.close()
    connection.close()

    # Extract lidar data strings and deserialize into list
    lidar_data_strings = [row[0] for row in lidar_chunks_data]
    deserialized_data = [json.loads(data) for data in lidar_data_strings]
    

    manager = Manager()
    shared_list = manager.list()

    processes = []
    for lidar_file in deserialized_data:
        # print("lidar file and type in table-------------",lidar_file, type(lidar_file))
        p = Process(target=process_lidar_file, args=(lidar_file, shared_list))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    print("All processes completed")
    print("List of lidar files processed:", shared_list)

if __name__ == "__main__":
    main()
