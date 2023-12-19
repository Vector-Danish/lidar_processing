import mysql.connector
from decouple import config
from multiprocessing import Process, Manager
import scripty
import json

def process_lidar_file(lidar_file_tuple, shared_list):
    lidar_file = lidar_file_tuple[0]
    print(f"Processing lidar file: {lidar_file}")
    scripty.downloadS3File(lidar_file)
    shared_list.append(lidar_file)

def main():
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

    lidar_data_id = 1
  
    cursor = connection.cursor()
    update_query = "UPDATE lidar_data SET status = 'inprogress' WHERE id = %s"
    cursor.execute(update_query, (lidar_data_id,))
    connection.commit()

    # Fetch lidar_data from lidar_chunks based on id
    cursor.execute("SELECT lidar_chunks.lidar_data FROM lidar_chunks WHERE lidar_chunks.lidar_id = %s", (lidar_data_id,))
    lidar_chunks_data = cursor.fetchall()
    # print("lidar chunk data",lidar_chunks_data)

    # Close the cursor and connection
    cursor.close()
    connection.close()

    # Step 1: Extract lidar data strings and deserialize into list
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
