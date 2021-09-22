import glob
from azure.storage.blob import BlobClient
#from azure.storage.blob import BlobServiceClient, PublicAccess
from threading import Thread
import os


# Uploads a single blob. May be invoked in thread.
def upload_blob_f(container, file, index=0, result=None):
    if result is None:
        result = [None]

    try:
        print(file.rsplit('\\', 2)[-2])
        blob_name = "testraw2/"+f.rsplit('\\', 2)[-2]+"/"+''.join(os.path.splitext(os.path.basename(file)))
        print(blob_name)
        #
        blob = BlobClient.from_connection_string(
            conn_str='DefaultEndpointsProtocol=https;AccountName=apdgecommerceadls;AccountKey=WT2xSWQHrna8YTuLy9QwjFnM7H2vMP8XiURNynDTis93osXlgIfF5COSh/fS8aovx/Pbr3xAqDYQO7JSkQsRmw==;EndpointSuffix=core.windows.net',
            container_name=container,
            blob_name=blob_name
        )

        with open(file, "rb") as data:
            blob.upload_blob(data, overwrite=True)

        print(f'Upload succeeded: {blob_name}')
        result[index] = True # example of returning result
    except Exception as e:
        print(e) # do something useful here
        result[index] = False # example of returning result


# container: string of container name. This example assumes the container exists.
# files: list of file paths.    
def upload_wrapper(container, files):
    # here, you can define a better threading/batching strategy than what is written
    # this code just creates a new thread for each file to be uploaded
    parallel_runs = len(files)
    threads = [None] * parallel_runs
    results = [None] * parallel_runs
    for i in range(parallel_runs):
        t = Thread(target=upload_blob_f, args=(container, files[i], i, results))
        threads[i] = t
        threads[i].start()

    for i in range(parallel_runs):  # wait for all threads to finish
        threads[i].join()

    # do something with results here

#magnus path


path = r"C:\Users\tan.j.53\OneDrive - Procter and Gamble\Documents\RAW"


files=[]
for f in glob.glob(path+"\**\*.xlsx",recursive=True):
    files.append(f)
    print(files)
    print(f)
    print(f.rsplit('\\', 2)[-2])
    print("hi")
upload_wrapper("ebr-dev",files)