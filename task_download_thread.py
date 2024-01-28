import threading
from s3_demo import download_file

class TaskDownloadThread(threading.Thread):
     def __init__(self, bucket, object_path, save_to_path):
         super(TaskDownloadThread, self).__init__()
         self.bucket = bucket
         self.object_path = object_path
         self.save_to_path = save_to_path
 
     def run(self):
          download_file(self.object_path, self.bucket, self.save_to_path)