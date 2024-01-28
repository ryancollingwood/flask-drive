import os
from datetime import datetime
import threading
from flask import Flask, render_template, request, redirect, send_file, Response
import zipstream
from dotenv import load_dotenv


from s3_demo import list_files, download_file, upload_file, list_files_with_prefix
from task_download_thread import TaskDownloadThread


load_dotenv()

app = Flask(__name__)

BUCKET = os.getenv("BUCKET")
DOWNLOAD_FOLDER = os.getenv("DOWNLOAD_FOLDER", "downloads")
UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", "uploads")
SECRET = os.getenv("SECRET", None)

def get_bucket(request):
    request_bucket = request.args.get('bucket')
    if request_bucket is not None:
        return request_bucket
    
    return BUCKET

def check_secret(request):
    if not SECRET:
        return True
    request_secret = request.args.get('secret')
    return request_secret == SECRET

@app.route('/')
def entry_point():
    return 'Hello World!'


@app.route("/storage")
def storage():
    contents = list_files(BUCKET)
    return render_template('storage.html', contents=contents)


@app.route("/upload", methods=['POST'])
def upload():
    if request.method == "POST":
        if not check_secret(request):
            return None
        f = request.files['file']
        f.save(f.filename)
        upload_file(f"{f.filename}", get_bucket(request))

        return redirect("/storage")


@app.route("/download/<filename>", methods=['GET'])
def download(filename):
    if request.method == 'GET':
        output = download_file(filename, get_bucket(request), DOWNLOAD_FOLDER)

        return send_file(output, as_attachment=True)


@app.route('/<path:text>', methods=['GET', 'POST'])
def all_routes(text: str):
    path = text.split("/")
    action = path[0]
    path = path[1:]
    bucket = get_bucket(request)

    if action == "download":
        if request.method == 'GET':
            if path[-1] == "*":
                files = list_files_with_prefix("/".join(path[:-1]), bucket)
                return zip_download_files(bucket, files)
            else:
                output = download_file("/".join(path), bucket, DOWNLOAD_FOLDER)
                return send_file(output, as_attachment=True)
    
    # no match here
    return None

def zip_download_files(bucket, files):
    def generator():
        z = zipstream.ZipFile(mode='w', compression=zipstream.ZIP_DEFLATED)
        for f in files:
            z.write(f'{DOWNLOAD_FOLDER}/{f}')

        for chunk in z:
            yield chunk

    batch_size = 20
    for i in range(0, len(files), batch_size):
        batch_files = files[i:i+batch_size]
        tasks = list()
        for r in batch_files:
            tasks.append(TaskDownloadThread(bucket, r, DOWNLOAD_FOLDER))
        
        for t in tasks:
            t.start()
        
        for t in tasks:
            t.join()

    response = Response(generator(), mimetype='application/zip')
    outfile_ts = datetime.now().isoformat(timespec="seconds").replace(" ", "_").replace(":", "-")
    response.headers['Content-Disposition'] = f'attachment; filename=files_{outfile_ts}.zip'

    for f in files:
        try:
            os.remove(f"{DOWNLOAD_FOLDER}/{f}")
        except:
            pass

    return response

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")
