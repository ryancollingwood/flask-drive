import os
from datetime import datetime
from pathlib import Path
import shutil
import threading
from flask import Flask, render_template, request, redirect, send_file, Response, jsonify
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


@app.route("/clean", methods=['GET'])
def clean():
    if request.method == 'GET':
        downloaded_files = [x for x in Path(DOWNLOAD_FOLDER).glob("*")]
        removed_files = list()
        for d_file in downloaded_files:
            if d_file.is_file():
                try:
                    if d_file.stem == ".gitignore":
                        continue
                    os.remove(d_file)
                    removed_files.append(d_file)
                except:
                    pass
            else:
                try:
                    shutil.rmtree(d_file)
                    removed_files.append(d_file)
                except:
                    pass

        return jsonify([str(x) for x in removed_files])


@app.route('/<path:text>', methods=['GET', 'POST'])
def all_routes(text: str):
    path = text.split("/")
    action = path[0]
    path = path[1:]
    bucket = get_bucket(request)

    if action == "download":
        if request.method == 'GET':
            if "*" in text:
                fetch_path = "/".join(path)
                fetch_path = f"{fetch_path}/"              
                files = list_files_with_prefix(fetch_path, bucket)
                return zip_download_files(bucket, files)
    
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

    return response

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0")
