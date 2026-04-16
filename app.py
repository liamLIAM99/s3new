from flask import Flask, render_template_string, request, redirect, flash, send_file
import boto3
from botocore.exceptions import ClientError
import io
import os
from dotenv import load_dotenv
app = Flask(__name__)

load_dotenv()
app.secret_key = "change-this-secret-key"

#awit
 
# ============================================
# CHANGE THESE VALUES TO YOUR AWS SETTINGS
# ============================================
# ============================================
 
# Create S3 client - connects to your AWS account
# GlusterFS concept: this is like opening a connection to your storage cluster
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    
)
 
# Helper: convert bytes to readable format (e.g. 1024 -> "1.0 KB")
def format_size(bytes):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes < 1024:
            return f"{bytes:.1f} {unit}"
        bytes /= 1024
    return f"{bytes:.1f} TB"
 
# Helper: list files in a bucket
# GlusterFS concept: listing files on a storage brick
def list_files(bucket):
    try:
        response = s3.list_objects_v2(Bucket=bucket)
        if 'Contents' in response:
            return [{'name': obj['Key'], 'size': format_size(obj['Size'])}
                    for obj in response['Contents']]
        return []
    except ClientError:
        return None
 
# ---- HTML Template (the web page) ----
HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>GlusterFS Demo (AWS S3 Implementation)</title>
    <style>
        body { font-family: Arial; max-width: 900px; margin: 50px auto; padding: 20px; }
        h1 { color: #333; }
        .upload-box { background: #f0f0f0; padding: 20px; margin: 20px 0; border-radius: 5px; }
        .nodes { display: flex; gap: 20px; margin: 20px 0; }
        .node { flex: 1; background: #e3f2fd; padding: 15px; border-radius: 5px; }
        .node h3 { margin-top: 0; }
        .file-list { background: white; padding: 10px; border-radius: 3px; }
        .file-item { padding: 8px; border-bottom: 1px solid #ddd; display: flex; justify-content: space-between; }
        .replicated { color: green; font-weight: bold; }
        .partial    { color: orange; }
        button { background: #2196F3; color: white; padding: 10px 20px; border: none;
                 border-radius: 3px; cursor: pointer; }
        button:hover  { background: #1976D2; }
        .delete-btn   { background: #f44336; padding: 5px 10px; font-size: 12px; }
        .message      { padding: 10px; margin: 10px 0; border-radius: 3px; }
        .success      { background: #d4edda; color: #155724; }
        .error        { background: #f8d7da; color: #721c24; }
    </style>
</head>
<body>
    <h1>&#128452; GlusterFS Demo - Distributed Storage - AWS S3</h1>
    <p>Demonstrating GlusterFS Concept that replicates files across multiple storage nodes using AWS S3</p>
 
    {% with messages = get_flashed_messages(with_categories=true) %}
    {% if messages %}
        {% for category, message in messages %}
            <div class="message {{ category }}">{{ message }}</div>
        {% endfor %}
    {% endif %}
    {% endwith %}
 
    <div class="upload-box">
        <h2>&#128228; Upload File</h2>
        <form method="POST" action="/upload" enctype="multipart/form-data">
            <input type="file" name="file" required>
            <button type="submit">Upload &amp; Replicate</button>
        </form>
        <p><small>File will be copied to both storage nodes automatically</small></p>
    </div>
 
    <h2>&#128202; Storage Nodes</h2>
    <div class="nodes">
        <div class="node">
            <h3>Node 1 - S3 Bucket (Primary)</h3>
            <p><strong>{{ primary_bucket }}</strong></p>
            <div class="file-list">
                {% if primary_files %}
                    {% for file in primary_files %}
                        <div class="file-item">
                            <span>{{ file.name }}</span>
                            <span>{{ file.size }}</span>
                        </div>
                    {% endfor %}
                {% else %}<p>No files</p>{% endif %}
            </div>
        </div>
        <div class="node">
            <h3>Node 2 - S3 Bucket (Secondary)</h3>
            <p><strong>{{ secondary_bucket }}</strong></p>
            <div class="file-list">
                {% if secondary_files %}
                    {% for file in secondary_files %}
                        <div class="file-item">
                            <span>{{ file.name }}</span>
                            <span>{{ file.size }}</span>
                        </div>
                    {% endfor %}
                {% else %}<p>No files</p>{% endif %}
            </div>
        </div>
    </div>
 
    <h2>&#128193; All Files</h2>
    {% if merged %}
        {% for file in merged %}
            <div class="file-item">
                <span>
                    {{ file.name }}
                    {% if file.replicated %}
                        <span class="replicated">&#10003; Replicated</span>
                    {% else %}
                        <span class="partial">&#9888; Partial</span>
                    {% endif %}
                </span>
                <span>
                    {{ file.size }}
                    <a href="/download/{{ file.name }}">Download</a>
                    <form method="POST" action="/delete/{{ file.name }}" style="display:inline;">
                        <button class="delete-btn" onclick="return confirm('Delete?')">Delete</button>
                    </form>
                </span>
            </div>
        {% endfor %}
    {% else %}
        <p>No files uploaded yet</p>
    {% endif %}
</body>
</html>
"""
 
# ---- Route: Home Page ----
# GlusterFS concept: shows the unified view of all storage nodes
@app.route('/')
def index():
    primary_files   = list_files(os.getenv("PRIMARY_BUCKET"))   or []
    secondary_files = list_files(os.getenv("SECONDARY_BUCKET")) or []
 
    # Merge file lists and check replication status
    # GlusterFS concept: checking which files are replicated across bricks
    all_files = {}
    for f in primary_files:
        all_files[f['name']] = {'name': f['name'], 'size': f['size'],
                                'in_primary': True, 'in_secondary': False}
    for f in secondary_files:
        if f['name'] in all_files:
            all_files[f['name']]['in_secondary'] = True
        else:
            all_files[f['name']] = {'name': f['name'], 'size': f['size'],
                                    'in_primary': False, 'in_secondary': True}
 
    merged = []
    for data in all_files.values():
        data['replicated'] = data['in_primary'] and data['in_secondary']
        merged.append(data)
 
    return render_template_string(HTML,
                                  primary_bucket=os.getenv("PRIMARY_BUCKET"),
                                  secondary_bucket=os.getenv("SECONDARY_BUCKET"),
                                  primary_files=primary_files,
                                  secondary_files=secondary_files,
                                  merged=merged)
 
# ---- Route: Upload ----
# GlusterFS concept: replication - writing to multiple bricks simultaneously
@app.route('/upload', methods=['POST'])
def upload():
    file = request.files.get('file')
    if not file or file.filename == '':
        flash('No file selected', 'error')
        return redirect('/')
 
    # FIX: Read file content ONCE into memory to avoid "closed file" error
    # This lets us upload to both buckets from the same data
    file_content = file.read()
    filename     = file.filename
 
    try:
        # Upload to primary bucket (Brick 1)
        s3.put_object(Bucket=os.getenv("PRIMARY_BUCKET"),   Key=filename, Body=file_content)
        # Replicate to secondary bucket (Brick 2)
        s3.put_object(Bucket=os.getenv("SECONDARY_BUCKET"), Key=filename, Body=file_content)
        flash(f'\u2713 {filename} replicated to both nodes!', 'success')
    except ClientError as e:
        flash(f'Upload failed: {str(e)}', 'error')
 
    return redirect('/')
 
# ---- Route: Download ----
# GlusterFS concept: high availability - fallback to secondary if primary fails
@app.route('/download/<filename>')
def download(filename):
    # Try primary first
    try:
        file_obj = io.BytesIO()
        s3.download_fileobj(os.getenv("PRIMARY_BUCKET"), filename, file_obj)
        file_obj.seek(0)
        return send_file(file_obj, as_attachment=True, download_name=filename)
    except ClientError:
        pass  # Primary failed, try secondary
 
    # Fallback to secondary (high availability)
    try:
        file_obj = io.BytesIO()
        s3.download_fileobj(os.getenv("SECONDARY_BUCKET"), filename, file_obj)
        file_obj.seek(0)
        flash('Retrieved from backup node', 'success')
        return send_file(file_obj, as_attachment=True, download_name=filename)
    except ClientError:
        flash(f'File not found: {filename}', 'error')
        return redirect('/')
 
# ---- Route: Delete ----
# GlusterFS concept: deletions are also replicated across all bricks
@app.route('/delete/<filename>', methods=['POST'])
def delete(filename):
    try:
        s3.delete_object(Bucket=os.getenv("PRIMARY_BUCKET"),   Key=filename)
        s3.delete_object(Bucket=os.getenv("SECONDARY_BUCKET"), Key=filename)
        flash(f'\u2713 Deleted {filename} from both nodes', 'success')
    except ClientError as e:
        flash(f'Delete failed: {str(e)}', 'error')
    return redirect('/')
 
# ---- Start the server ----
if __name__ == '__main__':
    print("=" * 50)
    print("GlusterFS Demo - Starting Server")
    print("=" * 50)
    print("Open your browser to: http://localhost:5000")
    app.run(debug=True, port=5000)
