# MinIO Proxy

A simple Go proxy server between your frontend and MinIO. Handles GET (download) and POST (upload) requests.

## Configuration

Set these environment variables (or create a `.env` and source it):

| Variable | Description | Default |
|----------|-------------|---------|
| `MINIO_ENDPOINT` | MinIO server (e.g. `kvm.local:9000`) | `localhost:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO secret key | `minioadmin` |
| `MINIO_BUCKET` | Bucket name | `mybucket` |
| `MINIO_USE_SSL` | Use HTTPS for MinIO | `false` |
| `LISTEN_ADDR` | Proxy listen address | `:8080` |

## Run

```bash
export MINIO_ENDPOINT=your-kvm-ip:9000
export MINIO_BUCKET=your-bucket
go run .
```

Or build and run:
```bash
go build -o minio-proxy .
./minio-proxy
```

Dev mode (live reload with [air](https://github.com/air-verse/air)):
```bash
air
```

## API

### GET `/objects/{path}`

Download an object from MinIO.

```bash
curl http://localhost:8080/objects/photos/avatar.jpg -o avatar.jpg
```

Frontend:
```js
const res = await fetch('/objects/photos/avatar.jpg');
const blob = await res.blob();
```

### POST `/objects/{path}`

Upload an object to MinIO. Send the file as raw body with `Content-Type` header.

```bash
curl -X POST -T myfile.jpg http://localhost:8080/objects/photos/myfile.jpg
```

Frontend (raw body):
```js
await fetch('/objects/photos/myfile.jpg', {
  method: 'POST',
  body: file,
  headers: { 'Content-Type': file.type }
});
```

Frontend (multipart form):
```js
const form = new FormData();
form.append('file', file);
await fetch('/objects/photos/myfile.jpg', {
  method: 'POST',
  body: form
});
```

### GET `/health`

Health check endpoint.
