# MinIO Proxy

A simple Go proxy server between your frontend and MinIO. Handles single and batch GET, POST, PUT, DELETE—batch operations use goroutines for parallel execution.

## Configuration

Set these environment variables (or create a `.env` and source it):

| Variable           | Description                                                                                       | Default          |
| ------------------ | ------------------------------------------------------------------------------------------------- | ---------------- |
| `MINIO_ENDPOINT`   | MinIO server (e.g. `kvm.local:9000`)                                                              | `localhost:9000` |
| `MINIO_ACCESS_KEY` | MinIO access key                                                                                  | `minioadmin`     |
| `MINIO_SECRET_KEY` | MinIO secret key                                                                                  | `minioadmin`     |
| `MINIO_BUCKET`     | Bucket name                                                                                       | `mybucket`       |
| `MINIO_USE_SSL`    | Use HTTPS for MinIO                                                                               | `false`          |
| `LISTEN_ADDR`      | Proxy listen address                                                                              | `:8080`          |
| `API_KEY`          | If set, all requests (except `/health`) must include `X-API-Key` or `Authorization: Bearer <key>` | _(disabled)_     |

## Run

```bash
export MINIO_ENDPOINT=your-kvm-ip:9000
export MINIO_BUCKET=your-bucket
go run .
```

Or build and run:

```bash
go build -o kzen-go .
./kzen-go
```

Dev mode (live reload with [air](https://github.com/air-verse/air)):

```bash
air
```

## Docker / Dokploy

```bash
docker compose up -d
```

For [Dokploy](https://dokploy.com): create a Compose app, point to this repo, set Compose path to `./docker-compose.yml`. Add `.env` with your MinIO settings. Use the Domains tab to attach a domain.

**Note:** When running in Docker, set `MINIO_ENDPOINT` to the host-accessible address of your MinIO (e.g. `host.docker.internal:9004` on Docker Desktop, or the KVM host IP).

## API

### Authentication

When `API_KEY` is set, include it in every request (except `/health`):

```bash
curl -H "X-API-Key: your-secret-key" http://localhost:8080/objects/photos/avatar.jpg
# or
curl -H "Authorization: Bearer your-secret-key" http://localhost:8080/objects/photos/avatar.jpg
```

---

### GET `/objects/{path}`

Download an object from MinIO.

```bash
curl http://localhost:8080/objects/photos/avatar.jpg -o avatar.jpg
```

Frontend:

```js
const res = await fetch('/objects/photos/avatar.jpg')
const blob = await res.blob()
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
    headers: { 'Content-Type': file.type },
})
```

Frontend (multipart form):

```js
const form = new FormData()
form.append('file', file)
await fetch('/objects/photos/myfile.jpg', {
    method: 'POST',
    body: form,
})
```

### PUT `/objects/{path}`

Overwrite an object (same as POST).

### DELETE `/objects/{path}`

Delete an object from MinIO.

```bash
curl -X DELETE http://localhost:8080/objects/photos/old.jpg
```

---

### Batch (parallel via goroutines)

#### GET `/batch?keys=key1,key2,...`

Fetch multiple objects. Returns `multipart/mixed` with each object as a part.

```bash
curl "http://localhost:8080/batch?keys=img1.jpg,img2.jpg" -o response.bin
```

#### POST `/batch`

Upload multiple files. Form: `keys` (comma-separated) + `files` (or `file`) multipart.

```bash
curl -X POST -F "keys=path/a.jpg,path/b.jpg" -F "files=@a.jpg" -F "files=@b.jpg" \
  http://localhost:8080/batch
```

Frontend:

```js
const form = new FormData()
form.append('keys', 'img1.jpg,img2.jpg')
form.append('files', file1)
form.append('files', file2)
await fetch('/batch', { method: 'POST', body: form })
```

#### DELETE `/batch?keys=key1,key2,...`

Delete multiple objects in parallel.

```bash
curl -X DELETE "http://localhost:8080/batch?keys=old1.jpg,old2.jpg"
```

---

### GET `/health`

Health check endpoint.
