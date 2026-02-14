# Required Header:
    - X-API-Key
# GET
    - curl http://localhost:9004/objects/path/to/file.jpg -o file.jpg

## get contents of storage in json format:
    - http://localhost:9004/debug/list?prefix=kzen/



# Testing the project
    - go test ./minioserver/... -run TestDebugList -v 