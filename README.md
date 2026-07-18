endpoints
---

The module provides simple low-level primitives for inter-process communication and message handling. 


## Usage

```sh
go get github.com/bmarinov/endpoints
```

## Testing

```sh
go test -race ./...
```

Mutation testing is optional and kept out of `go.mod`. Install [gremlins](https://github.com/go-gremlins/gremlins) and point it at a package:

```sh
go install github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0
gremlins unleash --timeout-coefficient 5 ./ipc/
```
