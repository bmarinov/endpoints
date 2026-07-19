endpoints
---

The module provides simple low-level primitives for inter-process communication and message handling. 


## Usage

```sh
go get github.com/bmarinov/endpoints
```

### ipc

Typed generic request/response between processes over a unix socket. Supports any `net.Conn` transport.

```go
// import "github.com/bmarinov/endpoints/ipc"

handler := ipc.HandlerFunc[string, string](func(msg string) (string, error) {
	return msg + " pong", nil
})

srv, err := ipc.NewServer("/tmp/app.sock", handler)
if err != nil {
	log.Fatal(err)
}
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
go srv.Run(ctx)

resp, err := ipc.Send[string, string](ctx, "ping", "/tmp/app.sock")
// resp == "ping pong"
```

`NewServerWithListener` and `SendConn` accept a listener and dialer for any
transport.  
Pass `WithReadTimeout`/`WithWriteTimeout` to configure the timeouts on the server. See the runnable [examples](ipc/example_test.go).

## Testing

```sh
go test -race ./...
```

Mutation testing is optional and kept out of `go.mod`. Install [gremlins](https://github.com/go-gremlins/gremlins) and point it at a package:

```sh
go install github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0
gremlins unleash --timeout-coefficient 5 ./ipc/
```
