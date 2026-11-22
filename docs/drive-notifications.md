# Drive notifications

DBlayer includes an HTTP server that sends Google Drive change signals to Android devices with an open event stream.

## Run the server

Build from the source checkout with Go 1.24 or later.

```sh
go -C cmd/drive-notifications build -o /tmp/dblayer-drive-notifications .
/tmp/dblayer-drive-notifications
```

The process listens on `127.0.0.1:8080`. Run it under your process supervisor. Put a public HTTPS reverse proxy on the same host and forward both routes to that listener.

| Route | Caller |
| --- | --- |
| `GET /events` | Android client with a server-sent events connection |
| `POST /drive/notifications` | Google Drive webhook |

Disable response buffering and compression for `/events`. Set the proxy idle timeout above 20 seconds. Forward the `Authorization`, `X-Goog-Channel-Token`, and `X-Goog-Resource-State` headers without logging them. Route both paths to one process so a webhook can reach the active connection.

## Delivery

The Android client generates a random subscription token and opens the event stream. After the server sends `ready`, the client registers a Drive notification channel with that token. A change sends `changed` to that connection. The client reads its Drive cursor with its own Google credentials.

The process holds active subscriptions in memory. Restarting it closes their streams. Clients reconnect and read Drive to recover changes made during the disconnect.
