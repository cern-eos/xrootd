# Parsock third-party copy

Pull-only `COPY` over one HTTP/2 session on libparsocket. Load it next to the
curl TPC plugin. Claim runs before `MatchesPath`, so a normal `https` `COPY`
stays with `libXrdHttpTPC`.

```
http.exthandler xrdps libXrdHttpParsockTpc.so
http.exthandler xrdtpc libXrdHttpTPC.so
```

This handler takes the request when `Source` is `spt://` or `parsock://`, or
the request carries `X-Xrd-Transport: parsock`. `X-Xrd-Parsock:
port=;transport=;protection=;sockets=` on that `COPY` selects the data-server
listener and skips discovering it from a redirect. `TransferHeader*` and
`Copy-Header` are copied onto the source `HEAD` and `GET`. A redirect to
another host drops `Authorization`.

The response is the usual early `201`, `Perf Marker` chunks (bytes committed
to the local file), then `success: Created` or `failure: ...`.

A `<path>.xrdtpc` marker stores the source URL, ETag (or `Repr-Digest`), and
size. After a failure the next `COPY` of the same source resumes from the
local file size. `Overwrite: T`, a missing marker, a different ETag, or a
local file larger than the source truncates and starts at byte 0. The marker
is removed after a successful copy.
