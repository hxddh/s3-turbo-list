#!/usr/bin/env python3
"""Local ListObjectsV2 mock for end-to-end benchmarks (docs/development.md).

Usage: mock-s3.py PORT LATENCY_MS BUCKET:KIND:N...
  KIND flat -> obj-%09d keys; skew -> 90% under big/, the rest in 20 dirs;
  tree -> 20 dirs of N/20 keys. A bucket named *-mod drops every 777th key
  and changes every 1000th ETag (a diff target). NOENC=1 ignores
  encoding-type=url. Never point s3tl at a real endpoint for benchmarks.
"""
import hashlib
import http.server
import os
import socketserver
import sys
import time
import urllib.parse
from bisect import bisect_right
from xml.sax.saxutils import escape

PORT = int(sys.argv[1])
LATENCY = float(sys.argv[2]) / 1000.0
BUCKETS = {}
NOENC = os.environ.get("NOENC") == "1"


def gen(kind, n):
    if kind == "flat":
        return [f"obj-{i:09d}" for i in range(n)]
    if kind == "skew":
        big = int(n * 0.9)
        keys = [f"big/part-{i:09d}" for i in range(big)]
        rest = n - big
        keys += [f"d{j:02d}/f-{i:07d}" for j in range(20) for i in range(rest // 20)]
        return sorted(keys)
    if kind == "tree":
        return sorted(f"d{j:02d}/f-{i:07d}" for j in range(20) for i in range(n // 20))
    raise SystemExit(f"unknown kind {kind}")


for spec in sys.argv[3:]:
    name, kind, n = spec.split(":")
    keys = gen(kind, int(n))
    etags = {}
    if name.endswith("-mod"):
        keys = [k for i, k in enumerate(keys) if i % 777 != 5]
        for i, k in enumerate(keys):
            if i % 1000 == 3:
                etags[k] = "f" * 32
    BUCKETS[name] = (keys, etags)


def etag_of(bucket, key):
    return BUCKETS[bucket][1].get(key) or hashlib.md5(key.encode()).hexdigest()


def enc(value, url):
    return urllib.parse.quote_plus(value, safe="/") if url else value


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def reply(self, status, body=b"", ctype="application/xml", headers=()):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_HEAD(self):
        bucket = urllib.parse.urlparse(self.path).path.strip("/").split("/")[0]
        self.reply(200 if bucket in BUCKETS else 404, headers=[("x-amz-bucket-region", "us-east-1")])

    def do_GET(self):
        time.sleep(LATENCY)
        parsed = urllib.parse.urlparse(self.path)
        bucket = parsed.path.strip("/").split("/")[0]
        q = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
        if bucket not in BUCKETS:
            return self.reply(404, b"<Error><Code>NoSuchBucket</Code><Message>no</Message></Error>")
        keys = BUCKETS[bucket][0]
        prefix = q.get("prefix", "")
        delim = q.get("delimiter", "")
        max_keys = int(q.get("max-keys", "1000"))
        url = q.get("encoding-type") == "url" and not NOENC
        token = q.get("continuation-token")
        after = token if token else q.get("start-after", "")
        i = bisect_right(keys, after) if after else 0
        contents, prefixes, last = [], [], None
        truncated = False
        while i < len(keys):
            k = keys[i]
            if not k.startswith(prefix):
                if k > prefix:
                    break
                i += 1
                continue
            if len(contents) + len(prefixes) >= max_keys:
                truncated = True
                break
            if delim:
                rest = k[len(prefix):]
                pos = rest.find(delim)
                if pos >= 0:
                    cp = prefix + rest[: pos + len(delim)]
                    prefixes.append(cp)
                    last = cp
                    # skip all keys under cp
                    i = bisect_right(keys, cp + "\U0010ffff")
                    continue
            contents.append(k)
            last = k
            i += 1
        out = ['<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">']
        out.append(f"<Name>{bucket}</Name><Prefix>{escape(enc(prefix, url))}</Prefix><KeyCount>{len(contents)+len(prefixes)}</KeyCount><MaxKeys>{max_keys}</MaxKeys>")
        if url:
            out.append("<EncodingType>url</EncodingType>")
        out.append(f"<IsTruncated>{'true' if truncated else 'false'}</IsTruncated>")
        if truncated:
            out.append(f"<NextContinuationToken>{escape(last)}</NextContinuationToken>")
        for k in contents:
            out.append(f"<Contents><Key>{escape(enc(k, url))}</Key><LastModified>2026-05-17T00:00:00.123Z</LastModified><ETag>&quot;{etag_of(bucket, k)}&quot;</ETag><Size>{len(k)}</Size><StorageClass>STANDARD</StorageClass></Contents>")
        for cp in prefixes:
            out.append(f"<CommonPrefixes><Prefix>{escape(enc(cp, url))}</Prefix></CommonPrefixes>")
        out.append("</ListBucketResult>")
        self.reply(200, "".join(out).encode())


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    request_queue_size = 1024


print(f"mock s3 on :{PORT}, buckets: " + ", ".join(f"{b}={len(v[0])}" for b, v in BUCKETS.items()), flush=True)
Server(("127.0.0.1", PORT), Handler).serve_forever()
