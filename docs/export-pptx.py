#!/usr/bin/env python3
"""Export the XrdHttp architecture deck to PPTX for Google Slides import."""
from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.oxml.ns import qn
from pptx.util import Emu, Inches, Pt
from lxml import etree
from copy import deepcopy
import os

HERE = os.path.dirname(os.path.abspath(__file__))
XRD = os.path.join(HERE, "slides-assets", "xrootd-logo.png")
CERN = os.path.join(HERE, "slides-assets", "cern-logo.svg.png")
OUT = os.path.join(HERE, "XrdHttp2-architecture-slides.pptx")

BG = RGBColor(0x07, 0x07, 0x07)
INK = RGBColor(0xF7, 0xF4, 0xEF)
MUTED = RGBColor(0xC9, 0xBF, 0xB6)
GOLD = RGBColor(0xB8, 0x92, 0x3A)
PINK = RGBColor(0xE8, 0xA0, 0xBF)
PANEL = RGBColor(0x16, 0x14, 0x16)
LINE = RGBColor(0x3A, 0x30, 0x18)


def set_run(run, text, size=18, color=INK, bold=False, italic=False):
    run.text = text
    run.font.size = Pt(size)
    run.font.color.rgb = color
    run.font.bold = bold
    run.font.italic = italic
    run.font.name = "Calibri"


def add_tb(slide, l, t, w, h, text, size=18, color=INK, bold=False, align=PP_ALIGN.LEFT):
    box = slide.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(h))
    tf = box.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.alignment = align
    run = p.add_run()
    set_run(run, text, size, color, bold)
    return box


def add_para(tf, text, size=16, color=INK, bold=False, space_before=4, space_after=2, align=PP_ALIGN.LEFT):
    if tf.paragraphs[0].text == "" and not any(r.text for r in tf.paragraphs[0].runs):
        p = tf.paragraphs[0]
    else:
        p = tf.add_paragraph()
    p.alignment = align
    p.space_before = Pt(space_before)
    p.space_after = Pt(space_after)
    run = p.add_run()
    set_run(run, text, size, color, bold)
    return p


def fill_shape(shape, color):
    shape.fill.solid()
    shape.fill.fore_color.rgb = color
    shape.line.fill.background()


def outline_shape(shape, color, width_pt=1.25):
    shape.fill.solid()
    shape.fill.fore_color.rgb = PANEL
    shape.line.color.rgb = color
    shape.line.width = Pt(width_pt)


def bg(slide):
    fill = slide.background.fill
    fill.solid()
    fill.fore_color.rgb = BG


def logos(slide, prs):
    if os.path.isfile(XRD):
        slide.shapes.add_picture(XRD, Inches(0.35), Inches(0.18), Inches(0.55), Inches(0.55))
    if os.path.isfile(CERN):
        slide.shapes.add_picture(CERN, Inches(12.45), Inches(0.18), Inches(0.52), Inches(0.52))
    bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0), Inches(0.82), prs.slide_width, Pt(1.5))
    fill_shape(bar, GOLD)


def footer(slide, prs, n, total):
    bar = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0), Inches(7.28), prs.slide_width, Inches(0.22))
    fill_shape(bar, GOLD)
    add_tb(slide, 11.6, 7.18, 1.5, 0.28, f"{n} / {total}", size=11, color=GOLD, align=PP_ALIGN.RIGHT)


def kicker(slide, text):
    add_tb(slide, 0.55, 0.95, 12, 0.32, text.upper(), size=12, color=PINK, bold=True)


def title(slide, text, y=1.22, h=0.7, size=28):
    add_tb(slide, 0.55, y, 12.2, h, text, size=size, color=INK, bold=True)


def lead(slide, text, y=1.9, h=0.85, size=16):
    add_tb(slide, 0.55, y, 12.2, h, text, size=size, color=MUTED)


def card(slide, l, t, w, h, heading, body_lines, label=None, label_color=GOLD):
    sh = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(l), Inches(t), Inches(w), Inches(h))
    outline_shape(sh, GOLD, 1.0)
    y = t + 0.1
    if label:
        add_tb(slide, l + 0.12, y, w - 0.24, 0.22, label.upper(), size=10, color=label_color, bold=True)
        y += 0.22
    add_tb(slide, l + 0.12, y, w - 0.24, 0.32, heading, size=15, color=GOLD, bold=True)
    y += 0.34
    box = slide.shapes.add_textbox(Inches(l + 0.12), Inches(y), Inches(w - 0.24), Inches(h - (y - t) - 0.1))
    tf = box.text_frame
    tf.word_wrap = True
    tf.clear()
    first = True
    for line in body_lines:
        p = tf.paragraphs[0] if first else tf.add_paragraph()
        first = False
        p.level = 0
        p.space_after = Pt(4)
        run = p.add_run()
        prefix = "• " if not line.startswith("•") else ""
        set_run(run, prefix + line, 13, INK)


def bullets(slide, l, t, w, h, lines, size=16):
    box = slide.shapes.add_textbox(Inches(l), Inches(t), Inches(w), Inches(h))
    tf = box.text_frame
    tf.word_wrap = True
    tf.clear()
    first = True
    for line in lines:
        p = tf.paragraphs[0] if first else tf.add_paragraph()
        first = False
        p.space_after = Pt(6)
        run = p.add_run()
        set_run(run, "•  " + line, size, INK)


def table(slide, l, t, w, h, rows, col_w=None):
    n_rows, n_cols = len(rows), len(rows[0])
    shp = slide.shapes.add_table(n_rows, n_cols, Inches(l), Inches(t), Inches(w), Inches(h))
    tbl = shp.table
    if col_w:
        for i, cw in enumerate(col_w):
            tbl.columns[i].width = Inches(cw)
    for r, row in enumerate(rows):
        for c, val in enumerate(row):
            cell = tbl.cell(r, c)
            cell.text = ""
            p = cell.text_frame.paragraphs[0]
            p.alignment = PP_ALIGN.LEFT
            run = p.add_run()
            is_hdr = r == 0
            set_run(run, val, 12 if not is_hdr else 11, GOLD if is_hdr else INK, bold=is_hdr)
            cell.text_frame.word_wrap = True
            # fill
            tc = cell._tc
            tcPr = tc.get_or_add_tcPr()
            solid = etree.SubElement(tcPr, qn("a:solidFill"))
            srgb = etree.SubElement(solid, qn("a:srgbClr"))
            srgb.set("val", "161416" if r else "1E1810")
    return shp


def new_slide(prs, n, total):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    bg(slide)
    logos(slide, prs)
    footer(slide, prs, n, total)
    return slide


def main():
    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    T = 19

    # 1 title
    s = new_slide(prs, 1, T)
    add_tb(s, 0.9, 2.4, 11.5, 1.4, "XrdHttp Extensions for XRootD", size=36, color=INK, bold=True)
    rule = s.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.9), Inches(3.85), Inches(1.6), Pt(3))
    fill_shape(rule, GOLD)
    add_tb(s, 0.9, 4.1, 11, 0.4, "Dr. Andreas-Joachim Peters - CERN IT-SD-PSS", size=20, color=PINK)
    add_tb(s, 0.9, 4.55, 11, 0.35, "XROOTD WORKSHOP LYON 2026", size=14, color=GOLD, bold=True)
    repo = add_tb(s, 0.9, 5.05, 11, 0.35, "github.com/cern-eos/xrootd - branch XrdHttp2", size=16, color=GOLD)
    repo.text_frame.paragraphs[0].runs[0].hyperlink.address = "https://github.com/cern-eos/xrootd/tree/XrdHttp2"

    # 2 intro
    s = new_slide(prs, 2, T)
    kicker(s, "XRootD")
    title(s, "Extensions in XrdHTTP")
    lead(s, "XrdHTTP is the HTTP plugin of XRootD. This work adds a proper header parser, HTTP/2, Kerberos for HTTP, and a POSIX client that can run in the Linux kernel. The long-term target is kernel, RDMA, and GPU access to the same files, over a small set of storage operations.", y=1.95, h=1.05)
    card(s, 0.55, 3.15, 3.9, 3.4, "Kernel", ["Mount storage in Linux. Reads and writes go through the page cache. TLS stays in the kernel after handshake."], "Now", GOLD)
    card(s, 4.7, 3.15, 3.9, 3.4, "RDMA", ["Same filesystem operations, but over RDMA instead of HTTP, for high throughput."], "Next", PINK)
    card(s, 8.85, 3.15, 3.9, 3.4, "GPU", ["Copy into GPU memory (DMA-BUF). HTTP cannot do that. It needs the RDMA path."], "Next", PINK)

    # 3 contents
    s = new_slide(prs, 3, T)
    kicker(s, "Talk")
    title(s, "Contents")
    bullets(s, 0.7, 2.05, 11, 4.6, [
        "1   Goals: kernel, RDMA, GPU",
        "2   What we had to add in XrdHTTP, and why",
        "3   Limits of the current XRootD server",
        "4   Parallelism: streams, Bridge, redirects",
        "5   The new pieces (parser, HTTP/2, Kerberos, mount)",
        "6   Test suite - all new tests pass",
        "7   Microbench: native parser still wins",
        "8   Status and what is still open",
    ], size=20)

    # 4 three paths
    s = new_slide(prs, 4, T)
    kicker(s, "Introduction · goals")
    title(s, "Three access paths, one operation list")
    lead(s, "The filesystem client defines operations (lookup, read, write, …) once. HTTP is the first transport. RDMA is meant to be the second.", y=1.95, h=0.7)
    card(s, 0.55, 2.75, 3.9, 1.7, "Kernel (CPU)", ["page cache · mmap · kTLS"], "Now", GOLD)
    card(s, 4.7, 2.75, 3.9, 1.7, "RDMA (planned)", ["same ops, no HTTP"], "Next", PINK)
    card(s, 8.85, 2.75, 3.9, 1.7, "GPU (planned)", ["DMA-BUF · not via HTTP"], "Next", PINK)
    card(s, 2.2, 4.6, 8.9, 1.15, "xiofs operations", ["lookup · getattr · read · write · mkdir · …"])
    add_tb(s, 0.55, 5.9, 12.2, 0.7, "See src/XrdXioFS/include/xiofs_ops.h. Memory targets: page cache, user buffer, DMA-BUF, GPU. GPU ioctls return -EOPNOTSUPP until RDMA exists.", size=13, color=MUTED)

    # 5 why xrdhttp
    s = new_slide(prs, 5, T)
    kicker(s, "Introduction · why XrdHTTP")
    title(s, "The kernel needs HTTP verbs that XrdHTTP did not have")
    lead(s, "A kernel filesystem cannot speak the native xroot protocol. It can speak HTTP/1.1. So the server had to grow the missing methods, and a wire layer that HTTP/2 and HTTP/1.1 both use.", y=1.95, h=0.85)
    card(s, 0.55, 2.95, 6.0, 3.7, "What we needed", [
        "Range GET for reads (page-cache fill)",
        "PATCH with byte range for pwrite (not a full PUT)",
        "PROPFIND for stat and directory listing",
        "PROPPATCH / LINK for chmod and hard links",
        "Many requests on one TLS connection (FUSE)",
        "Kerberos for HTTP clients at KDC sites",
    ], "Needed", PINK)
    card(s, 6.8, 2.95, 6.0, 3.7, "What that is not", [
        "Not a new storage backend - still ofs",
        "Not a replacement for root://",
        "Not HTTP/2 inside the kernel (too much protocol)",
        "Not GPU I/O on the HTTP path - kTLS copies into CPU pages",
    ], "Not", PINK)

    # 6 llhttp
    s = new_slide(prs, 6, T)
    kicker(s, "Why llhttp")
    title(s, "Our parser was a line scanner, not an HTTP parser")
    lead(s, "On master, headers are read with BuffgetLine (scan for newline) and parseLine (split on the first colon). The code itself calls this “naive parsing”. That is enough for a friendly curl client. It is a poor base for HTTP/2, keepalive, and hostile input.", y=1.9, h=0.95)
    card(s, 0.55, 2.95, 6.0, 3.35, "Problems with the own parser", [
        "Only complete lines. A header split across two reads waits for newline.",
        "Rewrites the receive buffer in place (inserts NUL).",
        "Header values still carry CR-LF; Connection is compared to “Keep-Alive\\r\\n”.",
        "No real grammar: a bad method or a huge incomplete header is easy to mishandle.",
        "HTTP/2 headers are HPACK, not lines - this parser cannot be reused.",
    ])
    card(s, 6.8, 2.95, 6.0, 3.35, "What llhttp gives us", [
        "Generated state machine (Node.js, MIT, vendored v9.4.1).",
        "Incremental: feed whatever bytes are on the socket.",
        "Rejects malformed request lines; 16 KiB incomplete-header cap.",
        "Name/value callbacks fill the same XrdHttpReq as HTTP/2.",
        "http.parser legacy remains for A/B and old tests.",
    ])
    add_tb(s, 0.55, 6.4, 12.2, 0.55, "This is not a speed project. On a typical GET the native line scanner is actually a bit faster. We switched so HTTP/1 is correct, bounded, and shares one request object with HTTP/2.", size=13, color=MUTED)

    # 7 http2 why
    s = new_slide(prs, 7, T)
    kicker(s, "Why HTTP/2")
    title(s, "Many file operations on one connection")
    add_tb(s, 0.55, 2.05, 6.0, 0.3, "HTTP/1.1 - one request, then the next", size=14, color=INK, bold=True)
    add_tb(s, 7.1, 2.05, 5.7, 0.3, "HTTP/2 - several streams at once", size=14, color=INK, bold=True)
    b1 = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(0.55), Inches(2.45), Inches(3.2), Inches(0.38))
    fill_shape(b1, PINK)
    add_tb(s, 0.65, 2.48, 3.0, 0.32, "GET /a", size=12, color=RGBColor(0x11, 0x11, 0x11), bold=True)
    add_tb(s, 3.9, 2.48, 2.6, 0.32, "next request waits", size=12, color=MUTED)
    b2 = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(7.1), Inches(2.45), Inches(4.2), Inches(0.32))
    fill_shape(b2, PINK)
    add_tb(s, 7.2, 2.46, 4.0, 0.3, "GET /a", size=12, color=RGBColor(0x11, 0x11, 0x11), bold=True)
    b3 = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(7.1), Inches(2.85), Inches(3.2), Inches(0.32))
    fill_shape(b3, GOLD)
    add_tb(s, 7.2, 2.86, 3.0, 0.3, "GET /b", size=12, color=RGBColor(0x11, 0x11, 0x11), bold=True)
    b4 = s.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, Inches(7.1), Inches(3.25), Inches(5.0), Inches(0.32))
    fill_shape(b4, RGBColor(0xE8, 0xC5, 0x47))
    add_tb(s, 7.2, 3.26, 4.8, 0.3, "PATCH /c  (write)", size=12, color=RGBColor(0x11, 0x11, 0x11), bold=True)
    card(s, 0.55, 3.85, 3.9, 2.7, "LAN / FUSE", ["A mount does parallel reads and writes. HTTP/2 keeps them on one TLS session. That is the main reason."])
    card(s, 4.7, 3.85, 3.9, 2.7, "WAN", ["Fewer TCP/TLS handshakes for many small files. Useful, not the driver."])
    card(s, 8.85, 3.85, 3.9, 2.7, "Kernel", ["The kernel module uses HTTP/1.1. HTTP/2 is too large (HPACK, streams). The kernel reuses the same methods (GET, PATCH, …)."])

    # 8 limits
    s = new_slide(prs, 8, T)
    kicker(s, "Limits")
    title(s, "XrdHTTP is still one plugin on one XRootD link")
    table(s, 0.55, 2.05, 12.2, 4.6, [
        ["XRootD fact", "What it means here"],
        ["One Bridge->Run() at a time per connection", "HTTP/2 can queue many streams. File I/O still runs one after another."],
        ["Process() returns 0 (continue), 1 (wait), or close", "HTTP/2 must follow that. It is not a thread per stream."],
        ["HTTPS already cannot use sendfile", "HTTP/2 cannot either. Zero-copy stays HTTP/1.1 without TLS."],
        ["Login is SecEntity", "Kerberos, tokens, and GSI all become a normal XRootD identity."],
        ["kTLS is CPU memory", "No GPU DMA on the HTTP path. GPU I/O waits for RDMA."],
    ], col_w=[5.4, 6.8])

    # 9 today vs parallel
    s = new_slide(prs, 9, T)
    kicker(s, "Limits · parallelism")
    title(s, "One socket can carry many streams. One Bridge still does not.")
    lead(s, "Three GETs on one HTTP/2 session. Left: today. Right: a normal HTTP/2 origin, and what we would need on the same node.", y=1.85, h=0.5)
    add_tb(s, 0.4, 2.15, 6.2, 0.26, "TODAY - SERIAL BRIDGE, THEN FAN-OUT", size=12, color=PINK, bold=True)
    card(s, 0.85, 2.42, 5.3, 0.88, "HTTP/2 client", ["streams 1, 3, 5"])
    card(s, 0.85, 3.34, 5.3, 0.88, "1 TLS socket", ["GETs already on the wire"])
    card(s, 0.85, 4.26, 5.3, 0.88, "nghttp2", ["ready_queue"])
    card(s, 0.55, 5.18, 5.9, 0.88, "one Bridge->Run()", ["/a then /b then /c - re-entry rejected"])
    card(s, 0.4, 6.1, 1.95, 0.85, "data srv A", ["new socket /a"])
    card(s, 2.45, 6.1, 1.95, 0.85, "data srv B", ["new socket /b"])
    card(s, 4.5, 6.1, 1.95, 0.85, "data srv C", ["new socket /c"])

    add_tb(s, 6.85, 2.15, 6.2, 0.26, "TARGET - PARALLEL BRIDGE ON THE SAME SOCKET", size=12, color=GOLD, bold=True)
    card(s, 7.3, 2.42, 5.3, 0.88, "HTTP/2 client", ["streams 1, 3, 5"])
    card(s, 7.3, 3.34, 5.3, 0.88, "1 TLS socket", ["same as today"])
    card(s, 7.3, 4.26, 5.3, 0.88, "nghttp2", ["three live streams"])
    card(s, 6.85, 5.18, 6.05, 1.05, "Bridge - three Run() in flight", ["Run a s1    Run b s3    Run c s5"])
    card(s, 7.3, 6.28, 5.3, 0.72, "ofs - three files at once", ["DATA interleaved on the original socket"])

    # 10 bridge changes
    s = new_slide(prs, 10, T)
    kicker(s, "Limits · Bridge")
    title(s, "What has to change for parallel Runs")
    lead(s, "Native root:// already carries a stream id in the 24-byte header. The HTTP Bridge copies that field, then refuses a second Run() on the same link. Login stays once per connection.", y=1.85, h=0.7)
    table(s, 0.4, 2.55, 12.5, 3.85, [
        ["Today, per HTTP connection", "Needed"],
        ["One CurrentReq / XrdHttpReq", "One request object per HTTP/2 stream (map from nghttp2 id)"],
        ["Transit::Run() fails if runStatus != 0", "N in-flight Runs, keyed by xroot stream id, each with its own header buffer"],
        ["One Result callback (XrdHttpReq)", "Done / Data / Error / Redir demuxed to that stream's writer"],
        ["dispatchNext() waits for appInFlight", "Start the next stream up to a cap (e.g. 8 or 16 in-flight)"],
        ["Process() 0 / 1 / close is the whole link", "A wait on stream 3 must not stop streams 1 and 5"],
        ["File table and SecEntity are per link", "Keep that. Concurrent opens of different files; one identity"],
    ], col_w=[5.5, 7.0])
    add_tb(s, 0.5, 6.45, 12.3, 0.55, "Not a thread per stream. Parallelism is several outstanding Bridge operations, the way a native client uses several stream ids. Redirect fan-out already gives cross-node parallelism without this work.", size=12, color=MUTED)

    # 11 architecture
    s = new_slide(prs, 11, T)
    kicker(s, "Architecture")
    title(s, "HTTP/1.1 and HTTP/2 share one request handler")
    lead(s, "HTTP/1 request handling is the same as on master: after headers are parsed, the request still goes through XrdHttpReq and the Bridge. GET, PUT, HEAD, DELETE, WebDAV, TPC, checksums, and request bodies are that same code. Only header parsing is new (llhttp). HTTP/2 uses the same handler after nghttp2 assembles the headers.", y=1.9, h=1.15)
    card(s, 0.55, 3.2, 6.0, 1.7, "HTTP/1.1", ["XrdHttp1Session (llhttp)", "XrdHttp1ResponseWriter"])
    card(s, 6.8, 3.2, 6.0, 1.7, "HTTP/2", ["XrdHttp2Session (nghttp2)", "XrdHttp2ResponseWriter"])
    card(s, 2.2, 5.1, 8.9, 1.5, "processParsedRequest() → XrdHttpReq → Bridge → ofs", ["Kerberos · tokens · GSI · TPC · checksums"])

    # 12 vs master
    s = new_slide(prs, 12, T)
    kicker(s, "Versus master")
    title(s, "HTTP/1 is the same request path as master")
    lead(s, "An HTTP/1.1 client still talks to the same XrdHttpReq and Bridge as on master. We did not rewrite GET/PUT/HEAD/DELETE or how bodies are read. What changed for HTTP/1 is only how request headers are parsed (llhttp). Set http.parser legacy to use master’s line parser.", y=1.9, h=0.95)
    table(s, 0.45, 2.95, 12.4, 3.9, [
        ["", "master", "this branch"],
        ["HTTP/1 GET/PUT/HEAD/DELETE, bodies, TPC, checksums", "XrdHttpReq + Bridge", "Same code"],
        ["HTTP/1 header parser", "line scanner in XrdHttpReq", "llhttp (or legacy)"],
        ["HTTP/2", "none", "nghttp2, then the same XrdHttpReq"],
        ["Kerberos on HTTP", "none (only root://)", "http.auth krb5 SPNEGO"],
        ["Partial write", "PUT replaces the whole file", "PATCH with Content-Range"],
        ["POSIX over HTTP", "none", "XIOFS: CLI, FUSE, kernel module"],
    ], col_w=[5.2, 3.5, 3.7])

    # 13 http2 pieces
    s = new_slide(prs, 13, T)
    kicker(s, "HTTP/2")
    title(s, "What was added")
    card(s, 0.55, 2.1, 6.0, 3.8, "How the client selects HTTP/2", [
        "HTTPS: TLS ALPN token h2",
        "Plain TCP: HTTP/2 connection preface, or HTTP/1.1 Upgrade: h2c",
        "http.h2 off advertises only HTTP/1.1",
    ])
    card(s, 6.8, 2.1, 6.0, 3.8, "Behaviour", [
        "PUT sends body as DATA arrives",
        "Up to 100 streams queued; Bridge still one at a time",
        "Flow control both ways; RST_STREAM drops a stream",
        "http.h2push /path after a GET, if the client allows push",
    ])
    add_tb(s, 0.55, 6.15, 12.2, 0.5, "Needs libnghttp2 (BUILD_HTTP2). Tests: XRootD::httph2.", size=14, color=MUTED)

    # 14 kerberos
    s = new_slide(prs, 14, T)
    kicker(s, "Kerberos")
    title(s, "HTTP clients can use the site KDC")
    lead(s, "New on this branch. Native xroot Kerberos is unchanged. HTTP uses SPNEGO (WWW-Authenticate: Negotiate) and then logs into the Bridge as krb5.", y=1.9, h=0.75)
    card(s, 0.55, 2.8, 6.0, 3.7, "Server", [
        "http.auth krb5 /etc/krb5.keytab HTTP/<host>@REALM",
        "Service class is HTTP, not host",
        "HTTPS required",
        "Usually http.tlsclientauth off",
    ])
    card(s, 6.8, 2.8, 6.0, 3.7, "Client", [
        "kinit, then: curl --negotiate -u : --cacert ca.pem https://host/file",
        "401, then the token on the same connection",
        "Independent of HTTP/1.1 vs HTTP/2",
    ])

    # 15 xiofs
    s = new_slide(prs, 15, T)
    kicker(s, "XIOFS")
    title(s, "POSIX client: CLI, FUSE, kernel")
    card(s, 0.55, 1.95, 3.9, 2.15, "xiofscli / xiofsd", ["HTTP/2. FUSE multiplexes I/O on one connection. Used to develop and test the methods."], "Userspace", GOLD)
    card(s, 4.7, 1.95, 3.9, 2.15, "xiofs.ko", ["Linux page cache. HTTP/1.1 on a kTLS socket. Handshake is still userspace (xiofsagent)."], "Kernel", GOLD)
    card(s, 8.85, 1.95, 3.9, 2.15, "RDMA / GPU", ["XIOFS_IOC_GPU_READ/WRITE exist. They return “not supported” until an RDMA transport is written."], "Stub", PINK)
    table(s, 0.55, 4.3, 12.2, 2.55, [
        ["POSIX", "HTTP method"],
        ["stat / readdir", "PROPFIND"],
        ["read", "GET with Range"],
        ["pwrite / writeback", "PATCH with Content-Range and If-Match"],
        ["create / mkdir / unlink / rename", "PUT / MKCOL / DELETE / MOVE"],
        ["chmod / hard link", "PROPPATCH / LINK"],
    ], col_w=[5.5, 6.7])

    # 16 kernel
    s = new_slide(prs, 16, T)
    kicker(s, "Kernel mount")
    title(s, "Handshake in userspace, data path in the kernel")
    add_tb(s, 0.55, 1.95, 12, 0.28, "Once per mount", size=12, color=PINK, bold=True)
    card(s, 0.45, 2.25, 3.9, 1.45, "xiofsagent", ["OpenSSL handshake"])
    card(s, 4.7, 2.25, 3.9, 1.45, "kTLS on the socket", ["TLS 1.2 AES-GCM"])
    card(s, 8.85, 2.25, 3.9, 1.45, "ioctl import", ["/dev/xiofsctl"])
    add_tb(s, 0.55, 3.85, 12, 0.28, "Every read / write / mmap", size=12, color=GOLD, bold=True)
    card(s, 0.35, 4.15, 2.4, 1.55, "app", ["POSIX"])
    card(s, 2.85, 4.15, 2.4, 1.55, "page cache", ["readahead / writeback"])
    card(s, 5.35, 4.15, 2.4, 1.55, "xiofs.ko", ["HTTP/1.1"])
    card(s, 7.85, 4.15, 2.4, 1.55, "kTLS + TCP", ["no extra copy"])
    card(s, 10.35, 4.15, 2.45, 1.55, "XrdHTTP", ["server"])
    add_tb(s, 0.55, 5.85, 12.2, 0.7, "AlmaLinux 9 and 10. One socket, so no HTTP/2 multiplex in-kernel. If the socket drops, run xiofsagent --import-only again.", size=14, color=MUTED)

    # 17 tests
    s = new_slide(prs, 17, T)
    kicker(s, "Tests")
    title(s, "New CTest coverage - all of these pass")
    lead(s, "The existing XRootD::http suite still runs. We added dedicated tests for the parser, HTTP/2, Kerberos, and the XIOFS client. They pass in the current tree (Linux, HTTP/2 and Kerberos enabled).", y=1.9, h=0.75)
    table(s, 0.45, 2.7, 12.4, 3.7, [
        ["CTest", "What it covers"],
        ["XRootD::http", "HTTP/1.1 regression, plus PATCH, If-Match, and open-handle reuse"],
        ["XRootD::httpparser", "llhttp headers, keep-alive, header size cap, h2c preface / Upgrade"],
        ["XRootD::httpparserlegacy", "Same checks on http.parser legacy"],
        ["XRootD::httph2", "HTTPS ALPN h2: GET/PUT/HEAD/DELETE, ranges, PATCH, multiplex, push, xiofscli"],
        ["XRootD::httpkrb5", "SPNEGO upload/download/HEAD; unauthenticated requests rejected"],
        ["xrdxiofs-unit-tests", "URL parsing and PROPFIND/WebDAV response parsing"],
    ], col_w=[3.6, 8.8])
    add_tb(s, 0.55, 6.5, 12.2, 0.4, "ctest -R 'XRootD::(http|httpparser|httph2|httpkrb5)'   ·   httph2 needs TLS; httpkrb5 needs Kerberos + TLS (Linux).", size=12, color=MUTED)

    # 18 microbench
    s = new_slide(prs, 18, T)
    title(s, "The native parser still wins")
    lead(s, "Apple Silicon, RelWithDebInfo xrootd, parse compiled -O2. Median of three runs. Parse: 200k iterations of a typical GET. Request loop: one keepalive connection, sequential, local xrootd, cleartext (HTTP/2 = h2c). Same XrdHttpReq after headers.", y=1.9, h=0.7)
    table(s, 0.45, 2.65, 6.15, 3.35, [
        ["Header parse", "ns/req", "Mreq/s"],
        ["Native typical GET (291 B, 8 hdrs)", "754", "1.33"],
        ["llhttp typical GET", "822", "1.22"],
        ["llhttp, 16 B chunks", "1048", "0.95"],
        ["Native 24 extra headers (1269 B)", "1448", "0.69"],
        ["llhttp 24 extra headers", "1519", "0.66"],
    ], col_w=[3.55, 1.25, 1.35])
    table(s, 6.8, 2.65, 6.05, 2.35, [
        ["Request handling", "HTTP/1.1", "HTTP/2"],
        ["HEAD", "44 µs", "76 µs"],
        ["GET 64 B", "62 µs", "100 µs"],
        ["GET 8 MiB", "1.7 ms", "2.3 ms"],
    ], col_w=[2.55, 1.75, 1.75])
    add_tb(s, 0.55, 6.05, 12.2, 0.85, "Native = master’s BuffgetLine / split-on-colon scanner. It still wins the parse microbench; we did not switch for speed. HTTP/2 on one stream is about 1.4× after nghttp2 NO_COPY + batched writev (no memcpy of the body into frames). Remaining gap: HPACK, 16 KiB DATA frames, serial Bridge. True sendfile like cleartext HTTP/1.1 is not possible - each DATA frame needs a 9-byte header.", size=12, color=MUTED)

    # 19 outlook
    s = new_slide(prs, 19, T)
    kicker(s, "Outlook")
    title(s, "Status")
    card(s, 0.55, 2.05, 3.9, 3.7, "Done", [
        "llhttp + HTTP/2 on the server",
        "Kerberos for HTTPS",
        "PATCH, PROPPATCH, LINK",
        "FUSE client (HTTP/2)",
        "Kernel module + kTLS (CPU / page cache)",
    ], "Done", GOLD)
    card(s, 4.7, 2.05, 3.9, 3.7, "Not done", [
        "RDMA transport",
        "GPU-direct (ioctl stub only)",
        "HTTP/2 in the kernel",
        "Automatic TLS reconnect",
        "Parallel Bridge on one HTTP/2 connection",
        "Symlinks, chown, utimens",
    ], "Not done", PINK)
    card(s, 8.85, 2.05, 3.9, 3.7, "Next steps", [
        "Land parser + HTTP/2 + Kerberos on master",
        "Keep XIOFS as the follow-up",
        "RDMA when XrdHTTP can serve it",
        "Then GPU I/O on DMA-BUF",
    ], "Next", PINK)
    add_tb(s, 0.55, 5.95, 12.2, 0.7, "Today: CPU applications can mount XrdHTTP and use the Linux page cache. RDMA and GPU are designed in the client API, not implemented on the wire.", size=15, color=MUTED)

    prs.save(OUT)
    print("Wrote", OUT)


if __name__ == "__main__":
    main()
