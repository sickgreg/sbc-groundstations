/*
 * aalink_idr_vrx_broadcast.c
 *
 * C implementation of the Python IDR requester for low-latency video links.
 * Target: Radxa Z3W (ARM64) — optimized for performance and low allocations.
 *
 * Features implemented:
 *  - UDP forwarder: listens on UDP_PORT and forwards to OUT_PORT (forward_ip)
 *  - Optional RTP stripping: forward only payload as raw H.26x over UDP
 *  - Detects lost RTP sequence numbers (16-bit) and requests IDR from camera
 *  - IDR bursts with retries according to policy; optional ACK handling
 *  - Stream resume detection and stream-down monitoring
 *  - Efficient pending-gap tracking using a 65536-entry timestamp array
 *  - Fixed-size token/ack entry ring for dedupe and waiting
 *  - NEW: optional external IDR passthrough listener on the same camera IDR
 *    UDP port; external players can poke this port and we request IDR using
 *    the same token/ACK machinery as for loss/resume.
 *
 * Build:
 *   gcc -O3 -march=native -pthread -o aalink_idr_vrx aalink_idr_vrx.c
 *
 * Run on Radxa Z3W as root or a user permitted to bind ports.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <signal.h>
#include <getopt.h>
#include <poll.h>

// ----- config -----
#define UDP_PORT_DEFAULT 5600
#define OUT_PORT_DEFAULT 5700

#define FORWARD_IP_DEFAULT "auto"

#define HTTP_FWD_PORT_DEFAULT 8088
#define SSH_FWD_PORT_DEFAULT  2222
#define IQ_FWD_PORT_DEFAULT   9876
#define BUFFER_SIZE 65536

#define CAMERA_IP_DEFAULT "192.168.0.1"
#define IDR_UDP_PORT_DEFAULT 11223
#define IDR_CLIENT_PORT_DEFAULT 11224

#define MICRO_LOSS_TIMEOUT_DEFAULT 0.02
#define STREAM_DOWN_TIMEOUT_DEFAULT 3.0

// Burst policy entries
struct burst_entry { int copies; double spacing; double wait_time; };
static const struct burst_entry IDR_BURST_POLICY[] = {
    {3, 0.03, 0.15},
    {3, 0.06, 0.25},
    {4, 0.12, 0.40},
};
#define IDR_BURST_POLICY_LEN (sizeof(IDR_BURST_POLICY)/sizeof(IDR_BURST_POLICY[0]))

#define IDR_CODE_LEN 3
#define MAX_TOKEN_HISTORY 200
#define USE_ACKS_DEFAULT 1 // set to 0 to disable ACK logic entirely

// ----- runtime-configurable state -----
static int udp_port = UDP_PORT_DEFAULT;
static int out_port = OUT_PORT_DEFAULT;
static int idr_use_broadcast = 1;          // 1 = send IDR UDP via directed broadcast by default
static char forward_ip[64] = FORWARD_IP_DEFAULT; // configurable forward target (e.g. Windows PC)
static int auto_forward_dest = 0;               // 1 = learn destination from client packets on idr_udp_port
static pthread_mutex_t fwd_dest_lock = PTHREAD_MUTEX_INITIALIZER;
static struct sockaddr_in fwd_dest_addr;        // valid when fwd_dest_gen != 0
static volatile unsigned fwd_dest_gen = 0;      // incremented on update (0 = unset)
static volatile double fwd_dest_last_seen = 0.0;
static const double fwd_dest_timeout_s = 10.0;  // stop forwarding after this without keepalive
// --- ownership lock (claim stream only when idle) ---
static int have_owner = 0;
static struct in_addr owner_ip;
static int owner_port = 0;              // viewer's desired dest port (from REG), else out_port
static double owner_last_seen = 0.0;    // last REG/trigger from owner
static volatile double last_video_rx = 0.0; // updated on incoming video packets
static const double stream_idle_s = 1.5;    // consider stream idle after this without packets
static const double owner_ttl_s   = 8.0;    // allow takeover if owner disappears
static char camera_ip[64]  = CAMERA_IP_DEFAULT;
static int idr_udp_port = IDR_UDP_PORT_DEFAULT;
static int idr_client_port = IDR_CLIENT_PORT_DEFAULT;
static double micro_loss_timeout = MICRO_LOSS_TIMEOUT_DEFAULT;
static double stream_down_timeout = STREAM_DOWN_TIMEOUT_DEFAULT;
static int use_acks = USE_ACKS_DEFAULT;
static int verbose = 0;
static int strip_rtp = 0;                      // 0 = keep RTP, 1 = strip 12-byte RTP header before forwarding
static int idr_passthru_enabled = 1;           // 1 = listen for external IDR triggers and forward to camera
static int enable_local_idr = 0;              // 1 = generate IDRs locally on loss/resume (default OFF)
static int http_ssh_fwd_enabled = 1;          // 1 = enable TCP forwarders (default ON)
static int http_fwd_port = 8088;              // listen port for HTTP forwarder (default 8088)
static int ssh_fwd_port  = 2222;              // listen port for SSH forwarder  (default 2222)
static int iq_fwd_port   = IQ_FWD_PORT_DEFAULT; // listen port for IQ forwarder   (default 9876)

// ----- runtime state -----
static volatile int running = 1;
static int sock_in = -1, sock_out = -1, sock_ctrl = -1, sock_passthru = -1;
static int sock_http_listen = -1;
static int sock_ssh_listen  = -1;
static int sock_iq_listen   = -1;

// pending gaps tracked by timestamp; 0.0 means not present
static double *pending_gap_ts = NULL; // size 65536

static pthread_mutex_t state_lock = PTHREAD_MUTEX_INITIALIZER;
static uint16_t expected_seq = 0;
static int expected_seq_known = 0;
static volatile int stream_active = 0;
static double last_packet_time = 0.0;

// ack entries ring buffer
struct ack_entry {
    char token[IDR_CODE_LEN+1];
    int used; // 0 = free, 1 = waiting, 2 = acked
    pthread_mutex_t mutex;
    pthread_cond_t cond;
};
static struct ack_entry ack_entries[MAX_TOKEN_HISTORY];

// current request (so new requests can cancel old bursts)
struct current_req {
    char token[IDR_CODE_LEN+1];
    volatile int stop;
};
static struct current_req *current_request = NULL;

// logging helpers
#define INFO(...) do { if (verbose) fprintf(stderr, __VA_ARGS__); } while(0)
#define WARN(...) do { fprintf(stderr, __VA_ARGS__); } while(0)
static double now_seconds();
// -------------------- Sticky forward destinations + persistence --------------------
//
// /etc/fwdIP.conf format (simple key=value, user-editable):
//   maxClients=2
//   viewer1=192.168.x.x
//   viewer1Port=5600
//
// Behavior:
//  - Forward always to viewer1 (even without REGs).
//  - While streaming, non-owner REGs do NOT hijack; they create a pending request and
//    we notify viewer1 via UDP:11223 with: REQ:<ip>:<port>\n
//  - viewer1 may respond (to forwarder UDP:11223) with:
//      CMD:IGNORE\n
//      CMD:SWITCH:<ip>:<port>\n
//      CMD:DUP:<ip>:<port>\n
//  - Duplicated clients are forwarded to up to maxClients (including viewer1).
//  - When stream is idle, if viewer1 isn't sending REGs, a continuously-REGing client
//    may take over without prompt; it becomes viewer1 and is persisted.
//

#define FWD_CONF_PATH "/etc/fwdIP.conf"
#define MAX_DUP_CLIENTS 8

static pthread_mutex_t client_lock = PTHREAD_MUTEX_INITIALIZER;

static int max_clients = 2;               // total including viewer1 (default 2)
static int viewer1_valid = 0;
static struct in_addr viewer1_ip;
static int viewer1_port_cfg = 0;          // 0 => use out_port
static double viewer1_last_reg = 0.0;

struct dup_client {
    int used;
    struct in_addr ip;
    int port;            // 0 => use out_port
    double last_reg;     // last keepalive from that client
};
static struct dup_client dup_clients[MAX_DUP_CLIENTS];

static int pending_active = 0;
static struct in_addr pending_ip;
static int pending_port = 0;
static double pending_since = 0.0;
static double pending_first = 0.0;
static double pending_last = 0.0;
static double pending_notify_last = 0.0;
static int pending_count = 0;

// Auto-duplication policy (Option 5): if viewer1 is silent while streaming,
// allow a continuously REG'ing requester to receive a duplicated stream.
// Never auto-switch while streaming.
static const double viewer1_silent_s = 8.0;   // no viewer1 REG for this long => treat as silent
static const double pending_window_s = 5.0;    // must send >=2 REGs within this window

static int clamp_max_clients(int v)
{
    if (v < 1) v = 1;
    if (v > (MAX_DUP_CLIENTS + 1)) v = (MAX_DUP_CLIENTS + 1);
    return v;
}

static int dup_count_locked(void)
{
    int c = 0;
    for (int i=0;i<MAX_DUP_CLIENTS;i++) if (dup_clients[i].used) c++;
    return c;
}

static int same_in_addr(struct in_addr a, struct in_addr b)
{
    return a.s_addr == b.s_addr;
}

static void dup_clear_locked(void)
{
    for (int i=0;i<MAX_DUP_CLIENTS;i++) dup_clients[i].used = 0;
}

static void conf_save_locked(void);

static void set_viewer1_locked(struct in_addr ip, int port)
{
    viewer1_ip = ip;
    viewer1_port_cfg = (port > 0) ? port : 0;
    viewer1_valid = 1;
    viewer1_last_reg = now_seconds();
    pending_active = 0;
    pending_count = 0;
    pending_first = pending_last = pending_notify_last = 0.0;
    dup_clear_locked();
    conf_save_locked();
}

static int dup_add_or_refresh_locked(struct in_addr ip, int port)
{
    // Don't add viewer1 as dup.
    if (viewer1_valid && same_in_addr(ip, viewer1_ip))
        return 1;

    // Refresh existing.
    for (int i=0;i<MAX_DUP_CLIENTS;i++)
    {
        if (dup_clients[i].used && same_in_addr(dup_clients[i].ip, ip))
        {
            dup_clients[i].last_reg = now_seconds();
            if (port > 0) dup_clients[i].port = port;
            return 1;
        }
    }

    // Enforce maxClients total.
    int total_allowed = clamp_max_clients(max_clients);
    int cur_total = 1 + dup_count_locked(); // viewer1 + dups
    if (cur_total >= total_allowed)
        return 0;

    for (int i=0;i<MAX_DUP_CLIENTS;i++)
    {
        if (!dup_clients[i].used)
        {
            dup_clients[i].used = 1;
            dup_clients[i].ip = ip;
            dup_clients[i].port = (port > 0) ? port : 0;
            dup_clients[i].last_reg = now_seconds();
            return 1;
        }
    }
    return 0;
}

static void send_req_to_viewer1(struct in_addr want_ip, int want_port)
{
    if (!viewer1_valid || sock_passthru < 0) return;

    char ipbuf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &want_ip, ipbuf, sizeof(ipbuf));
    char msg[128];
    snprintf(msg, sizeof(msg), "REQ:%s:%d\n", ipbuf, want_port);

    struct sockaddr_in dst;
    memset(&dst, 0, sizeof(dst));
    dst.sin_family = AF_INET;
    dst.sin_addr = viewer1_ip;
    dst.sin_port = htons(idr_udp_port);

    sendto(sock_passthru, msg, strlen(msg), 0, (struct sockaddr*)&dst, sizeof(dst));
}

static void conf_load(void)
{
    FILE* f = fopen(FWD_CONF_PATH, "r");
    if (!f) return;

    char line[256];
    while (fgets(line, sizeof(line), f))
    {
        char* s = line;
        while (*s && (*s==' '||*s=='\t'||*s=='\r'||*s=='\n')) s++;
        if (!*s || *s=='#') continue;

        char* eq = strchr(s, '=');
        if (!eq) continue;
        *eq++ = '\0';

        // trim key
        char* k = s;
        char* kend = k + strlen(k);
        while (kend > k && (kend[-1]==' '||kend[-1]=='\t')) kend--;
        *kend = '\0';

        // trim val
        char* v = eq;
        while (*v && (*v==' '||*v=='\t')) v++;
        char* vend = v + strlen(v);
        while (vend > v && (vend[-1]==' '||vend[-1]=='\t'||vend[-1]=='\r'||vend[-1]=='\n')) vend--;
        *vend = '\0';

        if (!*k || !*v) continue;

        if (strcasecmp(k, "maxClients")==0)
        {
            int mv = atoi(v);
            pthread_mutex_lock(&client_lock);
            max_clients = clamp_max_clients(mv);
            pthread_mutex_unlock(&client_lock);
        }
        else if (strcasecmp(k, "viewer1")==0)
        {
            struct in_addr ip;
            if (inet_aton(v, &ip))
            {
                pthread_mutex_lock(&client_lock);
                viewer1_ip = ip;
                viewer1_valid = 1;
                pthread_mutex_unlock(&client_lock);
            }
        }
        else if (strcasecmp(k, "viewer1Port")==0)
        {
            int pv = atoi(v);
            pthread_mutex_lock(&client_lock);
            viewer1_port_cfg = (pv > 0) ? pv : 0;
            pthread_mutex_unlock(&client_lock);
        }
    }

    fclose(f);
}

static void conf_save_locked(void)
{
    // Best effort (write to temp then rename).
    char tmpPath[128];
    snprintf(tmpPath, sizeof(tmpPath), "%s.tmp", FWD_CONF_PATH);

    static int warned = 0;
    FILE* f = fopen(tmpPath, "w");
    if (!f) {
        if (!warned) {
            warned = 1;
            INFO("[FWD] WARN: cannot write %s (%s)\n", FWD_CONF_PATH, strerror(errno));
        }
        return;
    }

    fprintf(f, "maxClients=%d\n", clamp_max_clients(max_clients));
    if (viewer1_valid)
    {
        char ipbuf[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &viewer1_ip, ipbuf, sizeof(ipbuf));
        fprintf(f, "viewer1=%s\n", ipbuf);
        if (viewer1_port_cfg > 0) fprintf(f, "viewer1Port=%d\n", viewer1_port_cfg);
    }
    fclose(f);
    rename(tmpPath, FWD_CONF_PATH);
}

static void prune_dups_if_idle_locked(double now_s)
{
    // Only prune when not actively streaming.
    if ((now_s - last_video_rx) < stream_idle_s)
        return;

    const double ttl = owner_ttl_s;
    for (int i=0;i<MAX_DUP_CLIENTS;i++)
    {
        if (dup_clients[i].used && (now_s - dup_clients[i].last_reg) > ttl)
            dup_clients[i].used = 0;
    }
}

static void handle_reg_from(const struct sockaddr_in *src, int port_override)
{
    if (!src) return;
    const double now_s = now_seconds();
    const int want_port = (port_override > 0) ? port_override : out_port;

    pthread_mutex_lock(&client_lock);

    if (!viewer1_valid)
    {
        set_viewer1_locked(src->sin_addr, want_port);
        pthread_mutex_unlock(&client_lock);
        INFO("[FWD] viewer1 set (no prior): %s:%d\n", inet_ntoa(src->sin_addr), want_port);
        return;
    }

    prune_dups_if_idle_locked(now_s);

    if (same_in_addr(src->sin_addr, viewer1_ip))
    {
        viewer1_last_reg = now_s;
        if (port_override > 0) viewer1_port_cfg = want_port;
        pthread_mutex_unlock(&client_lock);
        return;
    }

    // Refresh dup slot if already duplicated.
    // NOTE: we do NOT early-return here when viewer1 is silent; that would prevent reclaiming viewer1.
    int is_dup = 0;
    for (int i=0;i<MAX_DUP_CLIENTS;i++) {
        if (dup_clients[i].used && same_in_addr(dup_clients[i].ip, src->sin_addr)) {
            is_dup = 1;
            dup_clients[i].last_reg = now_s;
            if (port_override > 0) dup_clients[i].port = want_port;
            break;
        }
    }

    const int streaming = (now_s - last_video_rx) < stream_idle_s;

    if (streaming)
    {
        const int viewer1_is_silent_now = ((now_s - viewer1_last_reg) > viewer1_silent_s);
        if (is_dup && !viewer1_is_silent_now) {
            // Already receiving as a secondary while viewer1 is alive; nothing else to do.
            pthread_mutex_unlock(&client_lock);
            return;
        }

        // Track continuous REGs from a non-owner requester.
        int same_pending = (pending_active &&
                            same_in_addr(pending_ip, src->sin_addr) &&
                            pending_port == want_port);

        if (!same_pending || (now_s - pending_last) > pending_window_s)
        {
            pending_first = now_s;
            pending_count = 1;
        }
        else
        {
            pending_count++;
        }

        pending_active = 1;
        pending_ip = src->sin_addr;
        pending_port = want_port;
        pending_since = same_pending ? pending_since : now_s;
        pending_last = now_s;

        // Option 5: if viewer1 is silent while streaming, auto-DUP (never auto-switch)
        // to a continuously REG'ing requester, up to maxClients.
        const int viewer1_silent = ((now_s - viewer1_last_reg) > viewer1_silent_s);
        const int mc = clamp_max_clients(max_clients);
        const int can_dup = (mc > 1) && (dup_count_locked() < (mc - 1));
        const int continuous = (pending_count >= 2) && ((now_s - pending_first) <= pending_window_s);

        if (viewer1_silent && continuous)
        {
            // Auto handover when viewer1 appears dead/silent during an active stream:
            // requester becomes viewer1; old viewer1 is demoted to secondary (if maxClients allows).
            struct in_addr old_ip = viewer1_ip;
            int old_port = viewer1_port_cfg;

            // Promote requester to viewer1.
            viewer1_ip = src->sin_addr;
            viewer1_port_cfg = (want_port > 0) ? want_port : 0;
            viewer1_valid = 1;
            viewer1_last_reg = now_s;

            // Clear pending state.
            pending_active = 0;
            pending_count = 0;
            pending_first = pending_last = pending_notify_last = 0.0;

            // Make old viewer1 the only secondary (best effort).
            dup_clear_locked();
            const int mc2 = clamp_max_clients(max_clients);
            if (mc2 > 1)
            {
                // Don't add if it's the same host.
                if (old_ip.s_addr != viewer1_ip.s_addr)
                    dup_add_or_refresh_locked(old_ip, old_port);
            }

            conf_save_locked();

            pthread_mutex_unlock(&client_lock);

            INFO("[FWD] auto-handover viewer1=%s:%d (viewer1 silent; streaming)\n",
                 inet_ntoa(src->sin_addr), want_port);
            return;
        }

        // Rate-limit notifications/log spam for the same pending requester.
        int do_notify = 0;
        if ((now_s - pending_notify_last) > 1.0)
        {
            pending_notify_last = now_s;
            do_notify = 1;
        }

        pthread_mutex_unlock(&client_lock);

        if (do_notify)
        {
            send_req_to_viewer1(src->sin_addr, want_port);
            INFO("[FWD] pending request from %s:%d (streaming; notified viewer1)\n", inet_ntoa(src->sin_addr), want_port);
        }
        return;
    }

    // Stream idle: allow takeover if viewer1 isn't actively keeping ownership via REGs.
    if ((now_s - viewer1_last_reg) > owner_ttl_s)
    {
        set_viewer1_locked(src->sin_addr, want_port);
        pthread_mutex_unlock(&client_lock);
        INFO("[FWD] viewer1 takeover (idle): %s:%d\n", inet_ntoa(src->sin_addr), want_port);
        return;
    }

    pthread_mutex_unlock(&client_lock);
}

static void handle_cmd_from(const struct sockaddr_in *src, const char *s)
{
    if (!src || !s) return;

    pthread_mutex_lock(&client_lock);

    if (!viewer1_valid || !same_in_addr(src->sin_addr, viewer1_ip))
    {
        pthread_mutex_unlock(&client_lock);
        return; // only viewer1 can command
    }

    if (strncasecmp(s, "CMD:IGNORE", 10) == 0)
    {
        pending_active = 0;
        pthread_mutex_unlock(&client_lock);
        INFO("[FWD] CMD:IGNORE\n");
        return;
    }

    if (strncasecmp(s, "CMD:SWITCH:", 11) == 0)
    {
        const char* p = s + 11;
        char ipstr[64]; ipstr[0] = 0;
        int port = 0;
        const char* colon = strchr(p, ':');
        if (colon)
        {
            size_t n = (size_t)(colon - p);
            if (n >= sizeof(ipstr)) n = sizeof(ipstr) - 1;
            memcpy(ipstr, p, n);
            ipstr[n] = 0;
            port = atoi(colon + 1);
        }
        struct in_addr ip;
        if (ipstr[0] && port > 0 && inet_aton(ipstr, &ip))
        {
            set_viewer1_locked(ip, port);
            pthread_mutex_unlock(&client_lock);
            INFO("[FWD] CMD:SWITCH -> %s:%d\n", ipstr, port);
            return;
        }
    }

    if (strncasecmp(s, "CMD:DUP:", 8) == 0)
    {
        const char* p = s + 8;
        char ipstr[64]; ipstr[0] = 0;
        int port = 0;
        const char* colon = strchr(p, ':');
        if (colon)
        {
            size_t n = (size_t)(colon - p);
            if (n >= sizeof(ipstr)) n = sizeof(ipstr) - 1;
            memcpy(ipstr, p, n);
            ipstr[n] = 0;
            port = atoi(colon + 1);
        }
        struct in_addr ip;
        if (ipstr[0] && port > 0 && inet_aton(ipstr, &ip))
        {
            int ok = dup_add_or_refresh_locked(ip, port);
            pthread_mutex_unlock(&client_lock);
            INFO("[FWD] CMD:DUP %s:%d -> %s\n", ipstr, port, ok ? "OK" : "DENIED(maxClients)");
            return;
        }
    }

    pthread_mutex_unlock(&client_lock);
}


// utilities
static double now_seconds() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static int seq_diff(uint16_t a, uint16_t b) {
    int32_t diff = (int32_t)a - (int32_t)b;
    if (diff < -32768) diff += 65536;
    if (diff > 32767) diff -= 65536;
    return diff;
}

static void hexprint(const char *label, const char *buf, size_t n) {
    fprintf(stderr, "%s:", label);
    for (size_t i=0;i<n;i++) fprintf(stderr, " %02x", (unsigned char)buf[i]);
    fprintf(stderr, "\n");
}

static void update_forward_dest(const struct sockaddr_in *src, int port_override)
{
    if (!auto_forward_dest || !src) return;

    struct sockaddr_in dst;
    memset(&dst, 0, sizeof(dst));
    dst.sin_family = AF_INET;
    dst.sin_addr = src->sin_addr;
    dst.sin_port = htons(port_override > 0 ? port_override : out_port);

    pthread_mutex_lock(&fwd_dest_lock);
    fwd_dest_addr = dst;
    fwd_dest_last_seen = now_seconds();
    fwd_dest_gen++;
    pthread_mutex_unlock(&fwd_dest_lock);
}

static int is_streaming(double now_s)
{
    return (now_s - last_video_rx) < stream_idle_s;
}

static int owner_expired(double now_s)
{
    return (now_s - owner_last_seen) > owner_ttl_s;
}

static int same_owner(const struct sockaddr_in *src)
{
    return have_owner && src && (src->sin_addr.s_addr == owner_ip.s_addr);
}

// Returns 1 if the sender is allowed to update/claim destination now.
static int allow_claim_or_refresh(const struct sockaddr_in *src, int port_override, int is_reg)
{
    (void)port_override;

    const double now_s = now_seconds();

    pthread_mutex_lock(&fwd_dest_lock);

    if (!have_owner) {
        owner_ip = src->sin_addr;
        owner_port = (port_override > 0) ? port_override : out_port;
        owner_last_seen = now_s;
        have_owner = 1;
        pthread_mutex_unlock(&fwd_dest_lock);
        return 1;
    }

    if (src->sin_addr.s_addr == owner_ip.s_addr) {
        owner_last_seen = now_s;
        if (port_override > 0) owner_port = port_override;
        pthread_mutex_unlock(&fwd_dest_lock);
        return 1;
    }

    // Non-owner: allow claim only if stream is idle or owner has disappeared.
    // While streaming, lock out other REG/IDR triggers to prevent hijack.
    if (!is_streaming(now_s) || owner_expired(now_s)) {
        owner_ip = src->sin_addr;
        owner_port = (port_override > 0) ? port_override : out_port;
        owner_last_seen = now_s;
        have_owner = 1;
        pthread_mutex_unlock(&fwd_dest_lock);
        return 1;
    }

    pthread_mutex_unlock(&fwd_dest_lock);

    // If not allowed, silently ignore REG; for non-REG triggers, we ignore too.
    (void)is_reg;
    return 0;
}
// token generation (uses /dev/urandom)
static void generate_token(char *out, size_t n) {
    const char *alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
    unsigned char rnd[n];
    FILE *f = fopen("/dev/urandom", "rb");
    if (f) {
        fread(rnd, 1, n, f);
        fclose(f);
    } else {
        for (size_t i=0;i<n;i++) rnd[i] = (unsigned char)(rand() & 0xff);
    }
    for (size_t i=0;i<n;i++) out[i] = alphabet[rnd[i] % (strlen(alphabet))];
    out[n] = '\0';
}

// ack entry helpers
static int add_ack_entry(const char *token) {
    for (int i=0;i<MAX_TOKEN_HISTORY;i++) {
        if (!ack_entries[i].used) {
            pthread_mutex_lock(&ack_entries[i].mutex);
            strncpy(ack_entries[i].token, token, IDR_CODE_LEN+1);
            ack_entries[i].token[IDR_CODE_LEN] = '\0';
            ack_entries[i].used = 1; // waiting
            pthread_mutex_unlock(&ack_entries[i].mutex);
            return i;
        }
    }
    // no free slot -> overwrite random slot (simple approach)
    int i = rand() % MAX_TOKEN_HISTORY;
    pthread_mutex_lock(&ack_entries[i].mutex);
    strncpy(ack_entries[i].token, token, IDR_CODE_LEN+1);
    ack_entries[i].token[IDR_CODE_LEN] = '\0';
    ack_entries[i].used = 1;
    pthread_mutex_unlock(&ack_entries[i].mutex);
    return i;
}

static int mark_token_acked(const char *token) {
    int found = 0;
    for (int i=0;i<MAX_TOKEN_HISTORY;i++) {
        pthread_mutex_lock(&ack_entries[i].mutex);
        if (ack_entries[i].used == 1 &&
            strncmp(ack_entries[i].token, token, IDR_CODE_LEN)==0) {
            ack_entries[i].used = 2; // acked
            pthread_cond_broadcast(&ack_entries[i].cond);
            pthread_mutex_unlock(&ack_entries[i].mutex);
            found = 1;
            break;
        }
        pthread_mutex_unlock(&ack_entries[i].mutex);
    }
    return found;
}

static int wait_for_ack_index(int idx, double timeout_seconds) {
    if (idx < 0 || idx >= MAX_TOKEN_HISTORY) return 0;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    long add_sec = (long)timeout_seconds;
    long add_nsec = (long)((timeout_seconds - add_sec) * 1e9);
    ts.tv_sec += add_sec;
    ts.tv_nsec += add_nsec;
    if (ts.tv_nsec >= 1000000000L) { ts.tv_sec += 1; ts.tv_nsec -= 1000000000L; }

    pthread_mutex_lock(&ack_entries[idx].mutex);
    while (ack_entries[idx].used == 1) {
        int r = pthread_cond_timedwait(&ack_entries[idx].cond, &ack_entries[idx].mutex, &ts);
        if (r == ETIMEDOUT) break;
    }
    int res = (ack_entries[idx].used == 2);
    // cleanup slot
    ack_entries[idx].used = 0;
    pthread_mutex_unlock(&ack_entries[idx].mutex);
    return res;
}

// send token over control socket
static void send_token(const char *token) {
    char payload[IDR_CODE_LEN+2];
    snprintf(payload, sizeof(payload), "%s\n", token);
    struct sockaddr_in addr;
    memset(&addr,0,sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(idr_udp_port);

    if (idr_use_broadcast) {
        struct in_addr cam;
        if (inet_pton(AF_INET, camera_ip, &cam) == 1) {
            uint32_t host = ntohl(cam.s_addr);
            host = (host & 0xFFFFFF00u) | 0x000000FFu; // assume /24 for camera LAN
            addr.sin_addr.s_addr = htonl(host);
        } else {
            addr.sin_addr.s_addr = htonl(INADDR_BROADCAST);
        }
    } else {
        inet_pton(AF_INET, camera_ip, &addr.sin_addr);
    }

    ssize_t s = sendto(sock_ctrl, payload, strlen(payload), 0, (struct sockaddr*)&addr, sizeof(addr));
    if (s < 0) {
        WARN("[WARN] send_token failed: %s\n", strerror(errno));
    }
}

// abort existing request if any
static void cancel_current_request() {
    pthread_mutex_lock(&state_lock);
    if (current_request) {
        current_request->stop = 1;
    }
    pthread_mutex_unlock(&state_lock);
}

// burst thread
struct burst_arg { char token[IDR_CODE_LEN+1]; int ack_idx; struct current_req *owner; };
static void *burst_thread_fn(void *v) {
    struct burst_arg *arg = v;
    for (size_t be=0; be < IDR_BURST_POLICY_LEN; ++be) {
        int copies = IDR_BURST_POLICY[be].copies;
        double spacing = IDR_BURST_POLICY[be].spacing;
        double wait_time = IDR_BURST_POLICY[be].wait_time;
        for (int c=0;c<copies;c++) {
            if (arg->owner->stop) { free(arg); return NULL; }
            send_token(arg->token);
            // sleep spacing
            struct timespec req;
            req.tv_sec = (time_t)spacing;
            req.tv_nsec = (long)((spacing - (time_t)spacing) * 1e9);
            nanosleep(&req, NULL);
        }
        if (use_acks) {
            if (wait_for_ack_index(arg->ack_idx, wait_time)) {
                INFO("[INFO] IDR ACK received token=%s\n", arg->token);
                free(arg);
                return NULL;
            }
        } else {
            // continue sending bursts without waiting
            continue;
        }
    }
    if (use_acks) {
        WARN("[WARN] IDR ACK timeout token=%s\n", arg->token);
    }
    free(arg);
    return NULL;
}

static void request_idr(uint16_t seq, const char *reason) {
    char token[IDR_CODE_LEN+1];
    generate_token(token, IDR_CODE_LEN);

    INFO("[INFO] IDR requested seq=%u reason=%s token=%s\n",
         (unsigned)seq, reason, token);

    // cancel previous
    cancel_current_request();

    // allocate current_request
    struct current_req *req = calloc(1, sizeof(*req));
    if (!req) {
        WARN("[WARN] OOM allocating current_req\n");
        return;
    }
    strncpy(req->token, token, IDR_CODE_LEN+1);
    req->token[IDR_CODE_LEN] = '\0';
    req->stop = 0;
    pthread_mutex_lock(&state_lock);
    current_request = req;
    pthread_mutex_unlock(&state_lock);

    int ack_idx = -1;
    if (use_acks) {
        ack_idx = add_ack_entry(token);
    }

    struct burst_arg *arg = malloc(sizeof(*arg));
    if (!arg) {
        WARN("[WARN] OOM allocating burst_arg\n");
        return;
    }
    strncpy(arg->token, token, IDR_CODE_LEN+1);
    arg->token[IDR_CODE_LEN] = '\0';
    arg->ack_idx = ack_idx;
    arg->owner = req;

    pthread_t t;
    if (pthread_create(&t, NULL, burst_thread_fn, arg) == 0) {
        pthread_detach(t);
    } else {
        WARN("[WARN] pthread_create failed for burst_thread\n");
        free(arg);
    }
}

// ack listener thread
static void *ack_listener_fn(void *v) {
    (void)v;
    char buf[256];
    struct sockaddr_in src;
    socklen_t slen = sizeof(src);
    while (running && use_acks) {
        ssize_t r = recvfrom(sock_ctrl, buf, sizeof(buf)-1, 0, (struct sockaddr*)&src, &slen);
        if (r < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) { continue; }
            if (running) WARN("[WARN] ack_listener recvfrom: %s\n", strerror(errno));
            continue;
        }
        buf[r] = '\0';
        // permissive parsing
        char *s = buf;
        while (*s && (*s=='\r' || *s=='\n')) s++;
        if (!*s) continue;
        char lower[256];
        size_t slen2 = strlen(s);
        for (size_t i=0;i<slen2 && i<sizeof(lower)-1;i++) lower[i] = (char)tolower((unsigned char)s[i]);
        lower[slen2] = '\0';
        char token[IDR_CODE_LEN+1] = {0};
        if (strncmp(lower, "ack:", 4)==0) {
            strncpy(token, s+4, IDR_CODE_LEN);
        } else if (strncmp(lower, "ack ", 4)==0) {
            strncpy(token, s+4, IDR_CODE_LEN);
        } else {
            // maybe raw token
            strncpy(token, s, IDR_CODE_LEN);
        }
        token[IDR_CODE_LEN] = '\0';
        // trim
        for (int i=0;i<IDR_CODE_LEN;i++) {
            if (token[i]=='\r' || token[i]=='\n' || token[i]==' ' || token[i]=='\t') {
                token[i]='\0'; break;
            }
        }
        if (strlen(token) == 0) continue;
        // mark ack; only log once per token
        if (mark_token_acked(token)) {
            INFO("[ACK] received for token=%s\n", token);
        }
    }
    return NULL;
}

// external IDR passthrough listener: listens on idr_udp_port for simple triggers
static void *passthru_listener_fn(void *v) {
    (void)v;
    char buf[256];
    struct sockaddr_in src;
    socklen_t slen = sizeof(src);

    while (running && idr_passthru_enabled) {
        ssize_t r = recvfrom(sock_passthru, buf, sizeof(buf), 0, (struct sockaddr*)&src, &slen);
        if (r < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
            if (running) WARN("[WARN] passthru recvfrom: %s\n", strerror(errno));
            continue;
        }

        if (r >= (ssize_t)sizeof(buf)) r = (ssize_t)sizeof(buf) - 1;
        buf[r] = '\0';

        char *s = buf;
        while (*s && (*s=='\r' || *s=='\n' || *s==' ' || *s=='\t')) s++;
        if (!*s) continue;

        // CMD from viewer1
        if ((tolower((unsigned char)s[0])=='c') &&
            (tolower((unsigned char)s[1])=='m') &&
            (tolower((unsigned char)s[2])=='d') &&
            s[3]==':')
        {
            handle_cmd_from(&src, s);
            continue;
        }

        // REG keepalive (REG[:<port>])
        if (tolower((unsigned char)s[0])=='r' && tolower((unsigned char)s[1])=='e' && tolower((unsigned char)s[2])=='g') {
            int port = 0;
            char *p = strchr(s, ':');
            if (p) port = atoi(p + 1);
            handle_reg_from(&src, port);
            continue;
        }

        // Other content: treat as passthru IDR trigger.
        // Trust only viewer1 (prevents hijack while streaming).
        pthread_mutex_lock(&client_lock);
        int ok = (!viewer1_valid) || same_in_addr(src.sin_addr, viewer1_ip);
        pthread_mutex_unlock(&client_lock);
        if (!ok) {
            INFO("[FWD] IDR trigger ignored from %s:%d (not viewer1)\n",
                 inet_ntoa(src.sin_addr), ntohs(src.sin_port));
            continue;
        }

        INFO("[INFO] IDR passthru trigger from %s:%d -> camera %s:%d (%zd bytes)\n",
             inet_ntoa(src.sin_addr), ntohs(src.sin_port),
             camera_ip, idr_udp_port, r);

        uint16_t seq_for_log = expected_seq_known ? expected_seq : 0;
        request_idr(seq_for_log, "passthru");
    }
    return NULL;
}

// packet handling (uses RTP header: seq at bytes 2-3)
static void handle_packet(const char *data, ssize_t len) {
    if (len < 4) return;
    uint16_t seq = (uint8_t)data[2]; seq <<= 8; seq |= (uint8_t)data[3];
    double now = now_seconds();

    last_packet_time = now;
    if (!stream_active) {
        stream_active = 1;
        if (enable_local_idr) {
            INFO("[INFO] Stream resumed, requesting IDR (resume)\n");
            request_idr(seq, "resume");
        } else {
            INFO("[INFO] Stream resumed (local IDR disabled by default; passthru only unless -l)\n");
        }
    }

    if (enable_local_idr && pending_gap_ts) {
        if (expected_seq_known) {
            int diff = seq_diff(seq, expected_seq);
            if (diff > 1) {
                for (int m=1;m<diff;m++) {
                    uint16_t miss_seq = (expected_seq + m) & 0xFFFF;
                    pending_gap_ts[miss_seq] = now;
                }
            }
        }
    }
    expected_seq = seq;
    expected_seq_known = 1;
}

// forward loop
static void *forward_loop_fn(void *v) {
    (void)v;
    char buf[BUFFER_SIZE];
    struct sockaddr_in src;
    socklen_t slen = sizeof(src);

    while (running) {
        ssize_t r = recvfrom(sock_in, buf, sizeof(buf), 0, (struct sockaddr*)&src, &slen);
        if (r < 0) {
            if (errno == EINTR) continue;
            WARN("[WARN] forward recvfrom failed: %s\n", strerror(errno));
            continue;
        }

        // Track stream activity (used for streaming/idle decisions)
        last_video_rx = now_seconds();

        // Use full RTP packet for gap/IDR logic
        handle_packet(buf, r);

        // Decide what to forward (RTP vs payload-only)
        const char *send_ptr = buf;
        ssize_t send_len = r;
        if (strip_rtp && r > 12) {
            send_ptr = buf + 12;
            send_len = r - 12;
        }

        // Snapshot destinations under lock
        struct sockaddr_in dsts[1 + MAX_DUP_CLIENTS];
        int dstn = 0;

        pthread_mutex_lock(&client_lock);
        double now_s = now_seconds();
        prune_dups_if_idle_locked(now_s);

        if (viewer1_valid) {
            struct sockaddr_in d;
            memset(&d, 0, sizeof(d));
            d.sin_family = AF_INET;
            d.sin_addr = viewer1_ip;
            int port = (viewer1_port_cfg > 0) ? viewer1_port_cfg : out_port;
            d.sin_port = htons(port);
            dsts[dstn++] = d;

            for (int i=0;i<MAX_DUP_CLIENTS;i++) {
                if (dup_clients[i].used) {
                    struct sockaddr_in e;
                    memset(&e, 0, sizeof(e));
                    e.sin_family = AF_INET;
                    e.sin_addr = dup_clients[i].ip;
                    int p = (dup_clients[i].port > 0) ? dup_clients[i].port : out_port;
                    e.sin_port = htons(p);
                    dsts[dstn++] = e;
                }
            }
        }
        pthread_mutex_unlock(&client_lock);

        // If nobody is configured yet, drop (waiting for config or REG takeover).
        if (dstn <= 0)
            continue;

        for (int i=0;i<dstn;i++) {
            ssize_t s = sendto(sock_out, send_ptr, send_len, 0, (struct sockaddr*)&dsts[i], sizeof(dsts[i]));
            if (s < 0) {
                WARN("[WARN] forward sendto failed: %s\n", strerror(errno));
            }
        }
    }
    return NULL;
}

// loss loop
static void *loss_loop_fn(void *v) {
    (void)v;
    if (!enable_local_idr || !pending_gap_ts) return NULL;
    while (running) {
        struct timespec req = {0, 20000000}; // 20ms
        nanosleep(&req, NULL);
        double now = now_seconds();
        int found = -1;
        for (int i=0;i<65536;i++) {
            double t = pending_gap_ts[i];
            if (t > 0.0 && now - t > micro_loss_timeout) { found = i; break; }
        }
        if (found >= 0 && stream_active) {
            request_idr((uint16_t)found, "loss");
            // clear all expired
            for (int i=0;i<65536;i++) {
                double t = pending_gap_ts[i];
                if (t > 0.0 && now - t > micro_loss_timeout) pending_gap_ts[i] = 0.0;
            }
        }
    }
    return NULL;
}

// stream monitor
static void *stream_monitor_fn(void *v) {
    (void)v;
    while (running) {
        struct timespec req = {0, 50000000}; // 50ms
        nanosleep(&req, NULL);
        double now = now_seconds();
        if (stream_active && now - last_packet_time > stream_down_timeout) {
            stream_active = 0;
            expected_seq_known = 0;
            INFO("[INFO] Stream inactive\n");
        }
    }
    return NULL;
}

// signal handler
static void handle_signal(int sig) {
    (void)sig; running = 0;
}


// ----- simple TCP port forwarders (HTTP / SSH) -----
// Notes:
//  - SCP runs over SSH, so forwarding TCP/22 covers both.
//  - We default to disabled to avoid clashing with an existing sshd/socat on the forwarder host.

struct tcp_conn_arg {
    int client_fd;
    char target_ip[64];
    int target_port;
    const char *name;
};

static int send_all(int fd, const uint8_t *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t s = send(fd, buf + off, len - off, MSG_NOSIGNAL);
        if (s > 0) { off += (size_t)s; continue; }
        if (s < 0 && (errno == EINTR)) continue;
        if (s < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { usleep(1000); continue; }
        return -1;
    }
    return 0;
}

static void *tcp_conn_worker_fn(void *v) {
    struct tcp_conn_arg *a = (struct tcp_conn_arg *)v;

    int target_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (target_fd < 0) {
        WARN("[WARN] %s: socket(target) failed: %s\n", a->name, strerror(errno));
        close(a->client_fd);
        free(a);
        return NULL;
    }

    struct sockaddr_in taddr;
    memset(&taddr, 0, sizeof(taddr));
    taddr.sin_family = AF_INET;
    taddr.sin_port = htons((uint16_t)a->target_port);
    if (inet_pton(AF_INET, a->target_ip, &taddr.sin_addr) != 1) {
        WARN("[WARN] %s: bad target ip '%s'\n", a->name, a->target_ip);
        close(target_fd);
        close(a->client_fd);
        free(a);
        return NULL;
    }

    if (connect(target_fd, (struct sockaddr*)&taddr, sizeof(taddr)) < 0) {
        WARN("[WARN] %s: connect to %s:%d failed: %s\n", a->name, a->target_ip, a->target_port, strerror(errno));
        close(target_fd);
        close(a->client_fd);
        free(a);
        return NULL;
    }

    uint8_t buf[32768];
    struct pollfd pfds[2];
    pfds[0].fd = a->client_fd; pfds[0].events = POLLIN;
    pfds[1].fd = target_fd;    pfds[1].events = POLLIN;

    while (running) {
        int pr = poll(pfds, 2, 200);
        if (pr < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (pr == 0) continue;

        // client -> target
        if (pfds[0].revents & POLLIN) {
            ssize_t r = recv(a->client_fd, buf, sizeof(buf), 0);
            if (r <= 0) break;
            if (send_all(target_fd, buf, (size_t)r) < 0) break;
        } else if (pfds[0].revents & (POLLHUP|POLLERR|POLLNVAL)) {
            break;
        }

        // target -> client
        if (pfds[1].revents & POLLIN) {
            ssize_t r = recv(target_fd, buf, sizeof(buf), 0);
            if (r <= 0) break;
            if (send_all(a->client_fd, buf, (size_t)r) < 0) break;
        } else if (pfds[1].revents & (POLLHUP|POLLERR|POLLNVAL)) {
            break;
        }
    }

    close(target_fd);
    close(a->client_fd);
    free(a);
    return NULL;
}

struct tcp_proxy_cfg {
    int listen_port;
    int target_port;
    const char *name;
    int *listen_sock_out; // where to store the listener fd
};

static void *tcp_proxy_listener_fn(void *v) {
    struct tcp_proxy_cfg *cfg = (struct tcp_proxy_cfg *)v;

    int ls = socket(AF_INET, SOCK_STREAM, 0);
    if (ls < 0) {
        WARN("[WARN] %s: socket(listen) failed: %s\n", cfg->name, strerror(errno));
        return NULL;
    }

    int one = 1;
    setsockopt(ls, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in laddr;
    memset(&laddr, 0, sizeof(laddr));
    laddr.sin_family = AF_INET;
    laddr.sin_port = htons((uint16_t)cfg->listen_port);
    laddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(ls, (struct sockaddr*)&laddr, sizeof(laddr)) < 0) {
        WARN("[WARN] %s: bind :%d failed: %s\n", cfg->name, cfg->listen_port, strerror(errno));
        close(ls);
        return NULL;
    }
    if (listen(ls, 16) < 0) {
        WARN("[WARN] %s: listen failed: %s\n", cfg->name, strerror(errno));
        close(ls);
        return NULL;
    }

    if (cfg->listen_sock_out) *cfg->listen_sock_out = ls;

    INFO("[INFO] %s: listening on :%d -> %s:%d\n", cfg->name, cfg->listen_port, camera_ip, cfg->target_port);

    while (running) {
        struct pollfd pfd;
        pfd.fd = ls;
        pfd.events = POLLIN;
        int pr = poll(&pfd, 1, 200);
        if (pr < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (pr == 0) continue;
        if (!(pfd.revents & POLLIN)) {
            if (pfd.revents & (POLLHUP|POLLERR|POLLNVAL)) break;
            continue;
        }

        struct sockaddr_in caddr;
        socklen_t clen = sizeof(caddr);
        int cfd = accept(ls, (struct sockaddr*)&caddr, &clen);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            if (!running) break;
            continue;
        }

        struct tcp_conn_arg *a = calloc(1, sizeof(*a));
        if (!a) { close(cfd); continue; }
        a->client_fd = cfd;
        strncpy(a->target_ip, camera_ip, sizeof(a->target_ip)-1);
        a->target_port = cfg->target_port;
        a->name = cfg->name;
        pthread_t t;
        pthread_create(&t, NULL, tcp_conn_worker_fn, a);
        pthread_detach(t);
    }

    close(ls);
    return NULL;
}

// usage
static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s [options]\n"
        "\n"
        "Options:\n"
        "  -p, --in-port <port>            UDP input port (default %d)\n"
        "  -o, --out-port <port>           UDP output port (default %d)\n"
        "  -f, --forward-to-ip <ip|auto>   Forward UDP stream to this IP, or \"auto\" to learn destination (default %s)\n"
        "  -c, --camera-ip <ip>            Camera IP (default %s)\n"
        "  -i, --idr-port <port>           Camera IDR UDP port (default %d)\n"
        "  -L, --ctrl-port <port>          Local control/ACK port (default %d)\n"
        "  -m, --micro-timeout <sec>       Micro loss timeout (default 0.020)\n"
        "  -t, --stream-timeout <sec>      Stream-down timeout (default 3.000)\n"
        "  -U, --idr-unicast               Send IDR requests directly to camera_ip (default is broadcast)\n"
        "  -B, --idr-broadcast             Send IDR requests to directed broadcast derived from camera_ip (/24)\n"
        "  -R, --strip-rtp                 Strip 12-byte RTP header before forwarding\n"
        "  -A, --no-acks                   Disable ACK handling\n"
        "  -P, --disable-idr-passthru      Do not listen for external IDR requests\n"
        "  -l, --enable-local-idr          Enable local IDR generation (default OFF)\n"
        "  -w, --http-fwd-port <port>      Listen on TCP <port> and forward to camera_ip:80  (default %d)\n"
        "  -s, --ssh-fwd-port <port>       Listen on TCP <port> and forward to camera_ip:22  (default %d)\n"
        "  -q, --iq-fwd-port <port>        Listen on TCP <port> and forward to camera_ip:9876 (default %d)\n"
        "  -H, --disable-http-ssh-fwd      Disable HTTP+SSH+IQ TCP forwarders (default ON)\n"
        "  -v, --verbose                   Enable verbose logging\n"
        "  -h, --help                      Show this help\n"
        "\n"
        "Example:\n"
        "  %s -f 192.168.8.10 -o 5600\n"
        "\n"
        "  What this does:\n"
        "    - Listens for UDP video on 0.0.0.0:%d (default -p)\n"
        "    - Forwards it to 192.168.8.10:5600 (-f and -o)\n"
        "    - Uses default camera %s:%d for IDR requests\n"
        "    - IDR passthru listener is ON by default (disable with -P)\n"
        "    - Local IDR generation is OFF by default (enable with -l)\n"
        "    - HTTP forwarder listens on 0.0.0.0:%d -> %s:80 (disable with -H)\n"
        "    - SSH forwarder  listens on 0.0.0.0:%d -> %s:22 (disable with -H)\n"
        "    - IQ forwarder   listens on 0.0.0.0:%d -> %s:9876 (disable with -H)\n"
        ,
        prog,
        UDP_PORT_DEFAULT,
        OUT_PORT_DEFAULT,
        FORWARD_IP_DEFAULT,
        CAMERA_IP_DEFAULT,
        IDR_UDP_PORT_DEFAULT,
        IDR_CLIENT_PORT_DEFAULT,
        HTTP_FWD_PORT_DEFAULT,
        SSH_FWD_PORT_DEFAULT,
        IQ_FWD_PORT_DEFAULT,
        prog,
        UDP_PORT_DEFAULT,
        CAMERA_IP_DEFAULT,
        IDR_UDP_PORT_DEFAULT,
        HTTP_FWD_PORT_DEFAULT, CAMERA_IP_DEFAULT,
        SSH_FWD_PORT_DEFAULT, CAMERA_IP_DEFAULT,
        IQ_FWD_PORT_DEFAULT,  CAMERA_IP_DEFAULT
    );
}

int main(int argc, char **argv) {
    srand((unsigned)time(NULL));
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    signal(SIGPIPE, SIG_IGN);

    // command-line parsing
        static struct option long_opts[] = {
        {"in-port",               required_argument, 0, 'p'},
        {"out-port",              required_argument, 0, 'o'},
        {"forward-to-ip",         required_argument, 0, 'f'},
        {"camera-ip",             required_argument, 0, 'c'},
        {"idr-port",              required_argument, 0, 'i'},
        {"ctrl-port",             required_argument, 0, 'L'},
        {"micro-timeout",         required_argument, 0, 'm'},
        {"stream-timeout",        required_argument, 0, 't'},
        {"idr-unicast",           no_argument,       0, 'U'},
        {"idr-broadcast",         no_argument,       0, 'B'},
        {"strip-rtp",             no_argument,       0, 'R'},
        {"no-acks",               no_argument,       0, 'A'},
        {"disable-idr-passthru",  no_argument,       0, 'P'},
        {"enable-local-idr",      no_argument,       0, 'l'},
        {"http-fwd-port",         required_argument, 0, 'w'},
        {"ssh-fwd-port",          required_argument, 0, 's'},
        {"iq-fwd-port",           required_argument, 0, 'q'},
        {"disable-http-ssh-fwd",  no_argument,       0, 'H'},
        {"verbose",               no_argument,       0, 'v'},
        {"help",                  no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    int opt;
    while ((opt = getopt_long(argc, argv, "p:o:f:c:i:L:m:t:UBRAPlw:s:q:Hvh", long_opts, NULL)) != -1) {
        switch (opt) {
            case 'p':
                udp_port = atoi(optarg);
                break;
            case 'o':
                out_port = atoi(optarg);
                break;
            case 'f':
                strncpy(forward_ip, optarg, sizeof(forward_ip) - 1);
                forward_ip[sizeof(forward_ip) - 1] = '\0';
                break;
            case 'c':
                strncpy(camera_ip, optarg, sizeof(camera_ip) - 1);
                camera_ip[sizeof(camera_ip) - 1] = '\0';
                break;
            case 'i':
                idr_udp_port = atoi(optarg);
                break;
            case 'L':
                idr_client_port = atoi(optarg);
                break;
            case 'm':
                micro_loss_timeout = atof(optarg);
                break;
            case 't':
                stream_down_timeout = atof(optarg);
                break;
            case 'U':
                idr_use_broadcast = 0;
                break;
            case 'B':
                idr_use_broadcast = 1;
                break;
            case 'R':
                strip_rtp = 1;
                break;
            case 'A':
                use_acks = 0;
                break;
            case 'P':
                idr_passthru_enabled = 0;
                break;
            case 'l':
                enable_local_idr = 1;
                break;
            case 'w':
                http_fwd_port = atoi(optarg);
                break;
            case 's':
                ssh_fwd_port = atoi(optarg);
                break;
            case 'q':
                iq_fwd_port = atoi(optarg);
                break;
            case 'H':
                http_ssh_fwd_enabled = 0;
                break;
            case 'v':
                verbose = 1;
                break;
            case 'h':
                usage(argv[0]);
                return 0;
            default:
                usage(argv[0]);
                return 1;
        }
    }

    // Load sticky forwarding config (viewer1/maxClients)
    conf_load();

    // If user provided a fixed -f <ip>, seed viewer1 (unless config already has one)
    if (strcasecmp(forward_ip, "auto") != 0) {
        struct in_addr ip;
        if (inet_aton(forward_ip, &ip)) {
            pthread_mutex_lock(&client_lock);
            if (!viewer1_valid) {
                viewer1_ip = ip;
                viewer1_valid = 1;
                viewer1_port_cfg = 0;
                viewer1_last_reg = 0.0;
                conf_save_locked();
            }
            pthread_mutex_unlock(&client_lock);
        }
    }


    if (enable_local_idr) {
        pending_gap_ts = calloc(65536, sizeof(double));
        if (!pending_gap_ts) {
            fprintf(stderr, "OOM allocating pending_gap_ts\n");
            return 1;
        }
    } else {
        pending_gap_ts = NULL;
    }

    // init ack entries
    for (int i=0;i<MAX_TOKEN_HISTORY;i++) {
        ack_entries[i].used = 0;
        pthread_mutex_init(&ack_entries[i].mutex, NULL);
        pthread_cond_init(&ack_entries[i].cond, NULL);
    }

    // setup sockets
    sock_in = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_in < 0) { perror("socket in"); return 1; }
    struct sockaddr_in in_addr;
    memset(&in_addr,0,sizeof(in_addr));
    in_addr.sin_family = AF_INET;
    in_addr.sin_port = htons(udp_port);
    in_addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(sock_in, (struct sockaddr*)&in_addr, sizeof(in_addr)) < 0) {
        perror("bind in");
        return 1;
    }

    sock_out = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_out < 0) { perror("socket out"); return 1; }

    sock_ctrl = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_ctrl < 0) { perror("socket ctrl"); return 1; }
    int reuse = 1;
    setsockopt(sock_ctrl, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in ctrl_addr;
    memset(&ctrl_addr,0,sizeof(ctrl_addr));
    ctrl_addr.sin_family = AF_INET;
    ctrl_addr.sin_port = htons(idr_client_port);
    ctrl_addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(sock_ctrl, (struct sockaddr*)&ctrl_addr, sizeof(ctrl_addr)) < 0) {
        perror("bind ctrl");
        return 1;
    }

    {
        int one = 1;
        setsockopt(sock_ctrl, SOL_SOCKET, SO_BROADCAST, &one, sizeof(one));
    }

    if (use_acks) {
        struct timeval tv = {0, 200000}; // 200ms
        setsockopt(sock_ctrl, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }

    // optional external IDR passthru listener on idr_udp_port
    if (idr_passthru_enabled) {
        sock_passthru = socket(AF_INET, SOCK_DGRAM, 0);
        if (sock_passthru < 0) {
            perror("socket passthru");
            return 1;
        }
        int reuse2 = 1;
        setsockopt(sock_passthru, SOL_SOCKET, SO_REUSEADDR, &reuse2, sizeof(reuse2));
        struct sockaddr_in pass_addr;
        memset(&pass_addr, 0, sizeof(pass_addr));
        pass_addr.sin_family = AF_INET;
        pass_addr.sin_port = htons(idr_udp_port);
        pass_addr.sin_addr.s_addr = INADDR_ANY;
        if (bind(sock_passthru, (struct sockaddr*)&pass_addr, sizeof(pass_addr)) < 0) {
            perror("bind passthru");
            return 1;
        }
        struct timeval ptv = {0, 200000}; // 200ms
        setsockopt(sock_passthru, SOL_SOCKET, SO_RCVTIMEO, &ptv, sizeof(ptv));
    }

    INFO("[INFO] Listening on %d, forwarding to %s:%d (strip_rtp=%d). "
         "IDR control via %s:%d (USE_ACKS=%d, passthru=%s, local_idr=%s, idr_send=%s)\n",
         udp_port, forward_ip, out_port, strip_rtp,
         camera_ip, idr_udp_port, use_acks,
         idr_passthru_enabled ? "on" : "off",
         enable_local_idr ? "on" : "off",
         idr_use_broadcast ? "broadcast" : "unicast");

    if (http_ssh_fwd_enabled) {
        if (http_fwd_port > 0) INFO("[INFO] HTTP forward enabled: :%d -> %s:80\n", http_fwd_port, camera_ip);
        if (ssh_fwd_port  > 0) INFO("[INFO] SSH forward enabled:  :%d -> %s:22\n", ssh_fwd_port, camera_ip);
        if (iq_fwd_port   > 0) INFO("[INFO] IQ forward enabled:   :%d -> %s:9876\n", iq_fwd_port, camera_ip);
    } else {
        INFO("[INFO] HTTP+SSH+IQ forward disabled (-H)\n");
    }
if (!enable_local_idr && !idr_passthru_enabled) {
        WARN("[WARN] Local IDR generation disabled and passthru disabled: no IDR requests will be generated.\n");
    }

    pthread_t forward_t, loss_t, mon_t, ack_t, passthru_t;
    pthread_create(&forward_t, NULL, forward_loop_fn, NULL);
    if (enable_local_idr) pthread_create(&loss_t, NULL, loss_loop_fn, NULL);
    pthread_create(&mon_t, NULL, stream_monitor_fn, NULL);
    if (use_acks) pthread_create(&ack_t, NULL, ack_listener_fn, NULL);
    if (idr_passthru_enabled && sock_passthru >= 0) pthread_create(&passthru_t, NULL, passthru_listener_fn, NULL);

pthread_t http_t, ssh_t, iq_t;
struct tcp_proxy_cfg http_cfg = {0}, ssh_cfg = {0}, iq_cfg = {0};

if (http_ssh_fwd_enabled) {
    if (http_fwd_port > 0) {
        http_cfg.listen_port = http_fwd_port;
        http_cfg.target_port = 80;
        http_cfg.name = "HTTP-FWD";
        http_cfg.listen_sock_out = &sock_http_listen;
        pthread_create(&http_t, NULL, tcp_proxy_listener_fn, &http_cfg);
    }

    if (ssh_fwd_port > 0) {
        ssh_cfg.listen_port = ssh_fwd_port;
        ssh_cfg.target_port = 22;
        ssh_cfg.name = "SSH-FWD";
        ssh_cfg.listen_sock_out = &sock_ssh_listen;
        pthread_create(&ssh_t, NULL, tcp_proxy_listener_fn, &ssh_cfg);
    }

    if (iq_fwd_port > 0) {
        iq_cfg.listen_port = iq_fwd_port;
        iq_cfg.target_port = 9876;
        iq_cfg.name = "IQ-FWD";
        iq_cfg.listen_sock_out = &sock_iq_listen;
        pthread_create(&iq_t, NULL, tcp_proxy_listener_fn, &iq_cfg);
    }
}

    while (running) sleep(1);

    // cleanup
    if (sock_http_listen >= 0) close(sock_http_listen);
    if (sock_ssh_listen  >= 0) close(sock_ssh_listen);
    if (sock_iq_listen   >= 0) close(sock_iq_listen);
    close(sock_in);
    close(sock_out);
    close(sock_ctrl);
    if (sock_passthru >= 0) close(sock_passthru);
    free(pending_gap_ts);
    INFO("[INFO] exiting\n");
    return 0;
}
