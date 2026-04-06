# Token Bucket Filter — CSCI 402 Operating Systems (USC)

## Problem Statement

This project simulates a **token bucket filter** — a classic network
traffic shaper — using multi-threading in C.

A token bucket filter controls the rate at which packets are transmitted.
Packets arrive at rate λ (lambda) packets/second. Each packet needs P tokens
to become eligible for transmission. Tokens are added to a bucket at rate r
tokens/second. The bucket has a maximum depth of B tokens — overflow tokens
are discarded. A server transmits eligible packets at rate μ (mu)
packets/second.

### System Components
Packets (rate λ) → Q1 → [Token Bucket, depth B, rate r] → Q2 → Server S (rate u)
- **Q1** — waiting queue for packets that don't yet have enough tokens
- **Token bucket** — holds up to B tokens, filled at rate r tokens/sec
- **Q2** — ready queue for packets that have received their P tokens
- **Server S** — transmits packets from Q2 at rate μ packets/sec

### Rules
- A packet arriving at Q1 immediately moves to Q2 if the bucket has ≥ P tokens
- If P > B, the packet can never be served and is dropped immediately
- Tokens that arrive when the bucket is full are discarded (overflow)
- On SIGINT (Ctrl+C), all threads stop gracefully and statistics are printed

---

## Solution

### Approach
The system is implemented using **3 POSIX threads** running concurrently
inside a single process:

| Thread | Role |
|---|---|
| `arrival_thread` | Wakes every 1/λ seconds, creates a packet, adds it to Q1 |
| `token_thread` | Wakes every 1/r seconds, adds a token, moves Q1 → Q2 if possible |
| `server_thread` | Waits for Q2 to have a packet, serves it in 1/μ seconds |

### Synchronization
- A single **mutex** protects all shared state (Q1, Q2, token count)
- A **condition variable** lets the server sleep efficiently until Q2 is non-empty
- A **SIGINT handler** sets a flag that wakes and stops all threads gracefully

### OS Concepts Demonstrated
- Thread creation and lifecycle (`pthread_create`, `pthread_join`)
- Mutual exclusion (`pthread_mutex_lock/unlock`)
- Condition variables (`pthread_cond_wait`, `pthread_cond_signal`)
- Producer-consumer problem
- Race condition prevention
- Deadlock avoidance (mutex released before blocking sleep)
- Signal handling (`SIGINT`)
- Dynamic memory management (`malloc`/`free`)
- Shared memory between threads
- CPU scheduling and concurrency

---

## How to Build and Run

### Requirements
- Linux or WSL2 (Windows Subsystem for Linux)
- GCC with pthread support

### Build
```bash
make
```

### Run
```bash
./warmup2 -lambda 1 -mu 0.35 -r 1 -B 10 -P 3 -n 10
```

### Parameters

| Flag | Meaning | Default |
|---|---|---|
| `-lambda` | Packet arrival rate (packets/sec) | 1.0 |
| `-mu` | Server transmission rate (packets/sec) | 0.35 |
| `-r` | Token arrival rate (tokens/sec) | 1.0 |
| `-B` | Token bucket depth (max tokens) | 10 |
| `-P` | Tokens required per packet | 3 |
| `-n` | Total number of packets to simulate | 20 |

### Example Output
Emulation Parameters:
lambda=1  mu=0.35  r=1  B=10  P=3  n=5
p01: arriving, needs 3 tokens, joined Q1
token t1: bucket now has 1 token
token t2: bucket now has 2 token
token t3: bucket now has 3 token
p01: removed from Q1, joined Q2
p01: departs at 3842.310ms  Q1=2839.42ms  Q2=0.05ms  svc=2857.14ms  total=5696.61ms
...
Simulation complete.
Packets dropped : 0
Tokens dropped  : 0

---

## File Structure


warmup2/
├── warmup2.c      # All thread logic, main(), signal handler
├── Makefile       # Build instructions
└── README.md      # This file



---

