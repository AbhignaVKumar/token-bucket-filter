#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#include <sys/time.h>
#include <math.h>

/* ── simulation parameters ── */
int    num_packets  = 20;
double lambda       = 1.0;
double mu           = 0.35;
double r            = 1.0;
int    B            = 10;
int    P            = 3;

/* ── shared state ── */
int    token_count    = 0;
int    pkts_done      = 0;
int    pkts_dropped   = 0;
int    tokens_dropped = 0;
int    sig_received   = 0;

/* ── synchronization ── */
pthread_mutex_t mutex        = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  q2_not_empty = PTHREAD_COND_INITIALIZER;

/* ── timing ── */
struct timeval start_time;

/* ────────────────────────────────────────────
   Packet struct and Queue
──────────────────────────────────────────── */
typedef struct Packet {
    int            id;
    int            tokens_needed;
    struct timeval arrive_time;
    struct timeval q1_leave;
    struct timeval q2_leave;
    struct timeval depart_time;
    struct Packet *next;
} Packet;

Packet *Q1_head = NULL, *Q1_tail = NULL;
Packet *Q2_head = NULL, *Q2_tail = NULL;
int     Q1_size = 0,     Q2_size = 0;

void enqueue(Packet **head, Packet **tail, int *size, Packet *p) {
    p->next = NULL;
    if (*tail) (*tail)->next = p;
    else       *head = p;
    *tail = p;
    (*size)++;
}

Packet *dequeue(Packet **head, Packet **tail, int *size) {
    if (!*head) return NULL;
    Packet *p = *head;
    *head = p->next;
    if (!*head) *tail = NULL;
    (*size)--;
    return p;
}

/* ────────────────────────────────────────────
   Time helpers
──────────────────────────────────────────── */
double elapsed_ms(struct timeval *since) {
    struct timeval now;
    gettimeofday(&now, NULL);
    return (now.tv_sec  - since->tv_sec)  * 1000.0
         + (now.tv_usec - since->tv_usec) / 1000.0;
}

double diff_ms(struct timeval *a, struct timeval *b) {
    /* returns a - b in milliseconds */
    return (a->tv_sec  - b->tv_sec)  * 1000.0
         + (a->tv_usec - b->tv_usec) / 1000.0;
}

/* ────────────────────────────────────────────
   Try to move head of Q1 into Q2
   (call this while holding the mutex)
──────────────────────────────────────────── */
void try_move_q1_to_q2(void) {
    while (Q1_head != NULL && token_count >= Q1_head->tokens_needed) {
        Packet *front = dequeue(&Q1_head, &Q1_tail, &Q1_size);
        token_count -= front->tokens_needed;
        gettimeofday(&front->q1_leave, NULL);
        enqueue(&Q2_head, &Q2_tail, &Q2_size, front);
        printf("p%02d: removed from Q1, joined Q2 (token bucket now has %d token(s))\n",
               front->id, token_count);
        pthread_cond_signal(&q2_not_empty);
    }
}

/* ────────────────────────────────────────────
   Arrival Thread
──────────────────────────────────────────── */
void *arrival_thread(void *arg) {
    long interval_us = (long)(1000000.0 / lambda);

    for (int i = 1; i <= num_packets; i++) {
        usleep(interval_us);

        if (sig_received) break;

        pthread_mutex_lock(&mutex);

        Packet *p = (Packet *)malloc(sizeof(Packet));
        p->id           = i;
        p->tokens_needed = P;
        p->next         = NULL;
        gettimeofday(&p->arrive_time, NULL);

        printf("p%02d: %.3fms arrives, needs %d tokens\n",
               i, elapsed_ms(&start_time), P);

        /* packet can never be served if P > B — drop immediately */
        if (p->tokens_needed > B) {
            printf("p%02d: dropped (needs %d tokens, bucket depth B=%d)\n",
                   i, P, B);
            pkts_dropped++;
            free(p);
            pthread_mutex_unlock(&mutex);
            continue;
        }

        enqueue(&Q1_head, &Q1_tail, &Q1_size, p);
        printf("p%02d: joined Q1 (Q1 now has %d packet(s))\n", i, Q1_size);

        try_move_q1_to_q2();

        pthread_mutex_unlock(&mutex);
    }
    return NULL;
}

/* ────────────────────────────────────────────
   Token Thread
──────────────────────────────────────────── */
void *token_thread(void *arg) {
    long interval_us = (long)(1000000.0 / r);
    int  token_id    = 0;

    while (!sig_received) {
        usleep(interval_us);

        if (sig_received) break;

        pthread_mutex_lock(&mutex);

        token_id++;

        if (token_count < B) {
            token_count++;
            printf("token t%d: %.3fms arrives, token bucket now has %d token(s)\n",
                   token_id, elapsed_ms(&start_time), token_count);
        } else {
            tokens_dropped++;
            printf("token t%d: %.3fms arrives, dropped (bucket full at B=%d)\n",
                   token_id, elapsed_ms(&start_time), B);
        }

        try_move_q1_to_q2();

        /* stop when all packets are accounted for */
        if (pkts_done + pkts_dropped >= num_packets) {
            pthread_mutex_unlock(&mutex);
            break;
        }

        pthread_mutex_unlock(&mutex);
    }
    return NULL;
}

/* ────────────────────────────────────────────
   Server Thread
──────────────────────────────────────────── */
void *server_thread(void *arg) {
    long service_us = (long)(1000000.0 / mu);

    while (1) {
        pthread_mutex_lock(&mutex);

        /* block until Q2 has a packet, or we are done */
        while (Q2_size == 0
               && pkts_done + pkts_dropped < num_packets
               && !sig_received) {
            pthread_cond_wait(&q2_not_empty, &mutex);
        }

        /* exit if signal received or all packets done */
        if (sig_received
            || (Q2_size == 0 && pkts_done + pkts_dropped >= num_packets)) {
            pthread_mutex_unlock(&mutex);
            break;
        }

        Packet *p = dequeue(&Q2_head, &Q2_tail, &Q2_size);
        gettimeofday(&p->q2_leave, NULL);   /* service starts now */
        printf("p%02d: %.3fms begins service\n",
               p->id, elapsed_ms(&start_time));

        pthread_mutex_unlock(&mutex);

        /* serve the packet — sleep OUTSIDE the mutex */
        usleep(service_us);

        pthread_mutex_lock(&mutex);
        gettimeofday(&p->depart_time, NULL);
        pkts_done++;

        double q1_ms  = diff_ms(&p->q1_leave,    &p->arrive_time);
        double q2_ms  = diff_ms(&p->q2_leave,     &p->q1_leave);
        double svc_ms = diff_ms(&p->depart_time,  &p->q2_leave);
        double tot_ms = diff_ms(&p->depart_time,  &p->arrive_time);

        printf("p%02d: %.3fms departs  (Q1=%.3fms  Q2=%.3fms  svc=%.3fms  total=%.3fms)\n",
               p->id, elapsed_ms(&start_time),
               q1_ms, q2_ms, svc_ms, tot_ms);

        free(p);

        if (Q2_size > 0) pthread_cond_signal(&q2_not_empty);

        pthread_mutex_unlock(&mutex);
    }
    return NULL;
}

/* ────────────────────────────────────────────
   Signal Handler  (Ctrl+C)
──────────────────────────────────────────── */
void handle_sigint(int sig) {
    sig_received = 1;
    pthread_cond_broadcast(&q2_not_empty);
}

/* ────────────────────────────────────────────
   main
──────────────────────────────────────────── */
int main(int argc, char *argv[]) {

    /* parse command-line arguments */
    for (int i = 1; i < argc; i++) {
        if      (strcmp(argv[i], "-lambda") == 0) lambda      = atof(argv[++i]);
        else if (strcmp(argv[i], "-mu")     == 0) mu          = atof(argv[++i]);
        else if (strcmp(argv[i], "-r")      == 0) r           = atof(argv[++i]);
        else if (strcmp(argv[i], "-B")      == 0) B           = atoi(argv[++i]);
        else if (strcmp(argv[i], "-P")      == 0) P           = atoi(argv[++i]);
        else if (strcmp(argv[i], "-n")      == 0) num_packets = atoi(argv[++i]);
    }

    signal(SIGINT, handle_sigint);

    printf("Emulation Parameters:\n");
    printf("  number of packets = %d\n",  num_packets);
    printf("  lambda            = %.4g\n", lambda);
    printf("  mu                = %.4g\n", mu);
    printf("  r                 = %.4g\n", r);
    printf("  bucket depth  B   = %d\n",  B);
    printf("  tokens/packet P   = %d\n\n", P);

    gettimeofday(&start_time, NULL);

    pthread_t t_arrive, t_token, t_server;
    pthread_create(&t_arrive, NULL, arrival_thread, NULL);
    pthread_create(&t_token,  NULL, token_thread,   NULL);
    pthread_create(&t_server, NULL, server_thread,  NULL);

    pthread_join(t_arrive, NULL);
    pthread_join(t_token,  NULL);
    pthread_join(t_server, NULL);

    /* ── final statistics ── */
    printf("\nSimulation terminated.\n");
    printf("  packets arrived  : %d\n", pkts_done + pkts_dropped);
    printf("  packets served   : %d\n", pkts_done);
    printf("  packets dropped  : %d\n", pkts_dropped);
    printf("  tokens dropped   : %d\n", tokens_dropped);

    return 0;
}
