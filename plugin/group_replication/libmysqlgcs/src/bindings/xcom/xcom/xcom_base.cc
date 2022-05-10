/* Copyright (c) 2012, 2021, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2021, 2022, GreatDB Software Co., Ltd

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <assert.h>
#include <errno.h>
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#ifndef _WIN32
#include <inttypes.h>
#endif
#include <limits.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#ifdef _MSC_VER
#include <stdint.h>
#endif

#ifndef _WIN32
#include <poll.h>
#endif

#ifdef _WIN32
#define xcom_buf char
#else
#define xcom_buf void
#endif

/**
  @file
  xcom/xcom_base.c
    The new version of xcom is a major rewrite to allow
    transmission of multiple messages from several sources
    simultaneously without collision. The interface to xcom is largely
    intact, one notable change is that xcom will consider the message
    delivered as soon as it has got a majority. Consequently, the VP
    set will not necessarily show all nodes which will actually
    receive the message.

    OHKFIX Add wait for complete last known node set to mimic the old
    semantics.


    IMPORTANT: What xcom does and what it does not do:

    xcom messages are received in the same order on all nodes.

    xcom guarantees that if a message is delivered to one node, it will
    eventually be seen on all other nodes as well.

    xcom messages are available to a crashed node when it comes up
    again if at least one node which knows the value of the message
    has not crashed. The size of the message cache is configurable.

    OHKFIX Add logging to disk to make messages durable across system
    crash and to increase the number of messages which may be cached.

    There is no guarantee whatsoever about the order of messages from
    different nodes, not even the order of multiple messages from the
    same node. It is up to the client to impose such an order by
    waiting on a message before it sends the next.

    xcom can notify the client that a message has timed out, and in
    that case will try to cancel the message, but it cannot guarantee
    that a message which has timed out will not be delivered.

    xcom attaches a node set to each message as it is delivered to the
    client. This node set reflects the current node set that xcom
    believes is active, it does not mean that the message has been
    delivered yet to all nodes in the set. Neither does it mean that
    the message has not been delivered to the nodes not in the set.

    A cache of Paxos state machines is central to the new design. The
    purpose of the cache is both to store a window of messages, and to
    decouple the different parts of xcom, like message proposal,
    message delivery and execution, and recovery.  The old cache was
    limited to caching messages, and a single state machine ran the
    combined VP and Paxos algorithm. This constrained xcom to deliver
    only a single message at a time.

    Each instance of the Paxos state machine implements the basic
    Paxos protocol.  Unlike the cache in the old system, it is not
    cleared when a site is deleted.  This removes some problems
    related to message delivery during site deletion.  The cache is a
    classic fixed size LRU with a hash index.

    Some extensions to the basic Paxos algorithm has been implemented:

    A node has ownership to all synodes with its own node number. Only
    a node with node number N can propose a value for synode {X N},
    where X is the sequence number, and N is the node number. Other
    nodes can only propose the special value no_op for synode {X N}.
    The reason for this is to retain the leaderless Paxos algorithm,
    but to avoid collisions between nodes which are competing for the
    same synode number. With this scheme, each node has its own unique
    number series during normal operation. The scheme has the
    following implications:

    1. If a node N has not already proposed a value for the synode {X N},
    it may at any time send a LEARN message to the other nodes with
    the reserved value no_op, without going through phase 1 and 2 of
    Paxos. This is because the other nodes are constrained to propose
    no_op for this synode, so the final outcome will always be no_op.
    To avoid unnecessary message transmission, a node will try to
    broadcast the no_op LEARN messages by piggybacking the information
    on the messages of the basic Paxos protocol.

    2. Other nodes which want to find the value of synode {X N} may do
    so by trying to get the value no_op accepted by following the
    basic Paxos algorithm. The result will be the actual value
    proposed by node N if it has done so, otherwise no_op. This will
    typically only be necessary when a node is down, and the other
    nodes need to find the values from the missing node in order to be
    able to continue execution.

    Messages are delivered in order to the client, and the order is
    determined by the sequence number and the node number, with the
    sequence number as the most significant part.

    The xcom network interface has been redesigned and is now
    implemented directly on top of TCP, and has so far been completely
    trouble free. We use poll() or select() to implement non-blocking
    send and receive, but libev could equally well have been used.

    Multicast is implemented on top of unicast as before, but the
    implementation is prepared to use real multicast with relatively
    minor changes.

    The roles of proposer, acceptor/learner, and executor are now
    directly mapped to unique task types which interact with the Paxos
    state machines, whereas the previous implementation folded all the
    roles into a single event driven state machine.

    The following terminology will be used:

    A node is an instance of the xcom thread. There is only one instance
    of the xcom thread in the agent.
    A client is the application which is using xcom to send messages.
    A thread is a real OS thread.
    A task is a logical process. It is implemented by coroutines and
    an explicit stack.

    The implementation of tasks and non-blocking socket operations is
    isolated in task.h and task.c.

    A node will open a tcp connection to each of the other nodes. This
    connection is used for all communication initiated by the node,
    and replies to messages will arrive on the connection on which it
    was sent.

    static int tcp_server(task_arg);

    The tcp_server listens on the xcom port and starts an
    acceptor_learner_task whenever a new connection is detected.

    static int tcp_reaper_task(task_arg);

    Closes tcp connection which have been unused for too long.

    static int sender_task(task_arg);

    The sender_task waits for tcp messages on its input queue and
    sends it on the tcp socket. If the socket is closed for any
    reason, the sender_task will reconnect the socket. There is one
    sender_task for each socket. The sender task exists mainly to
    simplify the logic in the other tasks, but it could have been
    replaced with a coroutine which handles the connection logic after
    having reserved the socket for its client task.

    static int generator_task(task_arg);

    The generator_task reads messages from the client queue and moves
    them into the input queue of the proposer_task.

    OHKFIX Use a tcp socket instead of the client queue. We can then
    remove the generator_task and let the acceptor_learner_task do the
    dispatching.

    static int proposer_task(task_arg);

    Assign a message number to an outgoing message and try to get it
    accepted. There may be several proposer tasks on each node
    working in parallel. If there are multiple proposer tasks, xcom can
    not guarantee that the messages will be sent in the same order as
    received from the client.

    static int acceptor_learner_task(task_arg);

    This is the server part of the xcom thread. There is one
    acceptor_learner_task for each node in the system. The acceptor
    learner_task reads messages from the socket, finds the correct
    Paxos state machine, and dispatches to the correct message handler
    with the state machine and message as arguments.

    static int reply_handler_task(task_arg);

    The reply_handler_task does the same job as the
    acceptor_learner_task, but listens on the socket which the node
    uses to send messages, so it will handle only replies on that
    socket.

    static int executor_task(task_arg);

    The ececutor_task waits for a Paxos message to be accpeted. When
    the message is accepted, it is delivered to the client,
    unless it is a no-op. In either case, the executor_task steps to
    the next message and repeats the wait. If it times out waiting for
    a message, it will try to get a no-op accepted.

    static int alive_task(task_arg);

    Sends i-am-alive to other nodes if there has been no normal traffic
    for a while. It also pings nodes which seem to be inactive.

    static int detector_task(task_arg);

    The detector_task periodically scans the set of connections from
    other nodes and sees if there has been any activity. If there has
    been no activity for some time, it will assume that the node is
    dead, and send a view message to the client.


    Reconfiguration:

    The xcom reconfiguration process is essentially the one described in
    "Reconfiguring a State Machine" by Lamport et al. as the R-alpha
    algorithm.
    We execute the reconfiguration command immediately, but the config is
    only valid after a delay of alpha messages.
    The parameter alpha is the same as
    EVENT_HORIZON in this implementation. :/static.*too_far
    All tcp messages from beyond the event horizon will be ignored.

*/
#include "xcom/xcom_profile.h"

#ifndef XCOM_STANDALONE
#include "my_compiler.h"
#endif
#include "xcom/x_platform.h"

#ifndef _WIN32
#include <arpa/inet.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#ifndef __linux__
#include <sys/sockio.h>
#endif
#endif

#if defined(_WIN32)
#include <windows.h>
#endif

#include "xcom/app_data.h"
#include "xcom/get_synode_app_data.h"
#include "xcom/node_no.h"
#include "xcom/server_struct.h"
#include "xcom/simset.h"
#include "xcom/site_struct.h"
#include "xcom/task.h"
#include "xcom/task_net.h"
#include "xcom/task_os.h"
#include "xcom/xcom_base.h"
#include "xcom/xcom_common.h"
#include "xcom/xcom_detector.h"
#include "xcom/xcom_transport.h"
#include "xcom/xdr_utils.h"
#include "xdr_gen/xcom_vp.h"

#ifndef XCOM_WITHOUT_OPENSSL
#include "xcom/xcom_ssl_transport.h"
#endif

#include "xcom/bitset.h"
#include "xcom/node_list.h"
#include "xcom/node_set.h"
#include "xcom/pax_msg.h"
#include "xcom/site_def.h"
#include "xcom/sock_probe.h"
#include "xcom/synode_no.h"
#include "xcom/task_debug.h"
#include "xcom/task_net.h"
#include "xcom/xcom_cache.h"
#include "xcom/xcom_cfg.h"
#include "xcom/xcom_interface.h"
#include "xcom/xcom_memory.h"
#include "xcom/xcom_msg_queue.h"
#include "xcom/xcom_recover.h"
#include "xcom/xcom_statistics.h"
#include "xcom/xcom_vp_str.h"

#ifndef XCOM_WITHOUT_OPENSSL
#ifdef _WIN32
/* In OpenSSL before 1.1.0, we need this first. */
#include <winsock2.h>
#endif /* _WIN32 */

#include <openssl/ssl.h>

#endif

/* Defines and constants */

#define SYS_STRERROR_SIZE 512

/* Avoid printing the warning of protocol version mismatch too often */
#define PROTOVERSION_WARNING_TIMEOUT 600.0 /** Every 10 minutes */
static double protoversion_warning_time =
    0.0; /** Timestamp of previous protoversion warning */

/* Skip prepare for first ballot */
#ifdef ALWAYS_THREEPHASE
int const threephase = 1;
#else
int const threephase = 0;
#endif

#include "xcom/retry.h"

#ifdef NODE_0_IS_ARBITRATOR
int ARBITRATOR_HACK = 1;
#else
int ARBITRATOR_HACK = 0;
#endif

static int const no_duplicate_payload = 1;

/* Use buffered read when reading messages from the network */
static int use_buffered_read = 1;

/* Used to handle OOM errors */
static unsigned short oom_abort = 0;

/* Forward declarations */
long xcom_unique_long(void);

static double wakeup_delay_for_perf(double old);
static double wakeup_delay(const site_def *site, double old);
static void note_snapshot(node_no node);

/* Task types */
static int proposer_task(task_arg arg);
static int executor_task(task_arg arg);
static int sweeper_task(task_arg arg);
extern int alive_task(task_arg arg);
extern int cache_manager_task(task_arg arg);
extern int detector_task(task_arg arg);

static int finished(pax_machine *p);
static int accepted(pax_machine *p);
static int started(pax_machine *p);
static synode_no first_free_synode(synode_no msgno);
static void free_forced_config_site_def();
static void activate_sweeper();
static void force_pax_machine(pax_machine *p, int enforcer);
static void handle_need_snapshot(linkage *reply_queue, pax_msg *pm);
static void handle_skip(site_def const *site, pax_machine *p, pax_msg *m);

extern void bit_set_or(bit_set *x, bit_set const *y);

/* Global variables */

int xcom_shutdown = 0;  /* Xcom_Shutdown flag */
synode_no executed_msg; /* The message we are waiting to execute */
synode_no max_synode;   /* Max message number seen so far */
task_env *boot = NULL;
task_env *detector = NULL;
task_env *killer = NULL;
task_env *net_boot = NULL;
task_env *net_recover = NULL;
void *xcom_thread_input = 0;

long xcom_debug_mask =
    /* D_DETECT | */ D_FSM /* | D_FILEOP | D_CONS | D_BASE */ | D_TRANSPORT;
long xcom_dbg_stack[DBG_STACK_SIZE];
int xcom_dbg_stack_top = 0;

static void init_proposers();
void initialize_lsn(uint64_t n);

void init_base_vars() {
  xcom_shutdown = 0;          /* Xcom_Shutdown flag */
  executed_msg = null_synode; /* The message we are waiting to execute */
  max_synode = null_synode;   /* Max message number seen so far */
  boot = NULL;
  detector = NULL;
  killer = NULL;
  net_boot = NULL;
  net_recover = NULL;
  xcom_thread_input = 0;
}

static task_env *executor = NULL;
static task_env *sweeper = NULL;
static task_env *retry = NULL;
static task_env *proposer[PROPOSERS];
static task_env *alive_t = NULL;
static task_env *cache_task = NULL;

static uint32_t my_id = 0; /* Unique id of this instance */
uint32_t get_my_xcom_id() { return my_id; }
static synode_no current_message; /* Current message number */
static synode_no
    last_config_modification_id; /*Last configuration change proposal*/
static uint64_t lsn = 0;         /* Current log sequence number */

synode_no get_current_message() { return current_message; }

static channel prop_input_queue; /* Proposer task input queue */

extern int client_boot_done;
extern int netboot_ok;

static linkage exec_wait = {
    0, &exec_wait, &exec_wait}; /* Executor will wake up tasks sleeping here */

linkage detector_wait = {0, &detector_wait,
                         &detector_wait}; /* Detector sleeps here */

static struct {
  int n;
  unsigned long id[MAX_DEAD];
} dead_sites;

synode_no get_max_synode() { return max_synode; }

static bool_t is_latest_config(site_def const *const config) {
  site_def const *const latest_config = get_site_def();
  assert(latest_config != NULL);
  return config == latest_config;
}

/**
 * Get the first pending configuration that reconfigures the event horizon.
 *
 * Retrieve the first pending site_def, i.e. with the smallest start synod that
 * is greater than executed_msg, that reconfigures the event horizon.
 */
static site_def const *first_event_horizon_reconfig() {
  site_def const *active_config = find_site_def(executed_msg);
  xcom_event_horizon active_event_horizon = active_config->event_horizon;
  site_def const *first_event_horizon_reconfig = NULL;
  site_def const *next_config = NULL;
  for (next_config = find_next_site_def(active_config->start);
       next_config != NULL && first_event_horizon_reconfig == NULL;
       next_config = find_next_site_def(next_config->start)) {
    if (active_event_horizon != next_config->event_horizon) {
      first_event_horizon_reconfig = next_config;
    }
  }
  return first_event_horizon_reconfig;
}

/**
 * Get the latest pending configuration that reconfigures the event horizon.
 *
 * Retrieve the last pending site_def, i.e. with the greatest start synod that
 * is greater than executed_msg, that reconfigures the event horizon.
 */
static site_def const *latest_event_horizon_reconfig() {
  site_def const *active_config = find_site_def(executed_msg);
  xcom_event_horizon previous_event_horizon = active_config->event_horizon;
  site_def const *last_event_horizon_reconfig = NULL;
  site_def const *next_config = NULL;
  for (next_config = find_next_site_def(active_config->start);
       next_config != NULL;
       next_config = find_next_site_def(next_config->start)) {
    if (previous_event_horizon != next_config->event_horizon) {
      previous_event_horizon = next_config->event_horizon;
      last_event_horizon_reconfig = next_config;
    }
  }
  return last_event_horizon_reconfig;
}

/**
 * Add the event horizon to the given base synod s.
 *
 * We are assuming right now that this function is used solely in the context of
 * "we have received a reconfiguration command at synod s, when should it be
 * scheduled to take effect?"
 * The result of this function is *when* it should take effect.
 *
 * Common case: there are no configurations pending, or if there are, none of
 * them reconfigure the event horizon. The common case result is:
 *
 *   s + event_horizon(active_config) + 1
 *
 *
 * If an event horizon reconfiguration R is pending, it means that the command C
 * proposed for synod s is concurrent with R, i.e., s falls in the interval
 * ]proposed(R), start(R)[.
 *
 * In this situation we apply the command C proposed for synod s *after* taking
 * into account R's event horizon.
 *
 * This means that the result is:
 *
 *   start(R) + event_horizon(R) + 1
 */
/* purecov: begin deadcode */
static synode_no add_default_event_horizon(synode_no s) {
  s.msgno += EVENT_HORIZON_MIN + 1;
  return s;
}
/* purecov: end */

static synode_no add_event_horizon(synode_no s) {
  site_def const *active_config = find_site_def(executed_msg);
  if (active_config) {
    site_def const *pending_config = latest_event_horizon_reconfig();
    bool_t const no_event_horizon_reconfig_pending = (pending_config == NULL);
    if (is_latest_config(active_config) || no_event_horizon_reconfig_pending) {
      s.msgno = s.msgno + active_config->event_horizon + 1;
    } else {
      s.msgno = pending_config->start.msgno + pending_config->event_horizon + 1;
    }
    return s;
  } else { /* This is initial boot or recovery, we have no config */
#ifdef PERMISSIVE_EH_ACTIVE_CONFIG
    return add_default_event_horizon(s);
#else
    /* We should always have an active config */
    /* purecov: begin deadcode */
    assert(active_config != NULL);
    return null_synode;
    /* purecov: end */
#endif
  }
}

/**
   Set node group
*/
void set_group(uint32_t id) {
  IFDBG(D_NONE, FN; STRLIT("changing group id of global variables ");
        NDBG((unsigned long)id, lu););
  /*	set_group_id(id); */
  current_message.group_id = id;
  executed_msg.group_id = id;
  max_synode.group_id = id;
}

static void bury_site(uint32_t id) {
  if (id != 0) {
    dead_sites.id[dead_sites.n % MAX_DEAD] = id;
    dead_sites.n = (dead_sites.n + 1) % MAX_DEAD;
  }
}

static bool_t is_dead_site(uint32_t id) {
  int i = 0;
  for (i = 0; i < MAX_DEAD; i++) {
    if (dead_sites.id[i] == id)
      return TRUE;
    else if (dead_sites.id[i] == 0)
      return FALSE;
  }
  return FALSE;
}

extern node_set *init_node_set(node_set *set, u_int n);
extern node_set *alloc_node_set(node_set *set, u_int n);

#if 0
/* Find our previous message number. */
static synode_no decr_msgno(synode_no msgno)
{
	synode_no ret = msgno;
	ret.msgno--;
	ret.node = get_nodeno(find_site_def(ret)); /* In case site and node number has changed */
	return ret;
}
#endif

/* Find our next message number. */
static synode_no incr_msgno(synode_no msgno) {
  synode_no ret = msgno;
  ret.msgno++;
  ret.node = get_nodeno(
      find_site_def(ret)); /* In case site and node number has changed */
  return ret;
}

synode_no incr_synode(synode_no synode) {
  synode_no ret = synode;
  ret.node++;
  if (ret.node >= get_maxnodes(find_site_def(synode))) {
    ret.node = 0;
    ret.msgno++;
  }
  /* 	IFDBG(D_NONE, FN; SYCEXP(synode); SYCEXP(ret)); */
  return ret; /* Change this if we change message number type */
}

synode_no decr_synode(synode_no synode) {
  synode_no ret = synode;
  if (ret.node == 0) {
    ret.msgno--;
    ret.node = get_maxnodes(find_site_def(ret));
  }
  ret.node--;
  return ret; /* Change this if we change message number type */
}

static void skip_value(pax_msg *p) {
  IFDBG(D_NONE, FN; SYCEXP(p->synode));
  p->op = learn_op;
  p->msg_type = no_op;
}

/* Utilities and debug */

#ifndef _WIN32
/* Ignore this signal */
static int ignoresig(int signum) {
  struct sigaction act;
  struct sigaction oldact;

  memset(&act, 0, sizeof(act));
  act.sa_handler = SIG_IGN;
  memset(&oldact, 0, sizeof(oldact));

  return sigaction(signum, &act, &oldact);
}
#else
#define SIGPIPE 0
static int ignoresig(int signum) { return 0; }
#endif

static int recently_active(pax_machine *p) {
  IFDBG(D_NONE, FN; SYCEXP(p->synode); STRLIT(" op "); PTREXP(p);
        STRLIT(p->learner.msg ? pax_op_to_str(p->learner.msg->op) : "NULL");
        NDBG(p->last_modified, f); NDBG(task_now(), f));
  return p->last_modified != 0.0 &&
         (p->last_modified + BUILD_TIMEOUT + median_time()) > task_now();
}

static inline int finished(pax_machine *p) {
  IFDBG(D_NONE, FN; SYCEXP(p->synode); STRLIT(" op "); PTREXP(p);
        STRLIT(p->learner.msg ? pax_op_to_str(p->learner.msg->op) : "NULL"););
  return p->learner.msg && (p->learner.msg->op == learn_op ||
                            p->learner.msg->op == tiny_learn_op);
}

int pm_finished(pax_machine *p) { return finished(p); }

static inline int accepted(pax_machine *p) {
  IFDBG(D_NONE, FN; SYCEXP(p->synode); STRLIT(" op "); PTREXP(p);
        STRLIT(p->acceptor.msg ? pax_op_to_str(p->acceptor.msg->op) : "NULL"););
  return p->acceptor.msg && p->acceptor.msg->op != initial_op;
}

static inline int accepted_noop(pax_machine *p) {
  IFDBG(D_NONE, FN; SYCEXP(p->synode); STRLIT(" op "); PTREXP(p);
        STRLIT(p->acceptor.msg ? pax_op_to_str(p->acceptor.msg->op) : "NULL"););
  return accepted(p) && p->acceptor.msg->msg_type == no_op;
}

static inline int noop_match(pax_machine *p, pax_msg *pm) {
  return pm->msg_type == no_op && accepted_noop(p);
}

static inline int started(pax_machine *p) {
  return p->op != initial_op || (p->acceptor.promise.cnt > 0) ||
         (p->proposer.msg && (p->proposer.msg->op != initial_op)) ||
         accepted(p) || finished(p);
}

void set_last_received_config(synode_no received_config_change) {
  last_config_modification_id = received_config_change;
}

/* Definition of majority */
static inline node_no max_check(site_def const *site) {
#ifdef MAXACCEPT
  return MIN(get_maxnodes(site), MAXACCEPT);
#else
  return get_maxnodes(site);
#endif
}

static site_def *forced_config = 0;
static int is_forcing_node(pax_machine const *p) { return p->enforcer; }
static int wait_forced_config = 0;

#define MAX_ZONE_NUM 16

/* Definition of majority */
static inline int majority(bit_set const *nodeset, site_def const *s, int all,
                           int delay MY_ATTRIBUTE((unused)), int force) {
  node_no ok = 0;
  node_no i = 0;
  int retval = 0;
  int silent = DEFAULT_DETECTOR_LIVE_TIMEOUT;
  double sec = task_now();
  node_no max = max_check(s);
  int zone_hit[MAX_ZONE_NUM];
  int zone_ack_hit[MAX_ZONE_NUM];

  for (i = 0; i < MAX_ZONE_NUM; i++) {
    zone_hit[i] = 0;
    zone_ack_hit[i] = 0;
  }

  if (the_app_xcom_cfg) {
    silent = the_app_xcom_cfg->m_flp_timeout;
  }

  /* IFDBG(D_NONE, FN; NDBG(max,lu); NDBG(all,d); NDBG(delay,d); NDBG(force,d));
   */

  /* Count nodes that has answered */
  for (i = 0; i < max; i++) {
    int zone_id = s->servers[i]->zone_id;
    bool zone_sync_mode = s->servers[i]->zone_id_sync_mode;
    if (zone_sync_mode) zone_hit[zone_id]++;
    if (BIT_ISSET(i, nodeset)) {
      ok++;
      if (zone_sync_mode) zone_ack_hit[zone_id]++;
    }
#ifdef WAIT_FOR_ALL_FIRST
    else {
      if (all) return 0; /* Delay until all nodes have answered */
      if (delay && !may_be_dead(s->detected, i, sec, silent,
                                s->servers[i]->unreachable)) {
        return 0; /* Delay until all live nodes have answered */
      }
    }
#endif
  }

  /* If we are forcing messages, attempt to ensure consistency by
     requiring all remaining nodes to agree. Forced_config points to
     the config that should be used as acceptors in this
     case. Another possibility is to use the original config and
     count the number of live nodes, but since the force flag is
     being used only to force a new config, it seems safer to use
     the new config and no time-dependent info. Note that we are
     counting the answers based on the normal config, but use the
     number of nodes from forced_config. This is safe, since we can
     assume that the nodes that are not in forced_config will never
     answer. */

  if (force) {
    IFDBG(D_NONE, FN; STRLIT("force majority"); NDBG(ok, u); NDBG(max, u);
          NDBG(get_maxnodes(forced_config), u));
    return ok == get_maxnodes(forced_config);
  } else {
/* Have now seen answer from all live nodes */
#ifdef NODE_0_IS_ARBITRATOR
    retval = all ? ok == max
                 : ok > max / 2 ||
                       (ARBITRATOR_HACK && (get_nodeno(s) == 0) && (2 == max));
#else
    retval = all ? ok == max : ok > max / 2 || (ARBITRATOR_HACK && (2 == max));
#endif
    /* 	IFDBG(D_NONE, FN; NDBG(max,lu); NDBG(all,d); NDBG(delay,d);
     * NDBG(retval,d)); */

    if (retval) {
      int zone_num = 0, zone_ack_num = 0;
      for (i = 0; i < max; i++) {
        if (may_be_dead(s->detected, i, sec, silent,
                        s->servers[i]->unreachable)) {
          continue;
        }
        if (zone_hit[s->servers[i]->zone_id]) {
          zone_num++;
        }
        if (zone_ack_hit[s->servers[i]->zone_id]) {
          zone_ack_num++;
        }
      }

      if (zone_ack_num != zone_num) {
        return 0;
      }
    }

    return retval;
  }
}

#define IS_CONS_ALL(p) \
  ((p)->proposer.msg->a ? (p)->proposer.msg->a->consensus == cons_all : 0)

/* See if a majority of acceptors have answered our prepare */
static int prep_majority(site_def const *site, pax_machine *p) {
  int ok = 0;

  assert(p);
  assert(p->proposer.prep_nodeset);
  assert(p->proposer.msg);
  /* IFDBG(D_NONE, FN; BALCEXP(p->proposer.bal)); */
  ok = majority(p->proposer.prep_nodeset, site, IS_CONS_ALL(p),
                p->proposer.bal.cnt == 1,
                p->proposer.msg->force_delivery || p->force_delivery);
  return ok;
}

/* See if a majority of acceptors have answered our propose */
static int prop_majority(site_def const *site, pax_machine *p) {
  int ok = 0;

  assert(p);
  assert(p->proposer.prop_nodeset);
  assert(p->proposer.msg);
  /* IFDBG(D_NONE, FN; BALCEXP(p->proposer.bal)); */
  ok = majority(p->proposer.prop_nodeset, site, IS_CONS_ALL(p),
                p->proposer.bal.cnt == 1,
                p->proposer.msg->force_delivery || p->force_delivery);
  return ok;
}

/* Xcom thread */

static site_def *executor_site = 0;

site_def const *get_executor_site() { return executor_site; }
site_def *get_executor_site_rw() { return executor_site; }

static site_def *proposer_site = 0;

site_def const *get_proposer_site() { return proposer_site; }

/* delivered_msg may point to a no_op message, which will not actually be
 * delivered */
static synode_no delivered_msg = NULL_SYNODE;

synode_no get_delivered_msg() { return delivered_msg; }

/* last_delivered_msg is the last synode we actually delivered */
static synode_no last_delivered_msg = NULL_SYNODE;
synode_no get_last_delivered_msg() { return last_delivered_msg; }

void init_xcom_base() {
  IFDBG(D_NONE, FN);
  xcom_shutdown = 0;
  current_message = null_synode;
  executed_msg = null_synode;
  delivered_msg = null_synode;
  last_delivered_msg = null_synode;
  max_synode = null_synode;
  client_boot_done = 0;
  netboot_ok = 0;

  xcom_recover_init();
  my_id = new_id();
  push_site_def(NULL);
  /*	update_servers(NULL); */
  xcom_cache_var_init();
  median_filter_init();
  link_init(&exec_wait, TYPE_HASH("task_env"));
  link_init(&detector_wait, TYPE_HASH("task_env"));
  link_init(&connect_wait, TYPE_HASH("task_env"));
  executor_site = 0;
  proposer_site = 0;

  /** Reset lsn */
  initialize_lsn(0);
  IFDBG(D_NONE, FN);
}

static void init_tasks() {
  IFDBG(D_NONE, FN);
  set_task(&boot, NULL);
  set_task(&net_boot, NULL);
  set_task(&net_recover, NULL);
  set_task(&killer, NULL);
  set_task(&executor, NULL);
  set_task(&retry, NULL);
  set_task(&detector, NULL);
  init_proposers();
  set_task(&alive_t, NULL);
  set_task(&sweeper, NULL);
  set_task(&cache_task, NULL);
  IFDBG(D_NONE, FN);
}

/* Initialize the xcom thread */
void xcom_thread_init() {
#ifndef NO_SIGPIPE
  signal(SIGPIPE, SIG_IGN);
#endif
  init_base_vars();
  init_site_vars();
  init_crc32c();
  xcom_srand48((long int)task_now());

  init_xcom_base();
  init_tasks();

  /* Initialize input queue */
  channel_init(&prop_input_queue, TYPE_HASH("msg_link"));
  init_link_list();
  task_sys_init();

  init_cache();
}

/* Empty the proposer input queue */
static void empty_prop_input_queue() {
  empty_msg_channel(&prop_input_queue);
  IFDBG(D_NONE, FN; STRLIT("prop_input_queue empty"));
}

/* De-initialize the xcom thread */
void xcom_thread_deinit() {
  IFDBG(D_BUG, FN; STRLIT("Empty proposer input queue"));
  empty_prop_input_queue();
  IFDBG(D_BUG, FN; STRLIT("Empty link free list"));
  empty_link_free_list();
  IFDBG(D_BUG, FN; STRLIT("De-initialize cache"));
  deinit_cache();
  garbage_collect_servers();
  IFDBG(D_BUG, FN; STRLIT("De-initialize network cache"));
  deinit_network_cache();
  IFDBG(D_BUG, FN; STRLIT("De-initialize xcom_interface"));
  deinit_xcom_interface();
}

#define PROP_ITER \
  int i;          \
  for (i = 0; i < PROPOSERS; i++)

static void init_proposers() {
  PROP_ITER { set_task(&proposer[i], NULL); }
}

static void create_proposers() {
  PROP_ITER {
    set_task(&proposer[i], task_new(proposer_task, int_arg(i), "proposer_task",
                                    XCOM_THREAD_DEBUG));
  }
}

static void terminate_proposers() {
  PROP_ITER { task_terminate(proposer[i]); }
}

static void free_forced_config_site_def() {
  free_site_def(forced_config);
  forced_config = NULL;
}

#if TASK_DBUG_ON
static void dbg_proposers() MY_ATTRIBUTE((unused));
static void dbg_proposers() {
  GET_GOUT;
  if (!IS_XCOM_DEBUG_WITH(XCOM_DEBUG_TRACE)) return;
  NDBG(PROPOSERS, d);
  {
    PROP_ITER { PPUT(proposer[i]); }
  }
  PRINT_GOUT;
  FREE_GOUT;
}
#endif

static void set_proposer_startpoint() {
  IFDBG(D_NONE, FN; STRLIT("changing current message"));
  if (synode_gt(max_synode, get_current_message())) {
    if (max_synode.msgno <= 1)
      set_current_message(first_free_synode(max_synode));
    else
      set_current_message(incr_msgno(first_free_synode(max_synode)));
  }
  if (synode_gt(executed_msg, get_current_message())) {
    set_current_message(first_free_synode(executed_msg));
  }
}

/* Task functions */

static xcom_state_change_cb xcom_run_cb = 0;
static xcom_state_change_cb xcom_terminate_cb = 0;
static xcom_state_change_cb xcom_comms_cb = 0;
static xcom_state_change_cb xcom_exit_cb = 0;
static xcom_state_change_cb xcom_expel_cb = 0;
static xcom_input_try_pop_cb xcom_try_pop_from_input_cb = NULL;

void set_xcom_run_cb(xcom_state_change_cb x) { xcom_run_cb = x; }

void set_xcom_comms_cb(xcom_state_change_cb x) { xcom_comms_cb = x; }
/* purecov: begin deadcode */
void set_xcom_terminate_cb(xcom_state_change_cb x) { xcom_terminate_cb = x; }
/* purecov: end */
void set_xcom_exit_cb(xcom_state_change_cb x) { xcom_exit_cb = x; }

static xcom_recovery_cb recovery_begin_cb = NULL;
/* purecov: begin deadcode */
void set_xcom_recovery_begin_cb(xcom_recovery_cb x) { recovery_begin_cb = x; }
/* purecov: end */

static xcom_recovery_cb recovery_restart_cb = NULL;
/* purecov: begin deadcode */
void set_xcom_recovery_restart_cb(xcom_recovery_cb x) {
  recovery_restart_cb = x;
}
/* purecov: end */

static xcom_recovery_cb recovery_init_cb = NULL;
/* purecov: begin deadcode */
void set_xcom_recovery_init_cb(xcom_recovery_cb x) { recovery_init_cb = x; }
/* purecov: end */

static xcom_recovery_cb recovery_end_cb = NULL;
/* purecov: begin deadcode */
void set_xcom_recovery_end_cb(xcom_recovery_cb x) { recovery_end_cb = x; }
/* purecov: end */

void set_xcom_expel_cb(xcom_state_change_cb x) { xcom_expel_cb = x; }

void set_xcom_input_try_pop_cb(xcom_input_try_pop_cb pop) {
  xcom_try_pop_from_input_cb = pop;
}

static connection_descriptor *input_signal_connection = NULL;

#ifndef XCOM_WITHOUT_OPENSSL
static bool_t xcom_input_signal_connection_shutdown_ssl_wait_for_peer() {
  int ssl_error_code = 0;
  do {
    char buf[1024];
    ssl_error_code = SSL_read(input_signal_connection->ssl_fd, buf, 1024);
  } while (ssl_error_code > 0);

  bool_t const successful =
      (SSL_get_error(input_signal_connection->ssl_fd, ssl_error_code) ==
       SSL_ERROR_ZERO_RETURN);
  return successful;
}
static bool_t xcom_input_signal_connection_shutdown_ssl() {
  bool_t successful = FALSE;

  int ssl_error_code = SSL_shutdown(input_signal_connection->ssl_fd);

  bool_t const need_to_wait_for_peer_shutdown = (ssl_error_code == 0);
  bool_t const something_went_wrong = (ssl_error_code < 0);
  if (need_to_wait_for_peer_shutdown) {
    successful = xcom_input_signal_connection_shutdown_ssl_wait_for_peer();
    if (!successful) goto end;
  } else if (something_went_wrong) {
    goto end;
  }

  ssl_free_con(input_signal_connection);
  successful = TRUE;

end:
  return successful;
}
#endif

bool_t xcom_input_new_signal_connection(char const *address, xcom_port port) {
  bool_t const SUCCESSFUL = TRUE;
  bool_t const UNSUCCESSFUL = FALSE;
  assert(input_signal_connection == NULL);

  /* Try to connect. */
  input_signal_connection = xcom_open_client_connection(address, port);
  if (input_signal_connection == NULL) return UNSUCCESSFUL;

  /* Have the server handle the rest of this connection using a local_server
     task. */
  if (xcom_client_convert_into_local_server(input_signal_connection) == 1) {
    G_TRACE(
        "Converted the signalling connection handler into a local_server "
        "task on the client side.");
#ifndef XCOM_WITHOUT_OPENSSL
    /* No more SSL in this connection. */
    {
      bool_t const using_ssl = (input_signal_connection->ssl_fd != NULL);
      if (using_ssl) {
        bool_t successful = xcom_input_signal_connection_shutdown_ssl();
        if (!successful) {
          G_ERROR(
              "Error shutting down SSL on XCom's signalling connection on the "
              "client side.");
          xcom_input_free_signal_connection();
          return UNSUCCESSFUL;
        }
      }
    }
#endif
    return SUCCESSFUL;
  } else {
    G_DEBUG(
        "Error converting the signalling connection handler into a "
        "local_server task on the client side.");
    xcom_input_free_signal_connection();
    return UNSUCCESSFUL;
  }
}
static int64_t socket_write(connection_descriptor *wfd, void *_buf, uint32_t n);
bool_t xcom_input_signal() {
  bool_t successful = FALSE;
  if (input_signal_connection != NULL) {
    unsigned char tiny_buf[1] = {0};
    int64_t error_code = socket_write(input_signal_connection, tiny_buf, 1);
    successful = (error_code == 1);
  }
  return successful;
}
void xcom_input_free_signal_connection() {
  if (input_signal_connection != NULL) {
    xcom_close_client_connection(input_signal_connection);
    input_signal_connection = NULL;
  }
}

#ifndef XCOM_WITHOUT_OPENSSL
static int local_server_shutdown_ssl(connection_descriptor *con, void *buf,
                                     int n, int *ret) {
  DECL_ENV
  int ssl_error_code;
  bool_t need_to_wait_for_peer_shutdown;
  bool_t something_went_wrong;
  int64_t nr_read;
  END_ENV;
  *ret = 0;
  TASK_BEGIN
  ep->ssl_error_code = SSL_shutdown(con->ssl_fd);
  ep->need_to_wait_for_peer_shutdown = (ep->ssl_error_code == 0);
  ep->something_went_wrong = (ep->ssl_error_code < 0);
  if (ep->need_to_wait_for_peer_shutdown) {
    do {
      TASK_CALL(task_read(con, buf, n, &ep->nr_read));
    } while (ep->nr_read > 0);
    ep->ssl_error_code = SSL_get_error(con->ssl_fd, ep->nr_read);
    ep->something_went_wrong = (ep->ssl_error_code != SSL_ERROR_ZERO_RETURN);
  }
  if (ep->something_went_wrong) TERMINATE;
  ssl_free_con(con);
  *ret = 1;
  FINALLY
  TASK_END;
}
#endif

int local_server(task_arg arg) {
  DECL_ENV
  connection_descriptor rfd;
  int ssl_shutdown_ret;
  unsigned char buf[1024]; /* arbitrary size */
  int64_t nr_read;
  xcom_input_request_ptr request;
  xcom_input_request_ptr next_request;
  pax_msg *request_pax_msg;
  pax_msg *reply_payload;
  linkage internal_reply_queue;
  msg_link *internal_reply;
  END_ENV;
  TASK_BEGIN
  assert(xcom_try_pop_from_input_cb != NULL);
  {
    connection_descriptor *arg_rfd = (connection_descriptor *)get_void_arg(arg);
    ep->rfd = *arg_rfd;
    free(arg_rfd);
  }
  ep->ssl_shutdown_ret = 0;
  memset(ep->buf, 0, 1024);
  ep->nr_read = 0;
  ep->request = NULL;
  ep->next_request = NULL;
  ep->request_pax_msg = NULL;
  ep->reply_payload = NULL;
  link_init(&ep->internal_reply_queue, TYPE_HASH("msg_link"));
  ep->internal_reply = NULL;

#ifndef XCOM_WITHOUT_OPENSSL
  /* No more SSL in this connection. */
  if (ep->rfd.ssl_fd) {
    TASK_CALL(local_server_shutdown_ssl(&ep->rfd, ep->buf, 1024,
                                        &ep->ssl_shutdown_ret));
    if (ep->ssl_shutdown_ret != 1) {
      G_ERROR(
          "Error shutting down SSL on XCom's signalling connection on the "
          "server side.");
      TERMINATE;
    }
  }
#endif

  while (!xcom_shutdown) {
    /* Wait for signal that there is work to consume from the queue. */
    TASK_CALL(task_read(&ep->rfd, ep->buf, 1024, &ep->nr_read));
    if (ep->nr_read == 0) {
      /* purecov: begin inspected */
      G_WARNING("local_server: client closed the signalling connection?");
      break;
      /* purecov: end */
    } else if (ep->nr_read < 0) {
      /* purecov: begin inspected */
      IFDBG(D_NONE, FN; NDBG64(ep->nr_read));
      G_WARNING("local_server: error reading from the signalling connection?");
      break;
      /* purecov: end */
    }
    /* Pop, dispatch, and reply. */
    ep->request = xcom_try_pop_from_input_cb();
    while (ep->request != NULL) {
      /* Take ownership of the tail of the list, otherwise we lose it when we
         free ep->request. */
      ep->next_request = xcom_input_request_extract_next(ep->request);
      unchecked_replace_pax_msg(&ep->request_pax_msg,
                                pax_msg_new_0(null_synode));
      assert(ep->request_pax_msg->refcnt == 1);
      ep->request_pax_msg->op = client_msg;
      /* Take ownership of the request's app_data, otherwise the app_data is
         freed with ep->request. */
      ep->request_pax_msg->a = xcom_input_request_extract_app_data(ep->request);
      ep->request_pax_msg->to = VOID_NODE_NO;
      ep->request_pax_msg->force_delivery =
          (ep->request_pax_msg->a->body.c_t == force_config_type);
      dispatch_op(NULL, ep->request_pax_msg, &ep->internal_reply_queue);
      if (!link_empty(&ep->internal_reply_queue)) {
        ep->internal_reply =
            (msg_link *)(link_extract_first(&ep->internal_reply_queue));
        assert(ep->internal_reply->p);
        assert(ep->internal_reply->p->refcnt == 1);
        /* We are going to take ownership of the pax_msg which has the reply
           payload, so we bump its reference count so that it is not freed by
           msg_link_delete. */
        ep->reply_payload = ep->internal_reply->p;
        ep->reply_payload->refcnt++;
        msg_link_delete(&ep->internal_reply);
        /* There should only have been one reply. */
        assert(link_empty(&ep->internal_reply_queue));
      } else {
        ep->reply_payload = NULL;
      }
      /* Reply to the request. */
      xcom_input_request_reply(ep->request, ep->reply_payload);
      xcom_input_request_free(ep->request);
      ep->request = ep->next_request;
    }
  }
  FINALLY
  IFDBG(D_BUG, FN; STRLIT(" shutdown "); NDBG(ep->rfd.fd, d);
        NDBG(task_now(), f));
  /* Close the signalling connection. */
  shutdown_connection(&ep->rfd);
  unchecked_replace_pax_msg(&ep->request_pax_msg, NULL);
  IFDBG(D_NONE, FN; NDBG(xcom_shutdown, d));
  TASK_END;
}

static bool_t local_server_is_setup() {
  return xcom_try_pop_from_input_cb != NULL;
}

int xcom_taskmain2(xcom_port listen_port) {
  init_xcom_transport(listen_port);

  IFDBG(D_BUG, FN; STRLIT("enter taskmain"));
  ignoresig(SIGPIPE);

  {
    /* Setup tcp_server socket */
    result tcp_fd = {0, 0};

    if ((tcp_fd = announce_tcp(listen_port)).val < 0) {
      /* purecov: begin inspected */
      IFDBG(D_BUG, FN; STRLIT("cannot annonunce tcp "); NDBG(listen_port, d));
      task_dump_err(tcp_fd.funerr);
      g_critical("Unable to announce tcp port %d. Port already in use?",
                 listen_port);
      if (xcom_comms_cb) {
        xcom_comms_cb(XCOM_COMMS_ERROR);
      }
      if (xcom_terminate_cb) {
        xcom_terminate_cb(0);
      }
      goto cleanup;
      /* purecov: end */
    }

    if (xcom_comms_cb) {
      xcom_comms_cb(XCOM_COMMS_OK);
    }

    IFDBG(D_NONE, FN; STRLIT("Creating tasks"));
    /* task_new(generator_task, null_arg, "generator_task", XCOM_THREAD_DEBUG);
     */
    task_new(tcp_server, int_arg(tcp_fd.val), "tcp_server", XCOM_THREAD_DEBUG);
    task_new(tcp_reaper_task, null_arg, "tcp_reaper_task", XCOM_THREAD_DEBUG);
    IFDBG(D_BUG, FN; STRLIT("XCOM is listening on "); NPUT(listen_port, d));
  }

  if (recovery_init_cb) recovery_init_cb();

  if (recovery_begin_cb) recovery_begin_cb();

  task_loop();

cleanup:

#ifdef TASK_EVENT_TRACE
  dump_task_events();
#endif
#ifndef XCOM_WITHOUT_OPENSSL
  xcom_cleanup_ssl();
#endif

  xcom_thread_deinit();
  if (xcom_exit_cb) {
    xcom_exit_cb(0);
  }
  IFDBG(D_BUG, FN; STRLIT(" exit "); NDBG(xcom_dbg_stack_top, d);
        NDBG((unsigned)xcom_debug_mask, x));
  xcom_debug_mask = 0;
  xcom_dbg_stack_top = 0;
  return 1;
}

/* Paxos message construction and sending */

/* Initialize a message for sending */
static void prepare(pax_msg *p, pax_op op) {
  p->op = op;
  p->reply_to = p->proposal;
}

/* Initialize a prepare_msg */
void init_prepare_msg(pax_msg *p) { prepare(p, prepare_op); }

static int prepare_msg(pax_msg *p) {
  init_prepare_msg(p);
  /* p->msg_type = normal; */
  return send_to_acceptors(p, "prepare_msg");
}

/* Initialize a noop_msg */
pax_msg *create_noop(pax_msg *p) {
  init_prepare_msg(p);
  p->msg_type = no_op;
  return p;
}

/* Initialize a read_msg */
static pax_msg *create_read(site_def const *site, pax_msg *p) {
  p->msg_type = normal;
  p->proposal.node = get_nodeno(site);
  prepare(p, read_op);
  return p;
}

static int skip_msg(pax_msg *p) {
  prepare(p, skip_op);
  IFDBG(D_NONE, FN; STRLIT("skipping message "); SYCEXP(p->synode));
  p->msg_type = no_op;
  return send_to_all(p, "skip_msg");
}

static void brand_app_data(pax_msg *p) {
  if (p->a) {
    p->a->app_key.msgno = p->synode.msgno;
    p->a->app_key.node = p->synode.node;
    p->a->app_key.group_id = p->a->group_id = p->synode.group_id;
  }
}

static synode_no my_unique_id(synode_no synode) {
  assert(my_id != 0);
  /* Random number derived from node number and timestamp which uniquely defines
   * this instance */
  synode.group_id = my_id;
  return synode;
}

static void set_unique_id(pax_msg *msg, synode_no synode) {
  app_data_ptr a = msg->a;
  while (a) {
    a->unique_id = synode;
    a = a->next;
  }
}

void init_propose_msg(pax_msg *p) {
  p->op = accept_op;
  p->reply_to = p->proposal;
  brand_app_data(p);
  /* set_unique_id(p, my_unique_id(synode)); */
}

static int send_propose_msg(pax_msg *p) {
  return send_to_acceptors(p, "propose_msg");
}

static int propose_msg(pax_msg *p) {
  init_propose_msg(p);
  return send_propose_msg(p);
}

static void set_learn_type(pax_msg *p) {
  p->op = learn_op;
  p->msg_type = p->a ? normal : no_op;
}

/* purecov: begin deadcode */
static void init_learn_msg(pax_msg *p) {
  set_learn_type(p);
  p->reply_to = p->proposal;
  brand_app_data(p);
}

static int send_learn_msg(site_def const *site, pax_msg *p) {
  IFDBG(D_NONE, FN; dbg_bitset(p->receivers, get_maxnodes(site)););
  return send_to_all_site(site, p, "learn_msg");
}
/* purecov: end */

static pax_msg *create_tiny_learn_msg(pax_machine *pm, pax_msg *p) {
  pax_msg *tiny_learn_msg = clone_pax_msg_no_app(p);

  ref_msg(tiny_learn_msg);
  tiny_learn_msg->msg_type = p->a ? normal : no_op;
  tiny_learn_msg->op = tiny_learn_op;
  tiny_learn_msg->reply_to = pm->proposer.bal;
  brand_app_data(tiny_learn_msg);

  return tiny_learn_msg;
}

static int send_tiny_learn_msg(site_def const *site, pax_msg *p) {
  int retval = send_to_all_site(site, p, "tiny_learn_msg");
  unref_msg(&p);
  return retval;
}

/* Proposer task */

void prepare_push_3p(site_def const *site, pax_machine *p, pax_msg *msg,
                     synode_no msgno, pax_msg_type msg_type) {
  IFDBG(D_NONE, FN; SYCEXP(msgno); NDBG(p->proposer.bal.cnt, d);
        NDBG(p->acceptor.promise.cnt, d));
  BIT_ZERO(p->proposer.prep_nodeset);
  p->proposer.bal.node = get_nodeno(site);
  {
    int maxcnt = MAX(p->proposer.bal.cnt, p->acceptor.promise.cnt);
    p->proposer.bal.cnt = ++maxcnt;
  }
  msg->synode = msgno;
  msg->proposal = p->proposer.bal;
  msg->msg_type = msg_type;
  msg->force_delivery = p->force_delivery;
}

void prepare_push_2p(site_def const *site, pax_machine *p) {
  assert(p->proposer.msg);

  BIT_ZERO(p->proposer.prop_nodeset);
  IFDBG(D_NONE, FN; SYCEXP(p->synode));
  p->proposer.bal.cnt = 0;
  p->proposer.bal.node = get_nodeno(site);
  p->proposer.msg->proposal = p->proposer.bal;
  p->proposer.msg->synode = p->synode;
  p->proposer.msg->force_delivery = p->force_delivery;
}

static void push_msg_2p(site_def const *site, pax_machine *p) {
  prepare_push_2p(site, p);
  propose_msg(p->proposer.msg);
}

static void push_msg_3p(site_def const *site, pax_machine *p, pax_msg *msg,
                        synode_no msgno, pax_msg_type msg_type) {
  if (wait_forced_config) {
    force_pax_machine(p, 1);
  }

  assert(msgno.msgno != 0);
  prepare_push_3p(site, p, msg, msgno, msg_type);
  assert(p->proposer.msg);
  prepare_msg(msg);
  IFDBG(D_NONE, FN; BALCEXP(msg->proposal); SYCEXP(msgno); STRLIT(" op ");
        STRLIT(pax_op_to_str(msg->op)));
}

/* Brand client message with unique ID */
static void brand_client_msg(pax_msg *msg, synode_no msgno) {
  assert(!synode_eq(msgno, null_synode));
  set_unique_id(msg, my_unique_id(msgno));
}

void xcom_send(app_data_ptr a, pax_msg *msg) {
  IFDBG(D_NONE, FN; PTREXP(a); SYCEXP(a->app_key); SYCEXP(msg->synode));
  msg->a = a;
  msg->op = client_msg;
  {
    msg_link *link = msg_link_new(msg, VOID_NODE_NO);
    IFDBG(D_NONE, FN; COPY_AND_FREE_GOUT(dbg_pax_msg(msg)));
    channel_put(&prop_input_queue, &link->l);
  }
}

#define FNVSTART 0x811c9dc5

/* Fowler-Noll-Vo type multiplicative hash */
static uint32_t fnv_hash(unsigned char *buf, size_t length, uint32_t sum) {
  size_t i = 0;
  for (i = 0; i < length; i++) {
    sum = sum * (uint32_t)0x01000193 ^ (uint32_t)buf[i];
  }
  return sum;
}

/**
   Create a new (hopefully unique) ID. The basic idea is to create a hash from
   the host ID and a timestamp.
*/
uint32_t new_id() {
  long id = xcom_unique_long();
  double timestamp = task_now();
  uint32_t retval = 0;
  while (retval == 0 ||
         is_dead_site(retval)) { /* Avoid returning 0 or already used site id */
    retval = fnv_hash((unsigned char *)&id, sizeof(id), 0);
    retval = fnv_hash((unsigned char *)&timestamp, sizeof(timestamp), retval);
  }
  return retval;
}

static synode_no getstart(app_data_ptr a) {
  synode_no retval = null_synode;
  /* If a->group_id is null_id, we set the group id  from app_key.group_id,
   * which is hopefully not null_id. If it is, we're out of luck. */
  if (a && a->group_id == null_id) {
    /* purecov: begin deadcode */
    a->group_id = a->app_key.group_id; /* app_key may have valid group */
    /* purecov: end */
  }
  G_DEBUG("pid %d getstart group_id %x", xpid(), a->group_id);
  if (!a || a->group_id == null_id) {
    retval.group_id = new_id();
  } else {
    a->app_key.group_id = a->group_id;
    retval = a->app_key;
    if (get_site_def() &&
        retval.msgno > 1) { /* Special case for initial boot of site */
      /* Not valid until after event horizon has been passed */
      retval = add_event_horizon(retval);
    }
  }
  return retval;
}

/* purecov: begin deadcode */
synode_no get_default_start(app_data_ptr a) {
  synode_no retval = null_synode;
  /* If a->group_id is null_id, we set the group id  from app_key.group_id,
   * which is hopefully not null_id. If it is, we're out of luck. */
  if (a && a->group_id == null_id) {
    a->group_id = a->app_key.group_id; /* app_key may have valid group */
  }
  G_DEBUG("pid %d getstart group_id %x", xpid(), a->group_id);
  if (!a || a->group_id == null_id) {
    retval.group_id = new_id();
  } else {
    a->app_key.group_id = a->group_id;
    retval = a->app_key;
    if (retval.msgno > 1) { /* Special case for initial boot of site */
      /* Not valid until after event horizon has been passed */
      retval = add_default_event_horizon(retval);
    }
  }
  return retval;
}
/* purecov: end */

/* purecov: begin deadcode */
static void dump_xcom_node_names(site_def const *site) {
  u_int i;
  char buf[NSERVERS * 256]; /* Big enough */
  char *p = buf;
  if (!site) {
    G_INFO("pid %d no site", xpid());
    return;
  }
  *p = 0;
  for (i = 0; i < site->nodes.node_list_len; i++) {
    p = strcat(p, site->nodes.node_list_val[i].address);
    p = strcat(p, " ");
  }
  G_INFO("pid %d node names %s", xpid(), buf);
}
/* purecov: end */

void site_install_action(site_def *site, cargo_type operation) {
  IFDBG(D_NONE, FN; NDBG(get_nodeno(get_site_def()), u));
  assert(site->event_horizon);
  if (group_mismatch(site->start, max_synode) ||
      synode_gt(site->start, max_synode))
    set_max_synode(site->start);
  site->nodeno = xcom_find_node_index(&site->nodes);
  push_site_def(site);
  IFDBG(D_NONE, dump_xcom_node_names(site));
  IFDBG(D_BUG, FN; SYCEXP(site->start); SYCEXP(site->boot_key));
  IFDBG(D_BUG, FN; COPY_AND_FREE_GOUT(dbg_site_def(site)));
  set_group(get_group_id(site));
  if (get_maxnodes(get_site_def())) {
    G_INFO("update_servers is called, max nodes:%u",
           get_maxnodes(get_site_def()));
    update_servers(site, operation);
  }
  site->install_time = task_now();
  G_INFO("pid %d Installed site start=" SY_FMT " boot_key=" SY_FMT
         " event_horizon=%" PRIu32
         " node %u chksum_node_list(&site->nodes) %" PRIu32,
         xpid(), SY_MEM(site->start), SY_MEM(site->boot_key),
         site->event_horizon, get_nodeno(site), chksum_node_list(&site->nodes));
  IFDBG(D_NONE, FN; NDBG(get_nodeno(site), u));
  IFDBG(D_NONE, FN; SYCEXP(site->start); SYCEXP(site->boot_key);
        NDBG(site->install_time, f));
  IFDBG(D_NONE, FN; NDBG(get_nodeno(site), u));
  ADD_DBG(
      D_BASE, add_event(EVENT_DUMP_PAD, string_arg("nodeno"));
      add_event(EVENT_DUMP_PAD, uint_arg(get_nodeno(site)));
      add_event(EVENT_DUMP_PAD, string_arg("site->boot_key"));
      add_synode_event(site->boot_key);
      /* add_event(EVENT_DUMP_PAD, uint_arg(chksum_node_list(&site->nodes))); */
  );
}

static site_def *create_site_def_with_start(app_data_ptr a, synode_no start) {
  site_def *site = new_site_def();
  IFDBG(D_NONE, FN; COPY_AND_FREE_GOUT(dbg_list(&a->body.app_u_u.nodes)););
  init_site_def(a->body.app_u_u.nodes.node_list_len,
                a->body.app_u_u.nodes.node_list_val, site);
  site->start = start;
  site->boot_key = a->app_key;
  return site;
}

static site_def *install_ng_with_start(app_data_ptr a, synode_no start) {
  if (a) {
    site_def *site = create_site_def_with_start(a, start);
    G_INFO("install_ng_with_start calls site_install_action");
    site_install_action(site, a->body.c_t);
    return site;
  }
  return 0;
}

site_def *install_node_group(app_data_ptr a) {
  ADD_DBG(D_BASE, add_event(EVENT_DUMP_PAD, string_arg("a->app_key"));
          add_synode_event(a->app_key););
  if (a)
    return install_ng_with_start(a, getstart(a));
  else
    return 0;
}

void set_max_synode(synode_no synode) {
  max_synode = synode; /* Track max synode number */
  IFDBG(D_NONE, FN; STRLIT("new "); SYCEXP(max_synode));
  activate_sweeper();
}

static int is_busy(synode_no s) {
  pax_machine *p = hash_get(s);
  if (!p) {
    return 0;
  } else {
    return started(p);
  }
}

bool_t match_my_msg(pax_msg *learned, pax_msg *mine) {
  IFDBG(D_NONE, FN; PTREXP(learned->a);
        if (learned->a) SYCEXP(learned->a->unique_id); PTREXP(mine->a);
        if (mine->a) SYCEXP(mine->a->unique_id););
  if (learned->a && mine->a) { /* Both have app data, see if data is mine */
    return synode_eq(learned->a->unique_id, mine->a->unique_id);
  } else if (!(learned->a || mine->a)) { /* None have app data, anything goes */
    return TRUE;
  } else { /* Definitely mismatch */
    return FALSE;
  }
}

/*
 * Initialize the log sequence number (lsn).
 */
void initialize_lsn(uint64_t n) { lsn = n; }

/**
 * Assign the next log sequence number (lsn) for a message.
 *
 * Initial propose sets lsn to msgno of the max message number as safe starting
 * point, otherwise lsn shall be ever increasing. lsn ensures sender order known
 * on receiver side, as messages may arrive "out of order" due to
 * retransmission. We use max_synode instead of current_message to avoid any
 * conflict with lsn allocated by a previous instance of the node.
 */
static uint64_t assign_lsn() {
  if (lsn == 0) {
    initialize_lsn(max_synode.msgno);
  }
  lsn++;
  IFDBG(D_EXEC, NDBG64(lsn));
  return lsn;
}

/* purecov: begin deadcode */
static int check_lsn(app_data_ptr a) {
  while (a) {
    if (!a->lsn) return 0;
    a = a->next;
  }
  return 1;
}
/* purecov: end */

static void propose_noop(synode_no find, pax_machine *p);

/**
 * Checks if the given synod s is outside the event horizon.
 *
 * Common case: there are no configurations pending, or if there are, none of
 * them reconfigure the event horizon. The common case threshold is:
 *
 *   last_executed_synod + event_horizon(active_config)
 *
 *
 * If an event horizon reconfiguration R is pending, it is possible that it
 * reduces the event horizon. In that case, it is possible that the threshold
 * above falls outside the new event horizon.
 *
 * For example, consider last_executed_synod = 42 and
 * event_horizon(active_config) = 10.
 * At this point this member participates in synods up to 52.
 * Now consider an event horizon reconfiguration that takes effect at synod 45,
 * which modifies the event horizon to 2. This means that when
 * last_executed_synod = 45, event_horizon(active_config) = 2. At this point
 * this member should only be able to participate in synods up to 47. The member
 * may have previously started processing messages directed to synods between 47
 * and 52, but will now ignore messages directed to those same synods.
 *
 * We do not want to start processing messages that will eventually fall out
 * of the event horizon. More importantly, the threshold above may not be safe
 * due to the exit logic of executor_task.
 *
 * When a node removes itself from the group on configuration C starting at
 * synod start(C), the exit logic relies on knowing *when* a majority has
 * executed synod start(C) - 1, i.e. the last message of the last configuration
 * to contain the leaving node.
 *
 * With a constant event horizon, we know that when synod
 * start(C) + event_horizon is learnt, it is because a majority already executed
 * or is ready to execute (and thus learned) synod start(C). This implies that a
 * majority already executed start(C) - 1.
 *
 * With a dynamic event horizon, we cannot be sure that when synod
 * start(C) + event_horizon(C) is learnt, a majority already executed or is
 * ready to execute synod start(C).
 * This is because it is possible for a new, smaller, event horizon to take
 * effect between start(C) and start(C) + event_horizon(C).
 * If that happens, the threshold above allows nodes to participate in synods
 * which are possibly beyond start(C) + event_horizon(C), which can lead to the
 * value of synod start(C) + event_horizon(C) being learnt without a majority
 * already having executed or being ready to execute synod start(C).
 *
 * In order to maintain the assumption made by the executor_task's exit logic,
 * when an event horizon reconfiguration R is pending we set the threshold to
 * the minimum between:
 *
 *   last_executed_synod + event_horizon(active_config)
 *
 * and:
 *
 *   start(R) - 1 + event_horizon(R)
 */
static uint64_t too_far_threshold(xcom_event_horizon active_event_horizon) {
  return executed_msg.msgno + active_event_horizon;
}

static uint64_t too_far_threshold_new_event_horizon_pending(
    site_def const *new_config) {
  uint64_t last_executed = executed_msg.msgno;
  /* compute normal threshold */
  uint64_t possibly_unsafe_threshold;
  site_def const *active_config = find_site_def(executed_msg);
  xcom_event_horizon active_event_horizon = active_config->event_horizon;
  possibly_unsafe_threshold = last_executed + active_event_horizon;
  /* compute threshold taking into account new event horizon */ {
    uint64_t maximum_safe_threshold;
    xcom_event_horizon new_event_horizon;
    uint64_t start_new_event_horizon = new_config->start.msgno;
    new_event_horizon = new_config->event_horizon;
    maximum_safe_threshold = start_new_event_horizon - 1 + new_event_horizon;
    /* use the minimum of both for safety */
    return MIN(possibly_unsafe_threshold, maximum_safe_threshold);
  }
}

static inline int too_far(synode_no s) {
  uint64_t threshold = 0;
  site_def const *active_config = find_site_def(executed_msg);
  if (active_config != NULL) {
    site_def const *pending_config = first_event_horizon_reconfig();
    bool_t const no_event_horizon_reconfig_pending = (pending_config == NULL);
    if (is_latest_config(active_config) || no_event_horizon_reconfig_pending) {
      threshold = too_far_threshold(active_config->event_horizon);
    } else {
      threshold = too_far_threshold_new_event_horizon_pending(pending_config);
    }
  } else {
    /* we have no configs, resort to default */
    threshold = too_far_threshold(EVENT_HORIZON_MIN);
  }
  return s.msgno >= threshold;
}

#define GOTO(x)                                 \
  {                                             \
    IFDBG(D_NONE, STRLIT("goto "); STRLIT(#x)); \
    goto x;                                     \
  }

static inline int is_view(cargo_type x) { return x == view_msg; }

static inline int is_config(cargo_type x) {
  return x == unified_boot_type || x == add_node_type ||
         x == remove_node_type || x == set_event_horizon_type ||
         x == force_config_type;
}

static int wait_for_cache(pax_machine **pm, synode_no synode, double timeout);
static int prop_started = 0;
static int prop_finished = 0;

/* Send messages by fetching from the input queue and trying to get it accepted
   by a Paxos instance */
static int proposer_task(task_arg arg) {
  DECL_ENV
  int self;             /* ID of this proposer task */
  pax_machine *p;       /* Pointer to Paxos instance */
  msg_link *client_msg; /* The client message we are trying to push */
  synode_no msgno;
  pax_msg *prepare_msg;
  double start_propose;
  double start_push;
  double delay;
  ulong basic_timeout;
  site_def const *site;
  size_t size;
  size_t nr_batched_app_data;
  END_ENV;

  TASK_BEGIN

  ep->self = get_int_arg(arg);
  ep->p = NULL;
  ep->client_msg = NULL;
  ep->prepare_msg = NULL;
  ep->start_propose = 0.0;
  ep->start_push = 0.0;
  ep->delay = 0.0;
  ep->msgno = current_message;
  ep->site = 0;
  ep->size = 0;
  ep->nr_batched_app_data = 0;
  ep->basic_timeout = DEFAULT_DETECTOR_LIVE_TIMEOUT;

  IFDBG(D_NONE, FN; NDBG(ep->self, d); NDBG(task_now(), f));

  while (!xcom_shutdown) { /* Loop until no more work to do */
    /* Wait for client message */
    assert(!ep->client_msg);
    CHANNEL_GET(&prop_input_queue, &ep->client_msg, msg_link);
    prop_started++;
    IFDBG(D_NONE, FN; PTREXP(ep->client_msg->p->a); STRLIT("extracted ");
          SYCEXP(ep->client_msg->p->a->app_key));

    /* Grab rest of messages in queue as well, but never batch config messages,
     * which need a unique number */

    /* The batch is limited either by size or number of batched app_datas.
     * We limit the number of elements because the XDR deserialization
     * implementation is recursive, and batching too many app_datas will cause a
     * call stack overflow. */
    if (!is_config(ep->client_msg->p->a->body.c_t) &&
        !is_view(ep->client_msg->p->a->body.c_t)) {
      ep->size = app_data_size(ep->client_msg->p->a);
      ep->nr_batched_app_data = 1;
      while (AUTOBATCH && ep->size <= MAX_BATCH_SIZE &&
             ep->nr_batched_app_data <= MAX_BATCH_APP_DATA &&
             !link_empty(&prop_input_queue
                              .data)) { /* Batch payloads into single message */
        msg_link *tmp;
        app_data_ptr atmp;

        CHANNEL_GET(&prop_input_queue, &tmp, msg_link);
        atmp = tmp->p->a;
        ep->size += app_data_size(atmp);
        ep->nr_batched_app_data++;
        /* Abort batching if config or too big batch */
        if (is_config(atmp->body.c_t) || is_view(atmp->body.c_t) ||
            ep->nr_batched_app_data > MAX_BATCH_APP_DATA ||
            ep->size > MAX_BATCH_SIZE) {
          channel_put_front(&prop_input_queue, &tmp->l);
          break;
        }
        ADD_T_EV(seconds(), __FILE__, __LINE__, "batching");

        tmp->p->a = 0;                     /* Steal this payload */
        msg_link_delete(&tmp);             /* Get rid of the empty message */
        atmp->next = ep->client_msg->p->a; /* Add to list of app_data */
                                           /* G_TRACE("Batching %s %s",
                                            * cargo_type_to_str(ep->client_msg->p->a->body.c_t), */
        /* 	cargo_type_to_str(atmp->body.c_t)); */
        ep->client_msg->p->a = atmp;
        IFDBG(D_NONE, FN; PTREXP(ep->client_msg->p->a); STRLIT("extracted ");
              SYCEXP(ep->client_msg->p->a->app_key));
      }
    }

    ep->start_propose = task_now();
    ep->delay = 0.0;

    assert(!ep->client_msg->p->a->chosen);

    /* It is a new message */

    assert(!synode_eq(current_message, null_synode));

    /* Assign a log sequence number only on initial propose */
    {
      uint64_t prop_lsn = assign_lsn();
      app_data_ptr ap = ep->client_msg->p->a;
      /* Assign to all app_data structs */
      while (ap) {
        ap->lsn = prop_lsn;
        ap = ap->next;
      }
    }
    DBGOUT_ASSERT(check_lsn(ep->client_msg->p->a), STRLIT("NULL lsn"));
  retry_new:
    /* Find a free slot */

    assert(!synode_eq(current_message, null_synode));
    ep->msgno = current_message;
    proposer_site = find_site_def_rw(ep->msgno);
    ep->site = proposer_site;

    while (is_busy(ep->msgno)) {
      while (/* ! ep->client_msg->p->force_delivery &&  */ too_far(
          incr_msgno(ep->msgno))) { /* Too far ahead of executor */
        TIMED_TASK_WAIT(&exec_wait, 1.0);
        if (the_app_xcom_cfg) {
          ep->basic_timeout = the_app_xcom_cfg->m_flp_timeout;
        }
        IFDBG(D_NONE, FN; SYCEXP(ep->msgno); TIMECEXP(ep->start_propose);
              TIMECEXP(ep->client_msg->p->a->expiry_time); TIMECEXP(task_now());

              NDBG(enough_live_nodes(ep->site, ep->basic_timeout), d));
#ifdef DELIVERY_TIMEOUT
        if ((ep->start_propose + ep->client_msg->p->a->expiry_time) <
                task_now() &&
            !enough_live_nodes(ep->site, ep->basic_timeout)) {
          /* Give up */
          DBGOUT_ASSERT(check_lsn(ep->client_msg->p->a), STRLIT("NULL lsn"));
          IFDBG(D_NONE, FN; STRLIT("timeout -> delivery_failure"));
          deliver_to_app(NULL, ep->client_msg->p->a, delivery_failure);
          GOTO(next);
        }
#endif
      }
      ep->msgno = incr_msgno(ep->msgno);
      /* Refresh site to next msgno */
      proposer_site = find_site_def_rw(ep->msgno);
      ep->site = proposer_site;
    }
    assert(!synode_eq(ep->msgno, null_synode));

    /* See if we can do anything with this message */
    if (!ep->site || get_nodeno(ep->site) == VOID_NODE_NO) {
      /* Give up */
      DBGOUT_ASSERT(check_lsn(ep->client_msg->p->a), STRLIT("NULL lsn"));
      IFDBG(D_NONE, FN; STRLIT("delivery_failure "); SYCEXP(ep->msgno);
            PTREXP(ep->site); NDBG(get_nodeno(ep->site), u));
      deliver_to_app(NULL, ep->client_msg->p->a, delivery_failure);
      GOTO(next);
    }
    IFDBG(D_NONE, FN; STRLIT("changing current message to ");
          SYCEXP(ep->msgno));
    set_current_message(ep->msgno);

    brand_client_msg(ep->client_msg->p, ep->msgno);

    for (;;) { /* Loop until the client message has been learned */
      /* Get a Paxos instance to send the client message */

      TASK_CALL(wait_for_cache(&ep->p, ep->msgno, 60));
      if (!ep->p) {
        G_MESSAGE("Could not get a pax_machine for msgno %lu. Retrying",
                  (unsigned long)ep->msgno.msgno);
        goto retry_new;
      }

      assert(ep->p);
      if (ep->client_msg->p->force_delivery)
        ep->p->force_delivery = ep->client_msg->p->force_delivery;
      {
        int MY_ATTRIBUTE((unused)) lock = lock_pax_machine(ep->p);
        assert(!lock);
      }

      /* Set the client message as current proposal */
      assert(ep->client_msg->p);
      replace_pax_msg(&ep->p->proposer.msg, clone_pax_msg(ep->client_msg->p));
      if (ep->p->proposer.msg == NULL) {
        g_critical(
            "Node %u has run out of memory while sending a message and "
            "will now exit.",
            get_nodeno(proposer_site));
        terminate_and_exit(); /* Tell xcom to stop */
        TERMINATE;
      }
      assert(ep->p->proposer.msg);
      PAX_MSG_SANITY_CHECK(ep->p->proposer.msg);

      /* Create the prepare message */
      unchecked_replace_pax_msg(&ep->prepare_msg,
                                pax_msg_new(ep->msgno, ep->site));
      IFDBG(D_NONE, FN; PTREXP(ep->client_msg->p->a); STRLIT("pushing ");
            SYCEXP(ep->msgno));
      IFDBG(D_NONE, FN; COPY_AND_FREE_GOUT(dbg_app_data(ep->prepare_msg->a)));

      /* Use 3 phase algorithm if threephase is set or we are forcing or we have
         already accepted something, which may happen if another node has timed
         out waiting for this node and proposed a no_op, which we have accepted.
       */
      if (threephase || ep->p->force_delivery || ep->p->acceptor.promise.cnt) {
        push_msg_3p(ep->site, ep->p, ep->prepare_msg, ep->msgno, normal);
      } else {
        push_msg_2p(ep->site, ep->p);
      }

      ep->start_push = task_now();

      while (!finished(ep->p)) { /* Try to get a value accepted */
        /* We will wake up periodically, and whenever a message arrives */
        TIMED_TASK_WAIT(&ep->p->rv,
                        ep->delay = wakeup_delay(ep->site, ep->delay));
        if (!synode_eq(ep->msgno, ep->p->synode) ||
            ep->p->proposer.msg == NULL) {
          IFDBG(D_NONE, FN; STRLIT("detected stolen state machine, retry"););
          /* unlock_pax_machine(ep->p); */
          GOTO(retry_new); /* Need to break out of both loops,
                                                  and we have no "exit named
                              loop" construction */
        }
        assert(synode_eq(ep->msgno, ep->p->synode) && ep->p->proposer.msg);
        if (finished(ep->p)) break;
        {
          double now = task_now();
#ifdef DELIVERY_TIMEOUT
          if ((ep->start_propose + ep->client_msg->p->a->expiry_time) < now) {
            IFDBG(D_NONE, FN; STRLIT("timeout when pushing ");
                  SYCEXP(ep->msgno); SYCEXP(executed_msg));
            /* Proposing a no-op here is a last ditch effort to cancel the
            failed message. If any of the currently reachable nodes have
            participated in the failed consensus round, it is equivalent to
            retrying a final time, otherwise we could get a no-op
            accepted. Proposing a no-op is always harmless.
            Having a timeout on delivery and telling the client is really
            contrary to the spirit of
            Paxos, since we cannot guarantee that the message has not been
            delivered, but at the moment, MCM depends on it.
            Proposing a no-op here increases the probability that the outcome
            matches what we tell MCM about the outcome. */
            propose_noop(ep->msgno, ep->p);
            DBGOUT_ASSERT(check_lsn(ep->client_msg->p->a), STRLIT("NULL lsn"));
            IFDBG(D_NONE, FN; STRLIT("timeout -> delivery_failure"));
            deliver_to_app(ep->p, ep->client_msg->p->a, delivery_failure);
            unlock_pax_machine(ep->p);
            GOTO(next);
          }
#endif
          /* Retry pushing if the accumulative delay is more than one second */
          if ((ep->start_push + 1.0) <= now) {
            PAX_MSG_SANITY_CHECK(ep->p->proposer.msg);
            IFDBG(D_NONE, FN; STRLIT("retry pushing "); SYCEXP(ep->msgno));
            IFDBG(D_NONE, FN;
                  COPY_AND_FREE_GOUT(dbg_app_data(ep->prepare_msg->a)););
            IFDBG(D_NONE, BALCEXP(ep->p->proposer.bal);
                  BALCEXP(ep->p->acceptor.promise));
            push_msg_3p(ep->site, ep->p, ep->prepare_msg, ep->msgno, normal);
            ep->start_push = now;
          }
        }
      }
      /* When we get here, we know the value for this message number,
         but it may not be the value we tried to push,
         so loop until we have a successful push. */
      unlock_pax_machine(ep->p);
      IFDBG(D_NONE, FN; STRLIT(" found finished message "); SYCEXP(ep->msgno);
            STRLIT("seconds since last push ");
            NPUT(task_now() - ep->start_push, f); STRLIT("ep->client_msg ");
            COPY_AND_FREE_GOUT(dbg_pax_msg(ep->client_msg->p)););
      IFDBG(D_NONE, FN; STRLIT("ep->p->learner.msg ");
            COPY_AND_FREE_GOUT(dbg_pax_msg(ep->p->learner.msg)););
      if (match_my_msg(ep->p->learner.msg, ep->client_msg->p)) {
        break;
      } else
        GOTO(retry_new);
    }
  next : {
    double now = task_now();
    double used = now - ep->start_propose;
    add_to_filter(used);
    prop_finished++;
    IFDBG(D_NONE, FN; STRLIT("completed ep->msgno "); SYCEXP(ep->msgno);
          NDBG(used, f); NDBG(median_time(), f);
          STRLIT("seconds since last push "); NDBG(now - ep->start_push, f););
    IFDBG(D_NONE, FN; STRLIT("ep->client_msg ");
          COPY_AND_FREE_GOUT(dbg_pax_msg(ep->client_msg->p)););
    if (ep->p) {
      IFDBG(D_NONE, FN; STRLIT("ep->p->learner.msg ");
            COPY_AND_FREE_GOUT(dbg_pax_msg(ep->p->learner.msg)););
    }
    msg_link_delete(&ep->client_msg);
  }
  }
  FINALLY
  IFDBG(D_BUG, FN; STRLIT("exit "); NDBG(ep->self, d); NDBG(task_now(), f));
  if (ep->p) {
    unlock_pax_machine(ep->p);
  }
  replace_pax_msg(&ep->prepare_msg, NULL);
  if (ep->client_msg) { /* If we get here with a client message, we have
                           failed to deliver */
    DBGOUT_ASSERT(check_lsn(ep->client_msg->p->a), STRLIT("NULL lsn"));
    IFDBG(D_NONE, FN;
          STRLIT("undelivered message at task end -> delivery_failure"));
    deliver_to_app(ep->p, ep->client_msg->p->a, delivery_failure);
    msg_link_delete(&ep->client_msg);
  }
  TASK_END;
}

static xcom_proto constexpr first_protocol_that_ignores_intermediate_forced_configs_or_views =
    x_1_8;

static bool constexpr should_ignore_forced_config_or_view(
    xcom_proto protocol_version) {
  return protocol_version >=
         first_protocol_that_ignores_intermediate_forced_configs_or_views;
}

static node_no leader(site_def const *s) {
  node_no leader = 0;
  int silent = DEFAULT_DETECTOR_LIVE_TIMEOUT;
  if (the_app_xcom_cfg) {
    silent = the_app_xcom_cfg->m_flp_timeout;
  }
  for (leader = 0; leader < get_maxnodes(s); leader++) {
    if (!may_be_dead(s->detected, leader, task_now(), silent,
                     s->servers[leader]->unreachable))
      return leader;
  }
  return 0;
}

int iamthegreatest(site_def const *s) { return leader(s) == s->nodeno; }

void execute_msg(site_def *site, pax_machine *pma, pax_msg *p) {
  app_data_ptr a = p->a;
  IFDBG(D_EXEC, FN; COPY_AND_FREE_GOUT(dbg_pax_msg(p)););
  if (a) {
    switch (a->body.c_t) {
      case unified_boot_type:
      case force_config_type:
        deliver_config(a);
      case add_node_type:
      case remove_node_type:
        break;
      case app_type:
        IFDBG(D_NONE, FN; STRLIT(" learner.msg ");
              COPY_AND_FREE_GOUT(dbg_pax_msg(pma->learner.msg)););
        /* DBGOUT_ASSERT(check_lsn(a), STRLIT("NULL lsn")); */
        deliver_to_app(pma, a, delivery_ok);
        break;
      case view_msg:
        IFDBG(D_EXEC, FN; STRLIT(" global view ");
              COPY_AND_FREE_GOUT(dbg_pax_msg(pma->learner.msg)););
        if (site && site->global_node_set.node_set_len ==
                        a->body.app_u_u.present.node_set_len) {
          if ((p->force_delivery != 0) &&
              should_ignore_forced_config_or_view(site->x_proto)) {
            G_DEBUG(
                "execute_msg: Ignoring a forced intermediate, pending "
                "view_msg");
          } else {
            assert(site->global_node_set.node_set_len ==
                   a->body.app_u_u.present.node_set_len);
            copy_node_set(&a->body.app_u_u.present, &site->global_node_set);
            deliver_global_view_msg(site, p->synode);
            ADD_DBG(D_BASE,
                    add_event(EVENT_DUMP_PAD,
                              string_arg("deliver_global_view_msg p->synode"));
                    add_synode_event(p->synode););
          }
        }
        break;
      default:
        break;
    }
  }
  IFDBG(D_NONE, FN; SYCEXP(p->synode));
}

static void read_missing_values(int n);
static void propose_missing_values(int n);

#ifdef EXECUTOR_TASK_AGGRESSIVE_NO_OP
/* With many nodes sending read_ops on instances that are not decided yet, it
 * may take a very long time until someone finally decides to start a new
 * consensus round. As the cost of a new proposal is not that great, it's
 * acceptable to go directly to proposing a no-op instead of first trying to get
 * the value with a read_op. An added benefit of this is that if more than one
 * node needs the result, they will get it all when the consensus round
 * finishes. */
static void find_value(site_def const *site, unsigned int *wait, int n) {
  IFDBG(D_NONE, FN; NDBG(*wait, d));

  if (get_nodeno(site) == VOID_NODE_NO) {
    read_missing_values(n);
    return;
  }

  if ((*wait) > 1 || /* Only leader will propose initially */
      ((*wait) > 0 && iamthegreatest(site)))
    propose_missing_values(n);

#ifdef TASK_EVENT_TRACE
  if ((*wait) > 1) dump_task_events();
#endif
  (*wait)++;
}
#else
static void find_value(site_def const *site, unsigned int *wait, int n) {
  IFDBG(D_NONE, FN; NDBG(*wait, d));

  if (get_nodeno(site) == VOID_NODE_NO) {
    read_missing_values(n);
    return;
  }

  switch (*wait) {
    case 0:
    case 1:
      read_missing_values(n);
      (*wait)++;
      break;
    case 2:
      if (iamthegreatest(site))
        propose_missing_values(n);
      else
        read_missing_values(n);
      (*wait)++;
      break;
    case 3:
      propose_missing_values(n);
      break;
    default:
      break;
  }
}
#endif /* EXECUTOR_TASK_AGGRESSIVE_NO_OP */

static void dump_debug_exec_state();

#ifdef PROPOSE_IF_LEADER
int get_xcom_message(pax_machine **p, synode_no msgno, int n) {
  DECL_ENV
  unsigned int wait;
  double delay;
  int silent;
  site_def const *site;
  END_ENV;

  TASK_BEGIN

  ep->wait = 0;
  ep->delay = 0.0;
  *p = force_get_cache(msgno);
  ep->site = NULL;
  ep->silent = DEFAULT_DETECTOR_LIVE_TIMEOUT;

  dump_debug_exec_state();
  while (!finished(*p)) {
    ep->site = find_site_def(msgno);
    /* The end of the world ?, fake message by skipping */
    if (get_maxnodes(ep->site) == 0) {
      pax_msg *msg = pax_msg_new(msgno, ep->site);
      handle_skip(ep->site, *p, msg);
      break;
    }
    IFDBG(D_NONE, FN; STRLIT(" not finished "); SYCEXP(msgno); PTREXP(*p);
          NDBG(ep->wait, u); SYCEXP(msgno));
    if (the_app_xcom_cfg) {
      ep->silent = the_app_xcom_cfg->m_flp_timeout;
    }
    if (get_maxnodes(ep->site) > 1 && iamthegreatest(ep->site) &&
        ep->site->global_node_set.node_set_val &&
        !ep->site->global_node_set.node_set_val[msgno.node] &&
        may_be_dead(ep->site->detected, msgno.node, task_now(), silent,
                    ep->site->servers[msgno.node]->unreachable)) {
      propose_missing_values(n);
    } else {
      find_value(ep->site, &ep->wait, n);
    }
    TIMED_TASK_WAIT(&(*p)->rv, ep->delay = wakeup_delay_for_perf(ep->delay));
    *p = get_cache(msgno);
    dump_debug_exec_state();
  }

  FINALLY
  IFDBG(D_NONE, FN; SYCEXP(msgno); PTREXP(*p); NDBG(ep->wait, u);
        SYCEXP(msgno));
  TASK_END;
}
#else
int get_xcom_message(pax_machine **p, synode_no msgno, int n) {
  DECL_ENV
  unsigned int wait;
  double delay;
  site_def const *site;
  END_ENV;

  TASK_BEGIN

  ep->wait = 0;
  ep->delay = 0.0;
  *p = force_get_cache(msgno);
  ep->site = NULL;

  dump_debug_exec_state();
  while (!finished(*p)) {
    ep->site = find_site_def(msgno);
    /* The end of the world ?, fake message by skipping */
    if (get_maxnodes(ep->site) == 0) {
      pax_msg *msg = pax_msg_new(msgno, ep->site);
      handle_skip(ep->site, *p, msg);
      break;
    }
    IFDBG(D_NONE, FN; STRLIT("before find_value"); SYCEXP(msgno); PTREXP(*p);
          NDBG(ep->wait, u); SYCEXP(msgno));
    find_value(ep->site, &ep->wait, n);
    IFDBG(D_NONE, FN; STRLIT("after find_value"); SYCEXP(msgno); PTREXP(*p);
          NDBG(ep->wait, u); SYCEXP(msgno));
    ep->delay = wakeup_delay_for_perf(ep->delay);
    IFDBG(D_NONE, FN; NDBG(ep->delay, f));
    TIMED_TASK_WAIT(&(*p)->rv, ep->delay);
    *p = get_cache(msgno);
    dump_debug_exec_state();
  }

  FINALLY
  TASK_END;
}
#endif

synode_no set_executed_msg(synode_no msgno) {
  IFDBG(D_EXEC, FN; STRLIT("changing executed_msg from "); SYCEXP(executed_msg);
        STRLIT(" to "); SYCEXP(msgno));
  if (group_mismatch(msgno, current_message) ||
      synode_gt(msgno, current_message)) {
    IFDBG(D_EXEC, FN; STRLIT("changing current message"));
    set_current_message(first_free_synode(msgno));
  }

  if (msgno.msgno > executed_msg.msgno) task_wakeup(&exec_wait);

  executed_msg = msgno;
  executor_site = find_site_def_rw(executed_msg);
  return executed_msg;
}

static synode_no first_free_synode(synode_no msgno) {
  site_def const *site = find_site_def(msgno);
  synode_no retval = msgno;
  if (!site) {
    /* purecov: begin deadcode */
    site = get_site_def();
    IFDBG(D_NONE, FN; PTREXP(site); SYCEXP(msgno));
    assert(get_group_id(site) != 0);
    return site->start;
    /* purecov: end */
  }
  if (get_group_id(site) == 0) {
    IFDBG(D_NONE, FN; PTREXP(site); SYCEXP(msgno));
    if (site) {
      IFDBG(D_NONE, FN; SYCEXP(site->boot_key); SYCEXP(site->start);
            COPY_AND_FREE_GOUT(dbg_site_def(site)));
    }
  }
  assert(get_group_id(site) != 0);
  assert(!synode_eq(msgno, null_synode));
  if (retval.msgno == 0) retval.msgno = 1;
  retval.node = get_nodeno(site);
  if (synode_lt(retval, msgno))
    return incr_msgno(retval);
  else
    return retval;
}

synode_no set_current_message(synode_no msgno) {
  IFDBG(D_PROPOSE, FN; STRLIT("changing current_message from ");
        SYCEXP(current_message); STRLIT(" to "); SYCEXP(msgno));
  return current_message = msgno;
}

static void update_max_synode(pax_msg *p);

#if TASK_DBUG_ON
static void perf_dbg(int *_n, int *_old_n, double *_old_t)
    MY_ATTRIBUTE((unused));
static void perf_dbg(int *_n, int *_old_n, double *_old_t) {
  int n = *_n;
  int old_n = *_old_n;
  double old_t = *_old_t;

  if (!IS_XCOM_DEBUG_WITH(XCOM_DEBUG_TRACE)) return;

  IFDBG(D_NONE, FN; SYCEXP(executed_msg));
  if (!(n % 5000)) {
    GET_GOUT;
    NDBG(get_nodeno(get_site_def()), u);
    NDBG(task_now(), f);
    NDBG(n, d);
    NDBG(median_time(), f);
    SYCEXP(executed_msg);
    PRINT_GOUT;
    FREE_GOUT;
  }
  (*_n)++;
  if (task_now() - old_t > 1.0) {
    GET_GOUT;
    NDBG(get_nodeno(get_site_def()), u);
    NDBG(task_now(), f);
    NDBG(n, d);
    NDBG((n - old_n) / (task_now() - old_t), f);
    PRINT_GOUT;
    FREE_GOUT;
    *_old_t = task_now();
    *_old_n = n;
  }
}
#endif

#ifdef IGNORE_LOSERS

static inline int LOSER(synode_no x, site_def const *site) {
  IFDBG(D_NONE, NEXP(x.node, u);
        NEXP(site->global_node_set.node_set_val[(x).node], d));
  return (!(site)->global_node_set.node_set_val[(x).node]);
}

#else
#define LOSER(x, site) 0
#endif

static void debug_loser(synode_no x) MY_ATTRIBUTE((unused));
#if defined(TASK_DBUG_ON) && TASK_DBUG_ON
static void debug_loser(synode_no x) {
  if (!IS_XCOM_DEBUG_WITH(XCOM_DEBUG_TRACE)) return;
  if (1 || x.msgno < 10) {
    GET_GOUT;
    NDBG(get_nodeno(find_site_def(x)), u);
    STRLIT(" ignoring loser ");
    SYCEXP(x);
    SYCEXP(max_synode);
    PRINT_GOUT;
    FREE_GOUT;
  }
}
#else
/* purecov: begin deadcode */
static void debug_loser(synode_no x MY_ATTRIBUTE((unused))) {}
/* purecov: end */
#endif

static void send_value(site_def const *site, node_no to, synode_no synode) {
  pax_machine *pm = get_cache(synode);
  if (pm && pm->learner.msg) {
    pax_msg *msg = clone_pax_msg(pm->learner.msg);
    if (msg == NULL) return;
    ref_msg(msg);
    send_server_msg(site, to, msg);
    unref_msg(&msg);
  }
}

/**
 * Returns the message number where it is safe for nodes in previous
 * configuration to exit.
 *
 * @param start start synod of the next configuration
 * @param event_horizon event horizon of the next configuration
 */
static synode_no compute_delay(synode_no start,
                               xcom_event_horizon event_horizon) {
  start.msgno += event_horizon;
  return start;
}

/* Push messages to all nodes which were in the previous site, but not in this
 */
static void inform_removed(int index, int all) {
  site_def **sites = 0;
  uint32_t site_count = 0;
  IFDBG(D_NONE, FN; NEXP(index, d));
  get_all_site_defs(&sites, &site_count);
  while (site_count > 1 && index >= 0 && (uint32_t)(index + 1) < site_count) {
    site_def *s = sites[index];
    site_def *ps = sites[index + 1];

    /* Compute diff and push messages */
    IFDBG(D_NONE, FN; NDBG(index, d); PTREXP(s); if (s) SYCEXP(s->boot_key);
          PTREXP(ps); if (ps) SYCEXP(ps->boot_key));

    if (s && ps) {
      node_no i = 0;
      IFDBG(D_NONE, FN; SYCEXP(s->boot_key); SYCEXP(s->start);
            SYCEXP(ps->boot_key); SYCEXP(ps->start));
      for (i = 0; i < ps->nodes.node_list_len; i++) { /* Loop over prev site */
        if (ps->nodeno != i &&
            !node_exists(&ps->nodes.node_list_val[i], &s->nodes)) {
          synode_no synode = s->start;
          synode_no end = max_synode;
          while (!synode_gt(synode, end)) { /* Loop over relevant messages */
            send_value(ps, i, synode);
            synode = incr_synode(synode);
          }
        }
      }
    }
    if (!all) /* Early exit if not all configs should be examined */
      break;
    index--;
  }
}

static bool_t backwards_compatible(xcom_event_horizon event_horizon) {
  return event_horizon == EVENT_HORIZON_MIN;
}

static xcom_proto const first_event_horizon_aware_protocol = x_1_4;

static bool_t reconfigurable_event_horizon(xcom_proto protocol_version) {
  return protocol_version >= first_event_horizon_aware_protocol;
}

static bool_t add_node_unsafe_against_ipv4_old_nodes(app_data_ptr a) {
  assert(a->body.c_t == add_node_type);

  {
    site_def const *latest_config = get_site_def();
    if (latest_config && latest_config->x_proto >= minimum_ipv6_version())
      return FALSE;

    {
      u_int const nr_nodes_to_add = a->body.app_u_u.nodes.node_list_len;
      node_address *nodes_to_add = a->body.app_u_u.nodes.node_list_val;

      u_int i;
      xcom_port node_port = 0;
      char node_addr[IP_MAX_SIZE];

      for (i = 0; i < nr_nodes_to_add; i++) {
        if (get_ip_and_port(nodes_to_add[i].address, node_addr, &node_port)) {
          G_ERROR(
              "Error parsing address from a joining node. Join operation "
              "will be "
              "rejected");
          return TRUE;
        }

        if (!is_node_v4_reachable(node_addr)) return TRUE;
      }
    }

    return FALSE;
  }
}

/**
 * Check if a node is compatible with the group's event horizon.
 *
 * A node is compatible with the group's configuration if:
 *
 *    a) The node supports event horizon reconfigurations, or
 *    b) The group's event horizon is, or is scheduled to be, the default event
 *       horizon.
 */
static bool_t unsafe_against_event_horizon(node_address const *node) {
  site_def const *latest_config = get_site_def();
  xcom_proto node_max_protocol_version = node->proto.max_proto;
  bool_t const compatible =
      reconfigurable_event_horizon(node_max_protocol_version) ||
      backwards_compatible(latest_config->event_horizon);

  if (!compatible) {
    /*
     * The node that wants to join does not support event horizon
     * reconfigurations and the group's event horizon is, or is scheduled to
     * be, different from the default.
     * The node can not safely join the group so we deny its attempt to join.
     */
    G_INFO(
        "%s's request to join the group was rejected because the group's event "
        "horizon is, or will be %" PRIu32 " and %s only supports %" PRIu32,
        node->address, get_site_def()->event_horizon, node->address,
        EVENT_HORIZON_MIN);
    return TRUE;
  }
  return FALSE;
}

static bool_t add_node_unsafe_against_event_horizon(app_data_ptr a) {
  assert(a->body.c_t == add_node_type);
  {
    u_int nodes_len = a->body.app_u_u.nodes.node_list_len;
    node_address *nodes_to_add = a->body.app_u_u.nodes.node_list_val;
    u_int i;
    for (i = 0; i < nodes_len; i++) {
      if (unsafe_against_event_horizon(&nodes_to_add[i])) return TRUE;
    }
  }
  return FALSE;
}

/**
 * Reconfigure the group membership: add new member(s).
 *
 * It is possible that concurrent reconfigurations take effect between the time
 * this reconfiguration was proposed and now.
 *
 * Particularly, it is possible that any of the concurrent reconfigurations
 * modified the event horizon and that the new member(s) do not support event
 * horizon reconfigurations.
 *
 * We account for these situations by validating if adding the new members is
 * still possible under the current state.
 *
 * If it is not, this reconfiguration does not produce any effect, i.e. no new
 * configuration is installed.
 */
site_def *handle_add_node(app_data_ptr a) {
  if (add_node_unsafe_against_event_horizon(a)) {
    /*
     * Note that the result of this function is only applicable to
     * unused and not-fully-implemented code paths where add_node_type is used
     * forcibly.
     * Should this fact change, this obviously does not work.
     */
    return NULL;
  }
  {
    site_def *site = clone_site_def(get_site_def());
    IFDBG(D_NONE, FN; COPY_AND_FREE_GOUT(dbg_list(&a->body.app_u_u.nodes)););
    IFDBG(D_NONE, FN; COPY_AND_FREE_GOUT(dbg_list(&a->body.app_u_u.nodes)););
    ADD_DBG(D_BASE, add_event(EVENT_DUMP_PAD, string_arg("a->app_key"));
            add_synode_event(a->app_key););
    assert(get_site_def());
    assert(site);
    add_site_def(a->body.app_u_u.nodes.node_list_len,
                 a->body.app_u_u.nodes.node_list_val, site);
    site->start = getstart(a);
    site->boot_key = a->app_key;
    G_INFO("handle_add_node calls site_install_action");
    site_install_action(site, a->body.c_t);
    return site;
  }
}

/**
 * Check if we can reconfigure the event horizon.
 *
 * We can reconfigure the event horizon if all group members support
 * reconfiguring the event horizon, and the new event horizon in the domain
 * [EVENT_HORIZON_MIN, EVENT_HORIZON_MAX].
 *
 * We use the group's latest common XCom protocol as a proxy to decide if all
 * members support reconfiguring the event horizon.
 *
 * If the common protocol is at least version 5 (x_1_4) then all members run
 * compatible server instances.
 *
 * Otherwise there are older instances, and it follows that the event horizon
 * must be the default and cannot be reconfigured.
 */
enum allow_event_horizon_result {
  EVENT_HORIZON_ALLOWED,
  EVENT_HORIZON_INVALID,
  EVENT_HORIZON_UNCHANGEABLE
};
typedef enum allow_event_horizon_result allow_event_horizon_result;

static void log_event_horizon_reconfiguration_failure(
    allow_event_horizon_result error_code,
    xcom_event_horizon attempted_event_horizon) {
  switch (error_code) {
    case EVENT_HORIZON_INVALID:
      G_WARNING("The event horizon was not reconfigured to %" PRIu32
                "because its domain is [%" PRIu32 ", %" PRIu32 "]",
                attempted_event_horizon, xcom_get_minimum_event_horizon(),
                xcom_get_maximum_event_horizon());
      break;
    case EVENT_HORIZON_UNCHANGEABLE:
      G_WARNING("The event horizon was not reconfigured to %" PRIu32
                " because some of the group's members do not support "
                "reconfiguring the event horizon",
                attempted_event_horizon);
      break;
    case EVENT_HORIZON_ALLOWED:
      break;
  }
}

static allow_event_horizon_result allow_event_horizon(
    xcom_event_horizon event_horizon) {
  if (event_horizon < EVENT_HORIZON_MIN || event_horizon > EVENT_HORIZON_MAX)
    return EVENT_HORIZON_INVALID;

  {
    const site_def *latest_config = get_site_def();
    if (!reconfigurable_event_horizon(latest_config->x_proto)) {
      assert(backwards_compatible(latest_config->event_horizon));
      return EVENT_HORIZON_UNCHANGEABLE;
    }
  }
  return EVENT_HORIZON_ALLOWED;
}

static bool_t unsafe_event_horizon_reconfiguration(app_data_ptr a) {
  assert(a->body.c_t == set_event_horizon_type);
  {
    xcom_event_horizon new_event_horizon = a->body.app_u_u.event_horizon;
    bool_t result = FALSE;
    allow_event_horizon_result error_code;
    error_code = allow_event_horizon(new_event_horizon);
    switch (error_code) {
      case EVENT_HORIZON_INVALID:
      case EVENT_HORIZON_UNCHANGEABLE:
        log_event_horizon_reconfiguration_failure(error_code,
                                                  new_event_horizon);
        result = TRUE;
        break;
      case EVENT_HORIZON_ALLOWED:
        break;
    }
    return result;
  }
}

static bool_t are_there_dead_nodes_in_new_config(app_data_ptr a) {
  assert(a->body.c_t == force_config_type);

  {
    u_int nr_nodes_to_add = a->body.app_u_u.nodes.node_list_len;
    node_address *nodes_to_change = a->body.app_u_u.nodes.node_list_val;
    uint32_t i;
    int silent = DEFAULT_DETECTOR_LIVE_TIMEOUT;
    if (the_app_xcom_cfg) {
      silent = the_app_xcom_cfg->m_flp_timeout;
    }
    G_DEBUG("Checking for dead nodes in Forced Configuration")
    for (i = 0; i < nr_nodes_to_add; i++) {
      node_no node = find_nodeno(get_site_def(), nodes_to_change[i].address);

      if (node == get_nodeno(get_site_def()))
        continue; /* No need to validate myself */

      if (node == VOID_NODE_NO) {
        G_ERROR(
            "%s is not in the current configuration."
            "Only members in the current configuration can be present"
            " in a forced configuration list",
            nodes_to_change[i].address)
        return TRUE;
      }

      if (may_be_dead(get_site_def()->detected, node, task_now(), silent,
                      get_site_def()->servers[node]->unreachable)) {
        G_ERROR(
            "%s is suspected to be failed."
            "Only alive members in the current configuration should be present"
            " in a forced configuration list",
            nodes_to_change[i].address)
        return TRUE;
      }
    }
  }

  return FALSE;
}

/**
 * Reconfigure the event horizon.
 *
 * It is possible that concurrent reconfigurations take effect between the
 * time this reconfiguration was proposed and now.
 *
 * Particularly, it is possible that any of the concurrent reconfigurations
 * added a new member which does not support reconfiguring the event
 * horizon.
 *
 * We account for these situations by validating if the event horizon
 * reconfiguration is still possible under the current state.
 *
 * If it is not, this reconfiguration does not produce any effect, i.e. no
 * new configuration is installed.
 */
bool_t handle_event_horizon(app_data_ptr a) {
  if (unsafe_event_horizon_reconfiguration(a)) return FALSE;

  {
    xcom_event_horizon new_event_horizon = a->body.app_u_u.event_horizon;
    const site_def *latest_config = get_site_def();
    site_def *new_config = clone_site_def(latest_config);
    IFDBG(D_NONE, FN; NDBG(new_event_horizon, u));
    IFDBG(D_NONE, FN; NDBG(new_event_horizon, u));
    ADD_DBG(D_BASE, add_event(EVENT_DUMP_PAD, string_arg("a->app_key"));
            add_synode_event(a->app_key););
    assert(get_site_def());
    assert(new_config);
    new_config->event_horizon = new_event_horizon;
    new_config->start = getstart(a);
    new_config->boot_key = a->app_key;
    site_install_action(new_config, a->body.c_t);
    G_INFO("The event horizon was reconfigured to %" PRIu32, new_event_horizon);
  }
  return TRUE;
}

void terminate_and_exit() {
  IFDBG(D_NONE, FN;);
  ADD_DBG(D_FSM, add_event(EVENT_DUMP_PAD, string_arg("terminating"));)
  XCOM_FSM(x_fsm_terminate, int_arg(0)); /* Tell xcom to stop */
  XCOM_FSM(x_fsm_exit, int_arg(0));      /* Tell xcom to exit */
  if (xcom_expel_cb) xcom_expel_cb(0);
}

static inline int is_empty_site(site_def const *s) {
  return s->nodes.node_list_len == 0;
}

site_def *handle_remove_node(app_data_ptr a) {
  site_def *site = clone_site_def(get_site_def());
  IFDBG(D_NONE, FN; COPY_AND_FREE_GOUT(dbg_list(&a->body.app_u_u.nodes)));
  ADD_DBG(D_BASE, add_event(EVENT_DUMP_PAD, string_arg("a->app_key"));
          add_synode_event(a->app_key);
          add_event(EVENT_DUMP_PAD, string_arg("nodeno"));
          add_event(EVENT_DUMP_PAD, uint_arg(get_nodeno(site))););

  remove_site_def(a->body.app_u_u.nodes.node_list_len,
                  a->body.app_u_u.nodes.node_list_val, site);
  site->start = getstart(a);
  site->boot_key = a->app_key;
  G_INFO("handle_remove_node calls site_install_action");
  site_install_action(site, a->body.c_t);
  return site;
}

static void log_ignored_forced_config(app_data_ptr a,
                                      char const *const caller_name) {
  switch (a->body.c_t) {
    case unified_boot_type:
      G_DEBUG("%s: Ignoring a forced intermediate, pending unified_boot",
              caller_name);
      break;
    case add_node_type:
      G_DEBUG("%s: Ignoring a forced intermediate, pending add_node for %s",
              caller_name, a->body.app_u_u.nodes.node_list_val[0].address);
      break;
    case remove_node_type:
      G_DEBUG("%s: Ignoring a forced intermediate, pending remove_node for %s",
              caller_name, a->body.app_u_u.nodes.node_list_val[0].address);
      break;
    case set_event_horizon_type:
      G_DEBUG(
          "%s: Ignoring a forced intermediate, pending set_event_horizon for "
          "%" PRIu32,
          caller_name, a->body.app_u_u.event_horizon);
      break;
    case force_config_type:
      G_DEBUG("%s: Ignoring a forced intermediate, pending force_config",
              caller_name);
      break;
    case abort_trans:
    case app_type:
    case begin_trans:
    case convert_into_local_server_type:
    case disable_arbitrator:
    case enable_arbitrator:
    case exit_type:
    case get_event_horizon_type:
    case get_synode_app_data_type:
    case prepared_trans:
    case remove_reset_type:
    case reset_type:
    case set_cache_limit:
    case set_flp_timeout:
    case view_msg:
    case x_terminate_and_exit:
    case xcom_boot_type:
    case xcom_set_group:
      // Meaningless for any other `cargo_type`s. Ignore.
      break;
  }
}

bool_t handle_config(app_data_ptr a, bool const forced) {
  assert(a->body.c_t == unified_boot_type ||
         a->next == NULL); /* Reconfiguration commands are not batched. */
  {
    bool_t success = FALSE;
    if (forced &&
        should_ignore_forced_config_or_view(get_executor_site()->x_proto)) {
      log_ignored_forced_config(a, "handle_config");
      goto end;
    }
    switch (a->body.c_t) {
      case unified_boot_type:
        success = (install_node_group(a) != NULL);
        assert(success);
        break;
      case add_node_type:
        /*
         * May fail if meanwhile the event horizon was reconfigured and the
         * node is incompatible.
         */
        success = (handle_add_node(a) != NULL);
        break;
      case remove_node_type:
        ADD_DBG(D_BASE,
                add_event(EVENT_DUMP_PAD, string_arg("got remove_node_type"));)
        success = (handle_remove_node(a) != NULL);
        assert(success);
        break;
      case set_event_horizon_type:
        /* May fail if meanwhile an incompatible node joined. */
        success = handle_event_horizon(a);
        break;
      case force_config_type:
        success = (install_node_group(a) != NULL);
        assert(success);
        break;
      default:
        assert(FALSE); /* Boy oh boy, something is really wrong... */
        break;
    }
  end:
    return success;
  }
}

static inline int is_member(site_def const *site) {
  return site->nodeno != VOID_NODE_NO;
}

/*
Execute xcom message stream.

Beware  of  the exit logic in this task, which is both simple and
not so simple.  Consider three configs C1  and  C2.  C1  has  two
nodes,  A and B. C2 has only node B.  C3 is empty.  A config with
message number N will be activated after a delay  of  (at  least)
alpha  messages,  where alpha is the size of the pipeline (or the
event horizon).

So, C1.start = C1+alpha, and C2.start = C2+alpha. A, which is re‐
moved  from  C1, cannot exit until a majority of nodes in the new
config C2 (in this case B) has learned all the messages from con‐
fig  C1,  which  means all messages less than C2.start. How can A
know that a majority of C2 has learned those messages?

If we denote the first message that is not yet decided (and  exe‐
cuted)  by E, the proposers will not try to propose messages with
number >= E+alpha, and all incoming  tcp  messages  with  message
number  >=  E+alpha will be ignored.  E is incremented by the ex‐
ecutor task, so all messages < E are known.  This means that when
the value of E+alpha is known, all messages up to and including E
are also known, although not all messages  E+1..E+alpha‐1  neces‐
sarily are known.

This  leads  to  the requirement that a node which is removed (A)
needs to wait until it knows the value of  C2.start+alpha,  since
by  then it knows that a majority of the nodes in C2 are ready to
execute C2.start, which in turn implies that a majority of  nodes
in  C2  knows  all  the values from config C1. Note that the last
message that should be delivered to the  application  by  a  node
that  is  leaving  C1 is C2.start‐1, which is the last message of
C1.

How does a node that is removed get to know values from the  next
config?   There  are  two  ways, and we use both. First, the node
that tries to exit can simply ask for the message.  get_xcom_mes‐
sage()  will  do  this for all messages <= max_synode, but it may
take some time.  Second, the nodes of C2 can  send  the  messages
C2.start..C2.start+alpha  to  the  nodes  that are removed (nodes
that are in C1 but not in C2).  inform_removed()  does  this.  We
take  care to handle the case where configs are close enough that
C0 < C1 <= C0+alpha by tracking the oldest config  that  contains
nodes that are leaving.

This  takes care of nodes leaving C1. What about nodes that leave
C2? C3 is empty, so B, which is leaving C2, cannot wait for  mes‐
sages  from  C3. But since C3 is empty, there is no need to wait.
It can exit immediately after  having  executed  C3.start‐1,  the
last message of C2. What if C3.start‐1 < C2.start+alpha? This can
happen if C2 and C3 are close. In that case, B will exit before A
gets the chance to learn C2.start+alpha, which will leave A hang‐
ing forever. Clearly, we need to impose an additional constraint,
that  C3.start must be greater than C2.start+alpha. This is taken
care of by the special test for an empty config.

Complicated and confusing? Not really, but there is a  clean  and
simple  solution which has not been implemented yet, since it re‐
quires more changes to the consensus logic.  If we  require  that
for  the messages C2..C2.start‐1 we have a majority from both the
nodes in C1 and the nodes in C2, the nodes not  in  C2  can  exit
when  they  have  executed message C2.start‐1, since we then know
that a majority of the nodes of C2 has agreed on  those  messages
as  well,  so they do not depend on the nodes not in C2 any more.
This holds even if C2 is empty.  Note that requiring  a  majority
from  both  C1 and C2 is different from requiring a majority from
C1+C2, which means that the proposer logic needs to consider  an‐
swers  from  two  different sets of acceptors for those messages.
Since acceptors are identified by their node number, and the node
numbers  need  not be the same for both configs, we need to main‐
tain a mapping between the nodes numbers of any  two  consecutive
configs.  Alternatively,  we  could remove the node numbers alto‐
gether, and always use a unique, unchanging ID for a  node,  like
IP address + port.

TODO:

Move the delayed delivery logic into MCM-specific code, since it is
only needed by MCM.  Is it still needed?

Rewrite exit logic as FSM with more states. (RUN, EMPTY_EXIT,
NOT_MEMBER_EXIT) to avoid unnecessary tests.

*/

/* FIFO which tracks the message numbers where we should deliver queued messages
or
inform the removed nodes */
#define FIFO_SIZE 1000
static struct {
  int n;
  int front;
  int rear;
  synode_no q[FIFO_SIZE];
} delay_fifo;

static inline int addone(int i) { return ((i + 1) % FIFO_SIZE); }

/* Is queue empty?  */
static inline int fifo_empty() { return delay_fifo.n <= 0; }

/* Is queue full?  */
static inline int fifo_full() { return delay_fifo.n >= FIFO_SIZE; }

/* Insert in queue  */
static inline void fifo_insert(synode_no s) {
  if (!fifo_full()) {
    delay_fifo.n++;
    delay_fifo.q[delay_fifo.rear] = s;
    delay_fifo.rear = addone(delay_fifo.rear);
  }
}

/* Extract first from queue  */
static inline synode_no fifo_extract() {
  if (!fifo_empty()) {
    synode_no ret = delay_fifo.q[delay_fifo.front];
    delay_fifo.front = addone(delay_fifo.front);
    delay_fifo.n--;
    return ret;
  } else {
    return null_synode;
  }
}

/* Return first in queue, but do not dequeue  */
static inline synode_no fifo_front() {
  if (!fifo_empty()) {
    return delay_fifo.q[delay_fifo.front];
  } else {
    return null_synode;
  }
}

struct execute_context;
typedef struct execute_context execute_context;

typedef void (*exec_fp)(execute_context *xc);

struct execute_context {
  pax_machine *p;
  int n;
  int old_n;
  double old_t;
  synode_no exit_synode;
  synode_no delivery_limit;
  exec_fp state;
  int exit_flag; /* To avoid state explosion */
  int inform_index;
};

static void dump_exec_state(execute_context *xc, long dbg);
static int x_check_exit(execute_context *xc);
static int x_check_execute_inform(execute_context *xc);
static void x_fetch(execute_context *xc);
static void x_execute(execute_context *xc);
static void x_check_increment_fetch(execute_context *xc);
static void x_check_increment_execute(execute_context *xc);
static void x_terminate(execute_context *xc);

struct fp_name {
  exec_fp fp;
  char const *name;
};

#define NAME(f) \
  { f, #f }

/* List of fp, name pairs */
static struct fp_name oblist[] = {
    NAME(x_fetch), NAME(x_execute), NAME(x_terminate), {0, 0}};
#undef NAME

/* purecov: begin deadcode */
char const *get_fp_name(exec_fp fp) {
  struct fp_name *list = oblist;
  while (list->fp) {
    if (list->fp == fp) return list->name;
    list++;
  }
  return "no such fp";
}
/* purecov: end */

static void setup_exit_handling(execute_context *xc, site_def *site) {
  synode_no delay_until;
  if (is_member(site)) {
    delay_until = compute_delay(site->start, site->event_horizon);
  } else { /* Not in this site */
    /* See if site will be empty when we leave. If the new site
     * is empty, we should exit after having delivered the last
     * message from the old site. */

    /* Note limit of delivery. We should never deliver anything after the start
     * of the next site. */
    xc->delivery_limit = site->start;

    /* If we are not a member of the new site, we should exit
      after having seen enough messages beyond the end of the current site.
      This ensures that a majority of the next site will have agreed upon all
      messages that belong to the current site.
     */
    xc->exit_synode = compute_delay(site->start, site->event_horizon);
    if (is_empty_site(site)) {
      /* If site is empty, increase start to allow nodes to terminate before
       * start. This works as if there was a non-empty group after the
       * exit_synode, effectively allowing the majority of the current group to
       * agree on all messages up to exit_synode.
       */
      site->start = compute_delay(
          compute_delay(site->start, site->event_horizon), site->event_horizon);
    }
    if (!synode_lt(xc->exit_synode, max_synode)) {
      /* We need messages from the next site, so set max_synode accordingly. */
      set_max_synode(incr_synode(xc->exit_synode));
    }
    /* Note where we switch to execute and inform removed nodes */
    delay_until = xc->exit_synode;

    IFDBG(D_EXEC, FN; SYCEXP(delay_until); SYCEXP(executed_msg);
          SYCEXP(max_synode));
    IFDBG(D_EXEC, FN; SYCEXP(xc->exit_synode); SYCEXP(executed_msg);
          SYCEXP(max_synode));

    /* Note that we will exit */
    xc->exit_flag = 1;
  }

  /* Ensure that max_synode is greater than trigger for delivery
   */
  if (synode_gt(delay_until, max_synode))
    set_max_synode(incr_msgno(delay_until));
  fifo_insert(delay_until);
  (xc->inform_index)++;

  /* If I am the leader, will propose no-ops until current max_synode
   */
}

/* Called immediately after we have got a new message.
   Terminate if we have no site.
   Otherwise, handle config messages immediately.
   Afterwards, switch to check_exit_fetch. */
static void x_fetch(execute_context *xc) {
  /* Execute unified_boot immediately, but do not deliver site message
   * until we are ready to execute messages from the new site
   * definition. At that point we can be certain that a majority have
   * learned everything from the old site. */

  app_data *app = xc->p->learner.msg->a;
  if (app && is_config(app->body.c_t) &&
      synode_gt(executed_msg, get_site_def()->boot_key)) /* Redo test */
  {
    site_def *site = 0;
    bool_t reconfiguration_successful =
        handle_config(app, (xc->p->learner.msg->force_delivery != 0));
    if (reconfiguration_successful) {
      /* If the reconfiguration failed then it does not have any
       * effect. What follows only makes sense if the reconfiguration
       * took effect. */
      set_last_received_config(executed_msg);
      garbage_collect_site_defs(delivered_msg);
      site = get_site_def_rw();
      if (site == 0) {
        xc->state = x_terminate;
        return;
      }
      IFDBG(D_EXEC, FN; STRLIT("new config "); SYCEXP(site->boot_key););

      if (xc->exit_flag == 0) {
        /* We have not yet set the exit trigger */
        setup_exit_handling(xc, site);
      }
    }
  } else {
    IFDBG(D_EXEC, FN; SYCEXP(executed_msg); SYCEXP(get_site_def()->boot_key));
  }
  /* Check for exit and increment executed_msg */
  x_check_increment_fetch(xc);
}

/* Push messages to nodes that have been removed.
   Signal switch to execute when nothing left to push by returning 1 */
static int x_check_execute_inform(execute_context *xc) {
  IFDBG(D_EXEC, FN; SYCEXP(fifo_front()); SYCEXP(executed_msg);
        SYCEXP(xc->exit_synode); NDBG(xc->exit_flag, d));
  if (fifo_empty()) {
    return 1;
  } else if (!synode_lt(executed_msg, fifo_front())) {
    while (
        !fifo_empty() &&
        !synode_lt(executed_msg, fifo_front())) { /* More than one may match */
      inform_removed(xc->inform_index, 0);
      fifo_extract();
      (xc->inform_index)--;
    }
    garbage_collect_servers();
    return 1;
  }
  dump_exec_state(xc, D_EXEC);
  return 0;
}

/* Check for exit and return 1 if we should exit. */
static int x_check_exit(execute_context *xc) {
  /* See if we should exit when having seen this message */
  return (xc->exit_flag && !synode_lt(executed_msg, xc->exit_synode) &&
          !synode_lt(delivered_msg, xc->delivery_limit));
}

/* Terminate if we should exit, else increment executed_msg and see if we should
 * switch to execute */
static void x_check_increment_fetch(execute_context *xc) {
  if (x_check_exit(xc)) {
    xc->state = x_terminate;
  } else {
    SET_EXECUTED_MSG(incr_synode(executed_msg));
    if (x_check_execute_inform(xc)) {
      xc->state = x_execute;
    }
  }
}

/* Terminate if we should exit, else increment delivered_msg and see if we
 * should switch to fetch */
static void x_check_increment_execute(execute_context *xc) {
  if (x_check_exit(xc)) {
    xc->state = x_terminate;
  } else {
    /* Increment delivered_msg and switch to fetch if delivered_msg equals
     * executed_msg; */
    delivered_msg = incr_synode(delivered_msg);
    if (synode_eq(delivered_msg, executed_msg)) {
      xc->state = x_fetch;
    }
  }
}

/* Deliver one message if it should be delivered. Switch state to see if
   we should exit */
static void x_execute(execute_context *xc) {
  site_def const *x_site = find_site_def(delivered_msg);

  IFDBG(D_EXEC, FN; SYCEXP(delivered_msg); SYCEXP(delivered_msg);
        SYCEXP(executed_msg); SYCEXP(xc->exit_synode); NDBG(xc->exit_flag, d));
  if (!is_cached(delivered_msg)) {
    /* purecov: begin deadcode */
#ifdef TASK_EVENT_TRACE
    dump_task_events();
#endif
    /* purecov: end */
  }
  assert(is_cached(delivered_msg) && "delivered_msg should have been cached");
  xc->p = get_cache(delivered_msg);
  if (LOSER(delivered_msg, x_site)) {
#ifdef IGNORE_LOSERS
    IFDBG(D_EXEC, FN; debug_loser(delivered_msg); PTREXP(x_site);
          dbg_node_set(x_site->global_node_set));
#endif
  } else if (xc->p->learner.msg->msg_type != no_op) {
    /* Avoid delivery after start if we should exit */
    if (xc->exit_flag == 0 || synode_lt(delivered_msg, xc->delivery_limit)) {
      /* IFDBG(D_EXEC, FN; NDBG(ep->state, d); STRLIT("executing ");
         SYCEXP(delivered_msg); SYCEXP(executed_msg);
              SYCEXP(xc->delivery_limit); NDBG(xc->exit_flag, d)); */
      last_delivered_msg = delivered_msg;
      execute_msg(find_site_def_rw(delivered_msg), xc->p, xc->p->learner.msg);
    }
  }
  /* Garbage collect old servers */
  if (synode_eq(delivered_msg, x_site->start)) {
    garbage_collect_servers();
  }
#if defined(TASK_DBUG_ON) && TASK_DBUG_ON
  IFDBG(D_EXEC, perf_dbg(&xc->n, &xc->old_n, &xc->old_t));
#endif
  /* Check for exit and increment delivered_msg */
  x_check_increment_execute(xc);
}

static execute_context *debug_xc;

static void dump_exec_state(execute_context *xc MY_ATTRIBUTE((unused)),
                            long dbg MY_ATTRIBUTE((unused))) {
  IFDBG(dbg, FN; SYCEXP(executed_msg); SYCEXP(delivered_msg);
        SYCEXP(max_synode); SYCEXP(last_delivered_msg); NDBG(delay_fifo.n, d);
        NDBG(delay_fifo.front, d); NDBG(delay_fifo.rear, d);
        SYCEXP(fifo_front()); SYCEXP(xc->exit_synode);
        SYCEXP(xc->delivery_limit); NDBG(xc->exit_flag, d);
        NDBG(xc->inform_index, d); NDBG(prop_started, d);
        NDBG(prop_finished, d););
}

static void dump_debug_exec_state() {
  if (debug_xc) dump_exec_state(debug_xc, D_EXEC);
}

/* Terminate the excutor_task. */
static void x_terminate(execute_context *xc) {
  dump_exec_state(xc, D_BUG);
  xc->state = 0;
}

static int executor_task(task_arg arg MY_ATTRIBUTE((unused))) {
  DECL_ENV
  execute_context xc;
  END_ENV;
  /* xcom_debug_mask = D_BUG; */
  IFDBG(D_EXEC, FN; NDBG(stack->sp->state, d); SYCEXP(executed_msg););
  TASK_BEGIN
  ep->xc.p = NULL;
  ep->xc.n = 0;
  ep->xc.old_n = 0;
  ep->xc.old_t = task_now();
  ep->xc.exit_synode = null_synode;
  ep->xc.delivery_limit = null_synode;
  ep->xc.exit_flag = 0;
  ep->xc.inform_index = -1;
  delay_fifo.n = 0;
  delay_fifo.front = 0;
  delay_fifo.rear = 0;
  debug_xc = &ep->xc;

  if (executed_msg.msgno == 0) executed_msg.msgno = 1;
  delivered_msg = executed_msg;
  ep->xc.state = x_fetch;
  executor_site = find_site_def_rw(executed_msg);

  /* The following loop implements a state machine based on function pointers,
     effectively acting as non-local gotos.
     The functions all operate on data in the execution context xc, and
     switch state by setting xc->state to the function corresponding to the new
     state.
  */
  while (!xcom_shutdown && ep->xc.state != 0) {
    IFDBG(D_EXEC, FN; STRLIT(get_fp_name(ep->xc.state)););
    if (ep->xc.state == x_fetch) { /* Special case because of task macros */
      if (LOSER(executed_msg, executor_site)) {
        x_check_increment_fetch(&ep->xc); /* Just increment past losers */
      } else {
        TASK_CALL(get_xcom_message(&ep->xc.p, executed_msg, FIND_MAX));
        IFDBG(D_EXEC, FN; STRLIT("got message "); SYCEXP(ep->xc.p->synode);
              COPY_AND_FREE_GOUT(dbg_app_data(ep->xc.p->learner.msg->a)));
        x_fetch(&ep->xc);
      }
    } else {
      ep->xc.state(&ep->xc);
    }
  }

  /* Inform all removed nodes before we exit */
  ADD_DBG(D_FSM, add_event(EVENT_DUMP_PAD, string_arg("terminating"));)
  inform_removed(ep->xc.inform_index, 1);
  dump_exec_state(&ep->xc, D_BUG);

#ifndef NO_DELAYED_TERMINATION
  IFDBG(D_EXEC, FN; STRLIT("delayed terminate and exit"));

  /* Wait to allow messages to propagate */
  TASK_DELAY(TERMINATE_DELAY);

  /* Start termination of xcom */
  terminate_and_exit();
#endif

  FINALLY
  dump_exec_state(&ep->xc, D_BUG);
  IFDBG(D_BUG, FN; STRLIT(" shutdown "); SYCEXP(executed_msg);
        NDBG(task_now(), f));
  TASK_END;
}

static synode_no get_sweep_start() {
  synode_no find = executed_msg;
  find.node = get_nodeno(find_site_def(find));
  if (find.node < executed_msg.node) {
    find = incr_msgno(find);
  }
  return find;
}

static int sweeper_task(task_arg arg MY_ATTRIBUTE((unused))) {
  DECL_ENV
  synode_no find;
  END_ENV;

  TASK_BEGIN

  ep->find = get_sweep_start();

  while (!xcom_shutdown) {
    ep->find.group_id =
        executed_msg.group_id; /* In case group id has changed */
#ifndef AGGRESSIVE_SWEEP
    while (!is_only_task()) {
      TASK_YIELD;
    }
#endif
    ADD_DBG(D_NONE, add_event(EVENT_DUMP_PAD, string_arg("sweeper ready"));
            add_synode_event(executed_msg););
    /*		IFDBG(D_NONE, FN; STRLIT("ready to run ");   */
    /*			SYCEXP(executed_msg); SYCEXP(max_synode);
     * SYCEXP(ep->find));
     */
    {
      while (synode_lt(ep->find, max_synode) && !too_far(ep->find)) {
        /* pax_machine * pm = hash_get(ep->find); */
        pax_machine *pm = 0;
        ADD_DBG(D_NONE,
                add_event(EVENT_DUMP_PAD, string_arg("sweeper examining"));
                add_synode_event(ep->find););
        if (ep->find.node == VOID_NODE_NO) {
          if (synode_gt(executed_msg, ep->find)) {
            ep->find = get_sweep_start();
          }
          if (ep->find.node == VOID_NODE_NO) goto deactivate;
        }
        pm = get_cache(ep->find);
        ADD_DBG(D_CONS,
                add_event(EVENT_DUMP_PAD, string_arg("sweeper checking"));
                add_synode_event(ep->find);
                add_event(EVENT_DUMP_PAD, string_arg(pax_op_to_str(pm->op)));
                add_event(EVENT_DUMP_PAD, string_arg("pm"));
                add_event(EVENT_DUMP_PAD, void_arg(pm)););
        if (pm && !pm->force_delivery) { /* We want full 3 phase Paxos for
                                            forced messages */
          ADD_DBG(
              D_CONS, add_event(EVENT_DUMP_PAD, string_arg("sweeper checking"));
              add_synode_event(ep->find);
              add_event(EVENT_DUMP_PAD, string_arg(pax_op_to_str(pm->op)));
              add_event(EVENT_DUMP_PAD, string_arg("is_busy_machine"));
              add_event(EVENT_DUMP_PAD, int_arg(is_busy_machine(pm)));
              add_event(EVENT_DUMP_PAD, string_arg("pm->acceptor.promise.cnt"));
              add_event(EVENT_DUMP_PAD, int_arg(pm->acceptor.promise.cnt));
              add_event(EVENT_DUMP_PAD, string_arg("finished(pm)"));
              add_event(EVENT_DUMP_PAD, int_arg(finished(pm)));
              add_event(EVENT_DUMP_PAD, string_arg("pm->acceptor.msg"));
              add_event(EVENT_DUMP_PAD, void_arg(pm->acceptor.msg)););
          /* IFDBG(D_NONE, FN; dbg_pax_machine(pm)); */
          if (!is_busy_machine(pm) && pm->acceptor.promise.cnt == 0 &&
              !pm->acceptor.msg && !finished(pm)) {
            pm->op = skip_op;
            ADD_DBG(D_CONS,
                    add_event(EVENT_DUMP_PAD, string_arg("sweeper skipping"));
                    add_synode_event(ep->find); add_event(
                        EVENT_DUMP_PAD, string_arg(pax_op_to_str(pm->op))););
            skip_msg(pax_msg_new(ep->find, find_site_def(ep->find)));
            IFDBG(D_NONE, FN; STRLIT("skipping "); SYCEXP(ep->find));
            /* 						IFDBG(D_NONE, FN;
             * dbg_pax_machine(pm));
             */
          }
        }
        ep->find = incr_msgno(ep->find);
      }
    }
  deactivate:
    TASK_DEACTIVATE;
  }
  FINALLY
  IFDBG(D_BUG, FN; STRLIT(" shutdown sweeper "); SYCEXP(executed_msg);
        NDBG(task_now(), f));
  TASK_END;
}

static double wakeup_delay(const site_def *site, double old) {
  double retval = 0.0;
  if (0.0 == old) {
    retval = 0.001 + site->max_conn_rtt;
  } else {
    retval = old * 1.4; /* Exponential backoff */
  }

  {
    double const minimum_threshold = 0.005;
    double maximum_threshold = 0.500;
    double candidate_threshold = site->max_conn_rtt * 10;
    if (candidate_threshold < minimum_threshold) {
      candidate_threshold = minimum_threshold;
    }

    if (candidate_threshold < maximum_threshold) {
      maximum_threshold = candidate_threshold;
    }

    while (retval > maximum_threshold) retval /= 1.3;
  }
  /* IFDBG(D_NONE, FN; NDBG(retval,d)); */
  return retval;
}

static double wakeup_delay_for_perf(double old) {
  double retval = 0.0;
  if (0.0 == old) {
    double m = median_time() / 100;
    if (m == 0.0 || m > 0.003) m = 0.001;
    retval = 0.001 + 5.0 * m + m * xcom_drand48();
  } else {
    retval = old * 1.4142136; /* Exponential backoff */
  }

  {
    /* Set max value to be 3 millisecond */
    double const maximum_threshold = 0.003;
    while (retval > maximum_threshold) retval /= 1.31415926;
  }
  /* IFDBG(D_NONE, FN; NDBG(retval,d)); */
  return retval;
}

static void propose_noop(synode_no find, pax_machine *p) {
  /* Prepare to send a noop */
  site_def const *site = find_site_def(find);
  IFDBG(D_NONE, FN; SYCEXP(find); SYCEXP(executed_msg));
  assert(!too_far(find));
  replace_pax_msg(&p->proposer.msg, pax_msg_new(find, site));
  assert(p->proposer.msg);
  create_noop(p->proposer.msg);
  {
    pax_msg *clone = clone_pax_msg(p->proposer.msg);
    if (clone != NULL) {
      push_msg_3p(site, p, clone, find, no_op);
    } else {
      /* purecov: begin inspected */
      G_DEBUG("Unable to propose NoOp due to an OOM error.");
      /* purecov: end */
    }
  }
}

static void send_read(synode_no find) {
  /* Prepare to send a read_op */
  site_def const *site = find_site_def(find);

  IFDBG(D_NONE, FN; NDBG(get_maxnodes(site), u); NDBG(get_nodeno(site), u););
  ADD_DBG(D_CONS, add_event(EVENT_DUMP_PAD, string_arg("find"));
          add_synode_event(find); add_event(EVENT_DUMP_PAD, string_arg("site"));
          add_event(EVENT_DUMP_PAD, void_arg((void *)find_site_def_rw(find)));
          add_event(EVENT_DUMP_PAD, string_arg("get_nodeno(site)"));
          add_event(EVENT_DUMP_PAD, uint_arg(get_nodeno(site))););

  /* See if node number matches ours */
  if (site) {
    if (find.node != get_nodeno(site)) {
      pax_msg *pm = pax_msg_new(find, site);
      ref_msg(pm);
      create_read(site, pm);
      IFDBG(D_NONE, FN; SYCEXP(find););

      IFDBG(D_NONE, FN; NDBG(get_maxnodes(site), u); NDBG(get_nodeno(site), u);
            PTREXP(pm));
/* send_server_msg(site, find.node, pm); */
#if 0
			send_to_others(site, pm, "send_read");
#else
      /* If we have no node number,  ask all the others */
      if (get_nodeno(site) == VOID_NODE_NO)
        send_to_others(site, pm, "send_read");
      else
        /* Ask a random node */
        send_to_someone(site, pm, "send_read");
#endif
      unref_msg(&pm);
    } else { /* If node number matches our own number, ask all the others */
      pax_msg *pm = pax_msg_new(find, site);
      ref_msg(pm);
      create_read(site, pm);
      send_to_others(site, pm, "send_read");
      unref_msg(&pm);
    }
  }
}

/* Find missing values */

static int ok_to_propose(pax_machine *p) {
  int retval = (is_forcing_node(p) || !recently_active(p)) && !finished(p) &&
               !is_busy_machine(p);
  IFDBG(D_NONE, FN; NDBG(p->synode.node, u); NDBG(recently_active(p), d);
        NDBG(finished(p), d); NDBG(is_busy_machine(p), d); NDBG(retval, d));
  return retval;
}

static void read_missing_values(int n) {
  synode_no find = executed_msg;
  synode_no end = max_synode;
  int i = 0;

  IFDBG(D_NONE, FN; SYCEXP(find); SYCEXP(end));
  if (synode_gt(executed_msg, max_synode) ||
      synode_eq(executed_msg, null_synode))
    return;

  while (!synode_gt(find, end) && i < n && !too_far(find)) {
    pax_machine *p = force_get_cache(find);
    ADD_DBG(D_NONE, add_synode_event(find); add_synode_event(end);
            add_event(EVENT_DUMP_PAD, string_arg("active "));
            add_event(EVENT_DUMP_PAD, int_arg(recently_active(p)));
            add_event(EVENT_DUMP_PAD, string_arg("finished  "));
            add_event(EVENT_DUMP_PAD, int_arg(finished(p)));
            add_event(EVENT_DUMP_PAD, string_arg("busy "));
            add_event(EVENT_DUMP_PAD, int_arg(is_busy_machine(p))););
    IFDBG(D_NONE, FN; SYCEXP(find); SYCEXP(end); NDBG(recently_active(p), d);
          NDBG(finished(p), d); NDBG(is_busy_machine(p), d));
    if (!recently_active(p) && !finished(p) && !is_busy_machine(p)) {
      send_read(find);
    }
    find = incr_synode(find);
    i++;
  }
}

static void propose_missing_values(int n) {
  synode_no find = executed_msg;
  synode_no end = max_synode;
  int i = 0;

  IFDBG(D_NONE, FN; NDBG(get_maxnodes(get_site_def()), u); SYCEXP(find);
        SYCEXP(end));
  if (synode_gt(executed_msg, max_synode) ||
      synode_eq(executed_msg, null_synode))
    return;

  IFDBG(D_NONE, FN; SYCEXP(find); SYCEXP(end));
  i = 0;
  while (!synode_gt(find, end) && i < n && !too_far(find)) {
    pax_machine *p = force_get_cache(find);
    if (wait_forced_config) {
      force_pax_machine(p, 1);
    }
    IFDBG(D_NONE, FN; NDBG(ok_to_propose(p), d); TIMECEXP(task_now());
          TIMECEXP(p->last_modified); SYCEXP(find));
    if (get_nodeno(find_site_def(find)) == VOID_NODE_NO) break;
    if (ok_to_propose(p)) {
      propose_noop(find, p);
    }
    find = incr_synode(find);
    i++;
  }
}

/* Propose a noop for the range find..end */
void request_values(synode_no find, synode_no end) {
  IFDBG(D_NONE, FN; SYCEXP(find); SYCEXP(end););
  while (!synode_gt(find, end) && !too_far(find)) {
    pax_machine *p = get_cache(find);
    site_def const *site = find_site_def(find);
    if (get_nodeno(site) == VOID_NODE_NO) break;
    if (!finished(p) && !is_busy_machine(p)) {
      /* Prepare to send a noop */
      replace_pax_msg(&p->proposer.msg, pax_msg_new(find, site));
      assert(p->proposer.msg);
      create_noop(p->proposer.msg);

      IFDBG(D_NONE, FN; STRLIT("propose "); SYCEXP(find););
      push_msg_3p(site, p, pax_msg_new(find, site), find, no_op);
    }
    find = incr_synode(find);
  }
}

/* Message handlers */

/*
Reply to the sender of a message.
Avoid using the outbound TCP connection to the node that sent the message, since
it is simpler and safer to always use the same TCP connection as the one the
message arrived on. We then know that the answever will always go to the same
client (and the same instance of that client) that sent the request.
*/
#define reply_msg(m)                                              \
  {                                                               \
    if (is_local_node((m)->from, site)) {                         \
      dispatch_op(site, m, NULL);                                 \
    } else {                                                      \
      link_into(&(msg_link_new((m), (m)->from)->l), reply_queue); \
    }                                                             \
  }

#define CREATE_REPLY(x)  \
  pax_msg *reply = NULL; \
  CLONE_PAX_MSG(reply, x)

#define SEND_REPLY  \
  reply_msg(reply); \
  replace_pax_msg(&reply, NULL)

bool_t safe_app_data_copy(pax_msg **target, app_data_ptr source) {
  copy_app_data(&(*target)->a, source);
  if ((*target)->a == NULL && source != NULL) {
    oom_abort = 1;
    replace_pax_msg(target, NULL);
    return FALSE;
  }
  return TRUE;
}

static pax_msg *create_learn_msg_for_ignorant_node(pax_machine *p, pax_msg *pm,
                                                   synode_no synode) {
  CREATE_REPLY(pm);
  IFDBG(D_NONE, FN; SYCEXP(synode));
  reply->synode = synode;
  reply->proposal = p->learner.msg->proposal;
  reply->msg_type = p->learner.msg->msg_type;
  safe_app_data_copy(&reply, p->learner.msg->a);
  if (reply != NULL) set_learn_type(reply);
  /* set_unique_id(reply, p->learner.msg->unique_id); */
  return reply;
}

static void teach_ignorant_node(site_def const *site, pax_machine *p,
                                pax_msg *pm, synode_no synode,
                                linkage *reply_queue) {
  pax_msg *reply = create_learn_msg_for_ignorant_node(p, pm, synode);
  if (reply != NULL) SEND_REPLY;
}

/* Handle incoming read */
static void handle_read(site_def const *site, pax_machine *p,
                        linkage *reply_queue, pax_msg *pm) {
  IFDBG(D_NONE, FN; BALCEXP(pm->proposal); BALCEXP(p->acceptor.promise);
        if (p->acceptor.msg) BALCEXP(p->acceptor.msg->proposal);
        STRLIT("type "); STRLIT(pax_msg_type_to_str(pm->msg_type)));

  if (finished(p)) { /* We have learned a value */
    teach_ignorant_node(site, p, pm, pm->synode, reply_queue);
  }
}

static pax_msg *create_ack_prepare_msg(pax_machine *p, pax_msg *pm,
                                       synode_no synode) {
  CREATE_REPLY(pm);
  reply->synode = synode;
  if (accepted(p)) { /* We have accepted a value */
    reply->proposal = p->acceptor.msg->proposal;
    reply->msg_type = p->acceptor.msg->msg_type;
    IFDBG(D_NONE, FN; STRLIT(" already accepted value "); SYCEXP(synode));
    reply->op = ack_prepare_op;
    safe_app_data_copy(&reply, p->acceptor.msg->a);
  } else {
    IFDBG(D_NONE, FN; STRLIT(" no value synode "); SYCEXP(synode));
    reply->op = ack_prepare_empty_op;
  }
  return reply;
}

pax_msg *handle_simple_prepare(pax_machine *p, pax_msg *pm, synode_no synode) {
  pax_msg *reply = NULL;
  if (finished(p)) { /* We have learned a value */
    IFDBG(D_NONE, FN; SYCEXP(synode); BALCEXP(pm->proposal);
          NDBG(finished(p), d));
    reply = create_learn_msg_for_ignorant_node(p, pm, synode);
  } else {
    int greater =
        gt_ballot(pm->proposal,
                  p->acceptor.promise); /* Paxos acceptor phase 1 decision */
    IFDBG(D_NONE, FN; SYCEXP(synode); BALCEXP(pm->proposal); NDBG(greater, d));
    if (greater || noop_match(p, pm)) {
      p->last_modified = task_now();
      if (greater) {
        p->acceptor.promise = pm->proposal; /* promise to not accept any less */
      }
      reply = create_ack_prepare_msg(p, pm, synode);
    }
  }
  return reply;
}

/* Handle incoming prepare */
static void handle_prepare(site_def const *site, pax_machine *p,
                           linkage *reply_queue, pax_msg *pm) {
  ADD_DBG(D_CONS, add_synode_event(p->synode);
          add_event(EVENT_DUMP_PAD, string_arg("pm->from"));
          add_event(EVENT_DUMP_PAD, uint_arg(pm->from));
          add_event(EVENT_DUMP_PAD, string_arg(pax_op_to_str(pm->op)));
          add_event(EVENT_DUMP_PAD, string_arg("proposal"));
          add_ballot_event(pm->proposal);
          add_event(EVENT_DUMP_PAD, string_arg("promise"));
          add_ballot_event(p->acceptor.promise););
  IFDBG(D_NONE, FN; BALCEXP(pm->proposal); BALCEXP(p->acceptor.promise);
        if (p->acceptor.msg) BALCEXP(p->acceptor.msg->proposal);
        STRLIT("type "); STRLIT(pax_msg_type_to_str(pm->msg_type)));

  {
    pax_msg *reply = handle_simple_prepare(p, pm, pm->synode);
    if (reply != NULL) SEND_REPLY;
  }
}

bool_t check_propose(site_def const *site, pax_machine *p) {
  IFDBG(D_NONE, FN; SYCEXP(p->synode);
        COPY_AND_FREE_GOUT(dbg_machine_nodeset(p, get_maxnodes(site))););
  PAX_MSG_SANITY_CHECK(p->proposer.msg);
  {
    bool_t can_propose = FALSE;
    if (prep_majority(site, p)) {
      p->proposer.msg->proposal = p->proposer.bal;
      BIT_ZERO(p->proposer.prop_nodeset);
      p->proposer.msg->synode = p->synode;
      init_propose_msg(p->proposer.msg);
      p->proposer.sent_prop = p->proposer.bal;
      can_propose = TRUE;
    }
    return can_propose;
  }
}

static pax_msg *check_learn(site_def const *site, pax_machine *p) {
  IFDBG(D_NONE, FN; SYCEXP(p->synode);
        COPY_AND_FREE_GOUT(dbg_machine_nodeset(p, get_maxnodes(site))););
  PAX_MSG_SANITY_CHECK(p->proposer.msg);
  {
    pax_msg *learn_msg = NULL;
    if (get_nodeno(site) != VOID_NODE_NO && prop_majority(site, p)) {
      p->proposer.msg->synode = p->synode;
      if (p->proposer.msg->receivers) free_bit_set(p->proposer.msg->receivers);
      p->proposer.msg->receivers = clone_bit_set(p->proposer.prep_nodeset);
      BIT_SET(get_nodeno(site), p->proposer.msg->receivers);
      if (no_duplicate_payload) {
        learn_msg = create_tiny_learn_msg(p, p->proposer.msg);
      } else {
        /* purecov: begin deadcode */
        init_learn_msg(p->proposer.msg);
        learn_msg = p->proposer.msg;
        /* purecov: end */
      }
      p->proposer.sent_learn = p->proposer.bal;
    }
    return learn_msg;
  }
}

static void do_learn(site_def const *site MY_ATTRIBUTE((unused)),
                     pax_machine *p, pax_msg *m) {
  ADD_DBG(D_CONS, add_synode_event(p->synode);
          add_event(EVENT_DUMP_PAD, string_arg("m->from"));
          add_event(EVENT_DUMP_PAD, uint_arg(m->from));
          add_event(EVENT_DUMP_PAD, string_arg(pax_op_to_str(m->op)));
          add_event(EVENT_DUMP_PAD, string_arg("proposal"));
          add_ballot_event(m->proposal);
          add_event(EVENT_DUMP_PAD, string_arg("promise"));
          add_ballot_event(p->acceptor.promise););
  /* FN; SYCEXP(p->synode); SYCEXP(m->synode); STRLIT(NEWLINE); */
  IFDBG(D_NONE, FN; SYCEXP(p->synode); SYCEXP(m->synode);
        dbg_bitset(m->receivers, get_maxnodes(site)););
  if (m->a) m->a->chosen = TRUE;
  replace_pax_msg(&p->acceptor.msg, m);
  replace_pax_msg(&p->learner.msg, m);
  /*
     Track memory used by client data in the cache.
     If we do not care about instances that are being decided,
     it is only necessary to compute the added memory when we
     record the outcome of a consensus round.
  */
  add_cache_size(p);
  /* Shrink the cache size if necessary */
  shrink_cache();
}

bool_t handle_simple_ack_prepare(site_def const *site, pax_machine *p,
                                 pax_msg *m) {
  if (get_nodeno(site) != VOID_NODE_NO)
    BIT_SET(m->from, p->proposer.prep_nodeset);

  {
    bool_t can_propose = FALSE;
    if (m->op == ack_prepare_op &&
        gt_ballot(m->proposal, p->proposer.msg->proposal)) { /* greater */
      replace_pax_msg(&p->proposer.msg, m);
      assert(p->proposer.msg);
    }
    if (gt_ballot(m->reply_to, p->proposer.sent_prop)) {
      can_propose = check_propose(site, p);
    }
    return can_propose;
  }
}

/* Other node has already accepted a value */
static void handle_ack_prepare(site_def const *site, pax_machine *p,
                               pax_msg *m) {
  ADD_DBG(D_CONS, add_synode_event(p->synode);
          add_event(EVENT_DUMP_PAD, string_arg("m->from"));
          add_event(EVENT_DUMP_PAD, uint_arg(m->from));
          add_event(EVENT_DUMP_PAD, string_arg(pax_op_to_str(m->op))););
  assert(m);
  IFDBG(D_NONE, FN; if (p->proposer.msg) BALCEXP(p->proposer.msg->proposal);
        BALCEXP(p->proposer.bal); BALCEXP(m->reply_to);
        BALCEXP(p->proposer.sent_prop); SYCEXP(m->synode));
  /*
    If the node is preparing a Noop for another node's slot, it is possible
    that the leader of the slot has since proposed a value. Hence, there is
    no need to move forward if we know that the value has been accepted. This
    also prevents changing the size of a learned pax_machine, which would
    cause inconsistent reporting of memory usage in P_S.
  */
  if (finished(p)) return;

  if (m->from != VOID_NODE_NO &&
      eq_ballot(p->proposer.bal, m->reply_to)) { /* answer to my prepare */
    bool_t can_propose = handle_simple_ack_prepare(site, p, m);
    if (can_propose) send_propose_msg(p->proposer.msg);
  }
}

/* #define AUTO_MSG(p,synode) {if(!(p)){replace_pax_msg(&(p),
 * pax_msg_new(synode, site));} */

static pax_msg *create_ack_accept_msg(pax_msg *m, synode_no synode) {
  CREATE_REPLY(m);
  reply->op = ack_accept_op;
  reply->synode = synode;
  return reply;
}

pax_msg *handle_simple_accept(pax_machine *p, pax_msg *m, synode_no synode) {
  pax_msg *reply = NULL;
  if (finished(p)) { /* We have learned a value */
    reply = create_learn_msg_for_ignorant_node(p, m, synode);
  } else if (!gt_ballot(p->acceptor.promise,
                        m->proposal) || /* Paxos acceptor phase 2 decision */
             noop_match(p, m)) {
    IFDBG(D_NONE, FN; SYCEXP(m->synode); STRLIT("accept ");
          BALCEXP(m->proposal));
    p->last_modified = task_now();
    replace_pax_msg(&p->acceptor.msg, m);
    reply = create_ack_accept_msg(m, synode);
  }
  return reply;
}

/* Accecpt value if promise is not greater */
static void handle_accept(site_def const *site, pax_machine *p,
                          linkage *reply_queue, pax_msg *m) {
  IFDBG(D_NONE, FN; BALCEXP(p->acceptor.promise); BALCEXP(m->proposal);
        STREXP(pax_msg_type_to_str(m->msg_type)));
  PAX_MSG_SANITY_CHECK(m);
  ADD_DBG(D_CONS, add_synode_event(p->synode);
          add_event(EVENT_DUMP_PAD, string_arg("m->from"));
          add_event(EVENT_DUMP_PAD, uint_arg(m->from));
          add_event(EVENT_DUMP_PAD, string_arg(pax_op_to_str(m->op)));
          add_event(EVENT_DUMP_PAD, string_arg("proposal"));
          add_ballot_event(m->proposal);
          add_event(EVENT_DUMP_PAD, string_arg("promise"));
          add_ballot_event(p->acceptor.promise););

  {
    pax_msg *reply = handle_simple_accept(p, m, m->synode);
    if (reply != NULL) SEND_REPLY;
  }
}

/* Handle answer to accept */
pax_msg *handle_simple_ack_accept(site_def const *site, pax_machine *p,
                                  pax_msg *m) {
  pax_msg *learn_msg = NULL;
  if (get_nodeno(site) != VOID_NODE_NO && m->from != VOID_NODE_NO &&
      eq_ballot(p->proposer.bal, m->reply_to)) { /* answer to my accept */
    BIT_SET(m->from, p->proposer.prop_nodeset);
    if (gt_ballot(m->proposal, p->proposer.sent_learn)) {
      learn_msg = check_learn(site, p);
    }
  }
  return learn_msg;
}
static void handle_ack_accept(site_def const *site, pax_machine *p,
                              pax_msg *m) {
  ADD_DBG(D_CONS, add_synode_event(p->synode);
          add_event(EVENT_DUMP_PAD, string_arg("m->from"));
          add_event(EVENT_DUMP_PAD, uint_arg(m->from));
          add_event(EVENT_DUMP_PAD, string_arg(pax_op_to_str(m->op))););
  IFDBG(D_NONE, FN; SYCEXP(m->synode); BALCEXP(p->proposer.bal);
        BALCEXP(p->proposer.sent_learn); BALCEXP(m->proposal);
        BALCEXP(m->reply_to););
  IFDBG(D_NONE, FN; SYCEXP(p->synode);
        if (p->acceptor.msg) BALCEXP(p->acceptor.msg->proposal);
        BALCEXP(p->proposer.bal); BALCEXP(m->reply_to););

  {
    pax_msg *learn_msg = handle_simple_ack_accept(site, p, m);
    if (learn_msg != NULL) {
      if (learn_msg->op == tiny_learn_op) {
        send_tiny_learn_msg(site, learn_msg);
      } else {
        /* purecov: begin deadcode */
        assert(learn_msg->op == learn_op);
        send_learn_msg(site, learn_msg);
        /* purecov: end */
      }
    }
  }
}

/* Handle incoming learn. */
static void activate_sweeper();
void handle_tiny_learn(site_def const *site, pax_machine *pm, pax_msg *p) {
  assert(p->msg_type != no_op);
  if (pm->acceptor.msg) {
    /* 			BALCEXP(pm->acceptor.msg->proposal); */
    if (eq_ballot(pm->acceptor.msg->proposal, p->proposal)) {
      pm->acceptor.msg->op = learn_op;
      pm->last_modified = task_now();
      update_max_synode(p);
      handle_learn(site, pm, pm->acceptor.msg);
    } else {
      send_read(p->synode);
      IFDBG(D_NONE, FN; STRLIT("tiny_learn"); SYCEXP(p->synode);
            BALCEXP(pm->acceptor.msg->proposal); BALCEXP(p->proposal));
    }
  } else {
    send_read(p->synode);
    IFDBG(D_NONE, FN; STRLIT("tiny_learn"); SYCEXP(p->synode);
          BALCEXP(p->proposal));
  }
}

static void force_pax_machine(pax_machine *p, int enforcer) {
  if (!p->enforcer) { /* Not if already marked as forcing node */
    if (enforcer) {   /* Only if forcing node */
      /* Increase ballot count with a large increment without overflowing */
      /* p->proposer.bal.cnt may be -1. */
      int32_t delta = (INT32_MAX - MAX(p->proposer.bal.cnt, 0)) / 3;
      p->proposer.bal.cnt += delta;
    }
  }
  p->force_delivery = 1;
  p->enforcer = enforcer;
}

/* Configure all messages in interval start, end to be forced */
static void force_interval(synode_no start, synode_no end, int enforcer) {
  while (!synode_gt(start, end)) {
    pax_machine *p = get_cache(start);
    if (get_nodeno(find_site_def(start)) == VOID_NODE_NO) break;

    /* The forcing node will call force_interval twice, first when
    the new config is originally installed, and again when it
    receives it as an xcom message. start may be the same, but
    end will be greater the second time, since it is calculated
    based on the message number of the incoming config. Since the forcing
    node is the one responsible for delivering all messages until the
    start of the new site, it is important that all instances belonging to
    the old site are correctly marked. */

    if (p->enforcer) enforcer = 1; /* Extend to new instances */
    force_pax_machine(p, enforcer);

    /* Old nodesets are null and void */
    BIT_ZERO(p->proposer.prep_nodeset);
    BIT_ZERO(p->proposer.prop_nodeset);
    start = incr_synode(start);
  }
}

static void start_force_config(site_def *s, int enforcer) {
  synode_no end = add_event_horizon(s->boot_key);

  IFDBG(D_NONE, FN; SYCEXP(executed_msg); SYCEXP(end));
  if (synode_gt(end, max_synode)) set_max_synode(end);

  free_forced_config_site_def();
  wait_forced_config = 0;
  forced_config = s;
  force_interval(executed_msg, max_synode,
                 enforcer); /* Force everything in the pipeline */
}

/* Learn this value */
void handle_learn(site_def const *site, pax_machine *p, pax_msg *m) {
  IFDBG(D_NONE, FN; STRLIT("proposer nodeset ");
        dbg_bitset(p->proposer.prop_nodeset, get_maxnodes(site)););
  IFDBG(D_NONE, FN; STRLIT("receivers ");
        dbg_bitset(m->receivers, get_maxnodes(site)););
  IFDBG(D_NONE, FN; NDBG(task_now(), f); SYCEXP(p->synode);
        COPY_AND_FREE_GOUT(dbg_app_data(m->a)););

  PAX_MSG_SANITY_CHECK(m);
  p->last_modified = task_now();
  if (!finished(p)) { /* Avoid re-learn */
    activate_sweeper();
    do_learn(site, p, m);
    /* Check for special messages */
    if (m->a && m->a->body.c_t == unified_boot_type) {
      IFDBG(D_NONE, FN; STRLIT("Got unified_boot "); SYCEXP(p->synode);
            SYCEXP(m->synode););
      G_INFO("x_fsm_net_boot is set in handle_learn");
      XCOM_FSM(x_fsm_net_boot, void_arg(m->a));
    }
    /* See if someone is forcing a new config */
    if (m->force_delivery && m->a) {
      IFDBG(D_NONE, FN; STRLIT("Got forced config "); SYCEXP(p->synode);
            SYCEXP(m->synode););
      /* Configure all messages from executed_msg until start of new config
         as forced messages so they will eventually be finished */
      /* Immediately install this new config */
      switch (m->a->body.c_t) {
        case add_node_type:
          /* purecov: begin deadcode */
          if (should_ignore_forced_config_or_view(
                  find_site_def(p->synode)->x_proto)) {
            log_ignored_forced_config(m->a, "handle_learn");
          } else {
            start_force_config(clone_site_def(handle_add_node(m->a)), 0);
          }
          break;
        /* purecov: end */
        case remove_node_type:
          /* purecov: begin deadcode */
          if (should_ignore_forced_config_or_view(
                  find_site_def(p->synode)->x_proto)) {
            log_ignored_forced_config(m->a, "handle_learn");
          } else {
            start_force_config(clone_site_def(handle_remove_node(m->a)), 0);
          }
          break;
        /* purecov: end */
        case force_config_type:
          start_force_config(clone_site_def(install_node_group(m->a)), 0);
          break;
        default:
          break;
      }
    }
  }

  task_wakeup(&p->rv);
}

/* Skip this value */
static void handle_skip(site_def const *site, pax_machine *p, pax_msg *m) {
  /* IFDBG(D_NONE, FN;); */
  /* IFDBG(D_NONE, FN; NDBG(task_now(),f); SYCEXP(p->msg->synode)); */
  if (!finished(p)) {
    p->last_modified = task_now();
    skip_value(m);
    do_learn(site, p, m);
  }
  /* IFDBG(D_NONE, FN; STRLIT("taskwakeup "); SYCEXP(p->msg->synode)); */
  task_wakeup(&p->rv);
}

static void handle_client_msg(pax_msg *p) {
  if (!p || p->a == NULL) /* discard invalid message */
    return;
  {
    msg_link *ml = msg_link_new(p, VOID_NODE_NO);

    /* Put it in the proposer queue */
    ADD_T_EV(task_now(), __FILE__, __LINE__, "handle_client_msg");
    channel_put(&prop_input_queue, &ml->l);
  }
}

#ifdef ACCEPT_SITE_TEST
/* See if we should process an incoming ping from a node.
   The purpose is to avoid doing recovery from a node with an obsolete site
   definition */
static int accept_site(site_def const *site) {
  site_def *mysite = (site_def *)get_site_def();

  if (site) {
    if (!mysite) {
      site_def *prev = (site_def *)find_prev_site_def(site->boot_key);
      IFDBG(
          D_NONE, FN; PTREXP(site); PTREXP(mysite); PTREXP(prev);
          SYCEXP(site->boot_key); if (prev) { SYCEXP(prev->boot_key); });
      if (!prev) {
        /** alive when no site, no known previous definition, and present in
         * new is accepted */
        return (site->boot_key.group_id == 0
                    ? 1
                    : (xcom_find_node_index((node_list *)&site->nodes) !=
                       VOID_NODE_NO));
      } else {
        /** alive when no site, a previous definition of groupid is known, but
         * is older than site def, is accepted */
        return synode_gt(site->boot_key, prev->boot_key);
      }
    } else {
      IFDBG(D_NONE, FN; PTREXP(site); PTREXP(mysite); SYCEXP(site->boot_key);
            SYCEXP(mysite->boot_key));
      if (get_group_id(site) != get_group_id(mysite)) {
        /** alive from different site should never be accepted */
        return 0;
      } else {
        /** alive from same site should be accepted if boot_key is larger than
         * mine */
        node_no my_nodeno = xcom_find_node_index((node_list *)&mysite->nodes);
        node_no site_nodeno = xcom_find_node_index((node_list *)&site->nodes);
        return (synode_gt(site->boot_key, mysite->boot_key) &&
                ((my_nodeno != VOID_NODE_NO) || (site_nodeno != VOID_NODE_NO)));
      }
    }
  }
  /** Always accept a NULL site */
  IFDBG(D_NONE, FN; PTREXP(site));
  return 1;
}
#endif

/* Handle incoming "need boot" message. */
/* purecov: begin deadcode */
static inline void handle_boot(site_def const *site, linkage *reply_queue,
                               pax_msg *p) {
  /* This should never be TRUE, but validate it instead of asserting. */
  if (site == NULL || site->nodes.node_list_len < 1) {
    G_INFO(
        "handle_boot: Received an unexpected need_boot_op when site == NULL or "
        "site->nodes.node_list_len < 1");
    return;
  }

  if (ALWAYS_HANDLE_NEED_BOOT || should_handle_need_boot(site, p)) {
    handle_need_snapshot(reply_queue, p);
  } else {
    G_INFO(
        "Ignoring a need_boot_op message from an XCom incarnation that does "
        "not belong to the group.");
  }
}
/* purecov: end */

bool_t should_handle_need_boot(site_def const *site, pax_msg *p) {
  bool_t should_handle = FALSE;
  bool_t const sender_advertises_identity =
      (p->a != NULL && p->a->body.c_t == xcom_boot_type);

  /*
   If the message advertises the sender's identity, check if it matches the
   membership information.

   The sender's identity may not match if, e.g.:

     a. The member was already removed, or
     b. It is a new incarnation of a crashed member that is yet to be removed.

   ...or some other reason.

   If it is due to reason (b), we do not want to boot the sender because XCom
   only implements a simple fail-stop model. Allowing the sender to rejoin the
   group without going through the full remove+add node path could violate
   safety because the sender does not remember any previous Paxos acceptances it
   acknowledged before crashing.
   Since the pre-crash incarnation may have accepted a value for a given synod
   but the post-crash incarnation has forgotten that fact, the post-crash
   incarnation will fail to propagate the previously accepted value to a higher
   ballot. Since majorities can overlap on a single node, if the overlap node
   is the post-crash incarnation which has forgotten about the previously
   accepted value, a higher ballot proposer may get a different value accepted,
   leading to conflicting values to be accepted for different proposers, which
   is a violation of the safety properties of the Paxos protocol.

   If the sender does not advertise its identity, we boot it unconditionally.
   This is for backwards compatibility.
  */
  if (sender_advertises_identity) {
    bool_t const sender_advertises_one_identity =
        (p->a->body.app_u_u.nodes.node_list_len == 1);

    /* Defensively accept only messages with a single identity. */
    if (sender_advertises_one_identity) {
      node_address *sender_identity = p->a->body.app_u_u.nodes.node_list_val;

      should_handle = node_exists_with_uid(sender_identity, &site->nodes);
    }
  } else {
    should_handle = TRUE;
  }

  return should_handle;
}

void init_need_boot_op(pax_msg *p, node_address *identity) {
  p->op = need_boot_op;
  if (identity != NULL) {
    p->a = new_app_data();
    p->a->body.c_t = xcom_boot_type;
    init_node_list(1, identity, &p->a->body.app_u_u.nodes);
  }
}

#define PING_GATHERING_TIME_WINDOW 5.0
#define PINGS_GATHERED_BEFORE_CONNECTION_SHUTDOWN 3

int pre_process_incoming_ping(site_def const *site, pax_msg const *pm,
                              int has_client_already_booted,
                              double current_time) {
  // Yes... it is a ping for me, boot is done and it is a are_you_alive_op
  // This means that something wrong is not right...
  int did_shutdown = 0;

  if ((pm->from != get_nodeno(site)) && has_client_already_booted &&
      (pm->op == are_you_alive_op)) {
    G_DEBUG(
        "Received a ping to myself. This means that something must be wrong in "
        "a bi-directional connection")
    // Going to kill the connection for that node...
    if (site && (pm->from < site->nodes.node_list_len)) {
      // This is not the first ping received in the last 5 seconds...
      if (site->servers[pm->from]->last_ping_received >
          (current_time - PING_GATHERING_TIME_WINDOW)) {
        site->servers[pm->from]->number_of_pings_received++;
      } else {  // First ping since at least more than 5 seconds...
        site->servers[pm->from]->number_of_pings_received = 1;
      }

      site->servers[pm->from]->last_ping_received = current_time;

      // If we keep on receiving periodical pings... lets kill the connection
      if (is_connected(&site->servers[pm->from]->con) &&
          site->servers[pm->from]->number_of_pings_received ==
              PINGS_GATHERED_BEFORE_CONNECTION_SHUTDOWN) {
        site->servers[pm->from]->unreachable = DIRECT_ABORT_CONN;
        shutdown_connection(&site->servers[pm->from]->con);
        G_WARNING(
            "Shutting down an outgoing connection. This happens because "
            "something might be wrong on a bi-directional connection to node "
            "%s:%d. Please check the connection status to this member",
            site->servers[pm->from]->srv, site->servers[pm->from]->port);
        did_shutdown = 1;
      }
    }
  }

  return did_shutdown;
}

/* Handle incoming alive message */
static double sent_alive = 0.0;
static inline void handle_alive(site_def const *site, linkage *reply_queue,
                                pax_msg *pm) {
  if (pre_process_incoming_ping(site, pm, client_boot_done, task_now())) {
    return;
  }

  if (client_boot_done || !(task_now() - sent_alive > 1.0)) /* Already done? */
    return;

#ifdef ACCEPT_SITE_TEST
  if (!accept_site(site)) return;
#endif

  /* Avoid responding to own ping */
  if (pm->from == get_nodeno(site) || pm->from == pm->to) return;

  /*
   This code will check if the ping is intended to us.
   If the encoded node does not exist in the current configuration,
   we avoid sending need_boot_op, since it must be from a different
   reincarnation of this node.
   */
  if (site && pm->a && pm->a->body.c_t == xcom_boot_type) {
    IFDBG(D_NONE, FN;
          COPY_AND_FREE_GOUT(dbg_list(&pm->a->body.app_u_u.nodes)););

    if (!node_exists_with_uid(&pm->a->body.app_u_u.nodes.node_list_val[0],
                              &get_site_def()->nodes))
      return;
  }

  if (is_dead_site(pm->group_id)) return; /* Avoid dealing with zombies */

  {
    CREATE_REPLY(pm);
    init_need_boot_op(reply, cfg_app_xcom_get_identity());
    sent_alive = task_now();
    SEND_REPLY;
  }
  IFDBG(D_NONE, FN; STRLIT("sent need_boot_op"););
}

static void update_max_synode(pax_msg *p) {
  if (is_dead_site(p->group_id)) return;
  if (get_group_id(get_site_def()) == 0 || max_synode.group_id == 0) {
    set_max_synode(p->synode);
  } else if (max_synode.group_id == p->synode.group_id) {
    if (synode_gt(p->synode, max_synode)) {
      set_max_synode(p->synode);
    }
    if (synode_gt(p->max_synode, max_synode)) {
      set_max_synode(p->max_synode);
    }
  }
}

/* Message dispatch */
#define BAL_FMT "ballot {cnt %d node %d}"
#define BAL_MEM(x) (x).cnt, (x).node

static int clicnt = 0;

xcom_event_horizon xcom_get_minimum_event_horizon() {
  return EVENT_HORIZON_MIN;
}

xcom_event_horizon xcom_get_maximum_event_horizon() {
  return EVENT_HORIZON_MAX;
}

/**
 * Retrieves the latest event horizon.
 *
 * There is no specific reason for this method to return the latest event
 * horizon instead of the current one. Both would be acceptable results of
 * this function, but we had to make a decision of one over the other.
 *
 * @param[out] event_horizon the latest event horizon
 * @retval REQUEST_FAIL XCom is not initialized yet
 * @retval REQUEST_OK function was successful and event_horizon contains the
 *                    latest event horizon
 */
static client_reply_code xcom_get_event_horizon(
    xcom_event_horizon *event_horizon) {
  site_def const *latest_config = get_site_def();
  if (latest_config == NULL) return REQUEST_FAIL;
  *event_horizon = latest_config->event_horizon;
  return REQUEST_OK;
}

static u_int allow_add_node(app_data_ptr a) {
  /* Get information on the current site definition */
  const site_def *new_site_def = get_site_def();
  const site_def *valid_site_def = find_site_def(executed_msg);

  /* Get information on the nodes to be added */
  u_int nr_nodes_to_add = a->body.app_u_u.nodes.node_list_len;
  node_address *nodes_to_change = a->body.app_u_u.nodes.node_list_val;

  if (add_node_unsafe_against_event_horizon(a)) return 0;

  if (add_node_unsafe_against_ipv4_old_nodes(a)) {
    G_MESSAGE(
        "This server is unable to join the group as the NIC used is configured "
        "with IPv6 only and there are members in the group that are unable to "
        "communicate using IPv6, only IPv4.Please configure this server to "
        "join the group using an IPv4 address instead.");
    return 0;
  }

  {
    u_int i;
    for (i = 0; i < nr_nodes_to_add; i++) {
      if (node_exists(&nodes_to_change[i], &new_site_def->nodes) ||
          node_exists(&nodes_to_change[i], &valid_site_def->nodes)) {
        /*
        We are simply ignoring the attempt to add a node to the
        group when there is an old incarnation of it, meaning
        that the node has crashed and restarted so fastly that
        nobody has noticed that it has gone.

        In XCOM, the group is not automatically reconfigured
        and it is possible to start reusing a node that has
        crashed and restarted without reconfiguring the group
        by adding the node back to it.

        However, this operation may be unsafe because XCOM
        does not implement a crash-recovery model and nodes
        suffer from amnesia after restarting the service. In
        other words this may lead to inconsistency issues in
        the paxos protocol.

        Unfortunately, preventing that a node is added back
        to the system where there is an old incarnation will
        not fix this problem since other changes are required.
        */
        G_MESSAGE(
            "Old incarnation found while trying to "
            "add node %s %.*s.",
            nodes_to_change[i].address, nodes_to_change[i].uuid.data.data_len,
            nodes_to_change[i].uuid.data.data_val);
        return 0;
      }
    }
  }

  return 1;
}

static u_int allow_remove_node(app_data_ptr a) {
  /* Get information on the current site definition */
  const site_def *new_site_def = get_site_def();

  /* Get information on the nodes to be added */
  u_int nodes_len = a->body.app_u_u.nodes.node_list_len;
  node_address *nodes_to_change = a->body.app_u_u.nodes.node_list_val;

  u_int i;
  for (i = 0; i < nodes_len; i++) {
    if (!node_exists_with_uid(&nodes_to_change[i], &new_site_def->nodes)) {
      /*
      If the UID does not exist, then 1) the node has already been
      removed or 2) it has reincarnated.
      */
      /* purecov: begin inspected */
      if (node_exists(&nodes_to_change[i], &new_site_def->nodes)) {
        /*
        We also cannot allow an upper-layer to remove a new incarnation
        of a node when it tries to remove an old one.
        */
        G_MESSAGE(
            "New incarnation found while trying to "
            "remove node %s %.*s.",
            nodes_to_change[i].address, nodes_to_change[i].uuid.data.data_len,
            nodes_to_change[i].uuid.data.data_val);
      } else {
        /* The node has already been removed, so we block the request */
        G_MESSAGE(
            "Node has already been removed: "
            "%s %.*s.",
            nodes_to_change[i].address, nodes_to_change[i].uuid.data.data_len,
            nodes_to_change[i].uuid.data.data_val);
      }
      return 0;
      /* purecov: end */
    }
  }

  return 1;
}

/**
 * Logs the fact that an add/remove node request is aimed at another group.
 *
 * @param a a pointer to the app_data of the configuration command
 * @param message_fmt a formatted message to log, containing a single %s that
 * will be replaced by the node's address
 */
static void log_cfgchange_wrong_group(app_data_ptr a,
                                      const char *const message_fmt) {
  u_int const nr_nodes = a->body.app_u_u.nodes.node_list_len;
  u_int i;
  for (i = 0; i < nr_nodes; i++) {
    char const *const address = a->body.app_u_u.nodes.node_list_val[i].address;
    G_WARNING(message_fmt, address);
  }
}

/**
 * Validates if a configuration command can be executed.
 * Checks whether the configuration command is aimed at the correct group.
 * Checks whether the configuration command pertains to a node reincarnation.
 *
 * @param p a pointer to the pax_msg of the configuration command
 * @retval REQUEST_OK if the reconfiguration command can be executed
 * @retval REQUEST_RETRY if XCom is still booting
 * @retval REQUEST_FAIL if the configuration command cannot be executed
 */
static client_reply_code can_execute_cfgchange(pax_msg *p) {
  app_data_ptr a = p->a;

  if (executed_msg.msgno <= 2) return REQUEST_RETRY;

  if (a && a->group_id != 0 && a->group_id != executed_msg.group_id) {
    switch (a->body.c_t) {
      case add_node_type:
        log_cfgchange_wrong_group(
            a,
            "The request to add %s to the group has been rejected because it "
            "is aimed at another group");
        break;
      case remove_node_type:
        log_cfgchange_wrong_group(
            a,
            "The request to remove %s from the group has been rejected because "
            "it is aimed at another group");
        break;
      case force_config_type:
        G_WARNING(
            "The request to force the group membership has been rejected "
            "because it is aimed at another group");
        break;
      default:
        assert(0 &&
               "A cargo_type different from {add_node_type, remove_node_type, "
               "force_config_type} should not have hit this code path");
    }
    return REQUEST_FAIL;
  }

  if (a && a->body.c_t == add_node_type && !allow_add_node(a))
    return REQUEST_FAIL;

  if (a && a->body.c_t == remove_node_type && !allow_remove_node(a))
    return REQUEST_FAIL;

  if (a && a->body.c_t == set_event_horizon_type &&
      unsafe_event_horizon_reconfiguration(a))
    return REQUEST_FAIL;

  if (a && a->body.c_t == force_config_type &&
      are_there_dead_nodes_in_new_config(a))
    return REQUEST_FAIL;

  return REQUEST_OK;
}

static void activate_sweeper() {
  if (sweeper) {
    ADD_DBG(D_CONS, add_event(EVENT_DUMP_PAD,
                              string_arg("sweeper activated max_synode"));
            add_synode_event(max_synode););
    task_activate(sweeper);
  }
}

static synode_no start_config = NULL_SYNODE;

void dispatch_get_event_horizon(site_def const *site, pax_msg *p,
                                linkage *reply_queue) {
  CREATE_REPLY(p);
  IFDBG(D_NONE, FN; STRLIT("Got get_event_horizon from client");
        SYCEXP(p->synode););
  reply->op = xcom_client_reply;
  reply->cli_err = xcom_get_event_horizon(&reply->event_horizon);
  SEND_REPLY;
}

/*
 * Log the result of the get_synode_app_data command.
 */
static void log_get_synode_app_data_failure(
    xcom_get_synode_app_data_result error_code) {
  switch (error_code) {
    case XCOM_GET_SYNODE_APP_DATA_OK:
      break;
    case XCOM_GET_SYNODE_APP_DATA_ERROR:
      G_DEBUG("Could not reply successfully to request for synode data.");
      break;
    case XCOM_GET_SYNODE_APP_DATA_NOT_CACHED:
      G_DEBUG(
          "Could not reply successfully to request for synode data because "
          "some of the requested synodes are no longer cached.");
      break;
    case XCOM_GET_SYNODE_APP_DATA_NOT_DECIDED:
      G_DEBUG(
          "Could not reply successfully to request for synode data because "
          "some of the requested synodes are still undecided.");
      break;
    case XCOM_GET_SYNODE_APP_DATA_NO_MEMORY:
      G_DEBUG(
          "Could not reply successfully to request for synode data because "
          "memory could not be allocated.");
      break;
  }
}

void dispatch_get_synode_app_data(site_def const *site, pax_msg *p,
                                  linkage *reply_queue) {
  IFDBG(D_NONE, FN; STRLIT("Got get_synode_app_data from client");
        SYCEXP(p->synode););

  {
    CREATE_REPLY(p);
    reply->op = xcom_client_reply;

    {
      xcom_get_synode_app_data_result error_code;
      error_code = xcom_get_synode_app_data(&p->a->body.app_u_u.synodes,
                                            &reply->requested_synode_app_data);
      switch (error_code) {
        case XCOM_GET_SYNODE_APP_DATA_OK:
          reply->cli_err = REQUEST_OK;
          break;
        case XCOM_GET_SYNODE_APP_DATA_NOT_CACHED:
        case XCOM_GET_SYNODE_APP_DATA_NOT_DECIDED:
        case XCOM_GET_SYNODE_APP_DATA_NO_MEMORY:
        case XCOM_GET_SYNODE_APP_DATA_ERROR:
          reply->cli_err = REQUEST_FAIL;
          log_get_synode_app_data_failure(error_code);
          break;
      }

      SEND_REPLY;
    }
  }
}

static int can_send_snapshot();

pax_msg *dispatch_op(site_def const *site, pax_msg *p, linkage *reply_queue) {
  pax_machine *pm = NULL;
  site_def *dsite = find_site_def_rw(p->synode);
  int in_front = too_far(p->synode);

  if (p->force_delivery) {
    /* Ensure that forced message can be processed */
    in_front = 0;
  }

  if (dsite && p->op != client_msg && is_server_connected(dsite, p->from)) {
    /* Wake up the detector task if this node was previously marked as
     * potentially failed. */
    if (!note_detected(dsite, p->from)) task_wakeup(&detector_wait);
    update_delivered(dsite, p->from, p->delivered_msg);
  }

  IFDBG(D_NONE, FN; STRLIT("incoming message ");
        COPY_AND_FREE_GOUT(dbg_pax_msg(p)););
  ADD_DBG(D_NONE, add_synode_event(p->synode);
          add_event(EVENT_DUMP_PAD, string_arg("p->from"));
          add_event(EVENT_DUMP_PAD, uint_arg(p->from));
          add_event(EVENT_DUMP_PAD, string_arg("in_front"));
          add_event(EVENT_DUMP_PAD, int_arg(in_front));
          add_event(EVENT_DUMP_PAD, string_arg(pax_op_to_str(p->op))););

  switch (p->op) {
    case client_msg:
      clicnt++;
      if (p->a && (p->a->body.c_t == exit_type)) {
        /* purecov: begin deadcode */
        IFDBG(D_NONE, FN; STRLIT("Got exit from client"); SYCEXP(p->synode););
        bury_site(get_group_id(get_site_def()));
        ADD_DBG(D_FSM, add_event(EVENT_DUMP_PAD, string_arg("terminating"));)
        terminate_and_exit();
        break;
        /* purecov: end */
      }
      if (p->a && (p->a->body.c_t == reset_type)) {
        /* purecov: begin deadcode */
        IFDBG(D_NONE, FN; STRLIT("Got reset from client"); SYCEXP(p->synode););
        bury_site(get_group_id(get_site_def()));
        ADD_DBG(D_FSM, add_event(EVENT_DUMP_PAD, string_arg("terminating"));)
        XCOM_FSM(x_fsm_terminate, int_arg(0));
        break;
        /* purecov: end */
      }
      if (p->a && (p->a->body.c_t == remove_reset_type)) {
        /* purecov: begin deadcode */
        IFDBG(D_NONE, FN; STRLIT("Got remove_reset from client");
              SYCEXP(p->synode););
        ADD_DBG(D_FSM, add_event(EVENT_DUMP_PAD, string_arg("terminating"));)
        XCOM_FSM(x_fsm_terminate, int_arg(0));
        break;
        /* purecov: end */
      }
      if (p->a && (p->a->body.c_t == enable_arbitrator)) {
        CREATE_REPLY(p);
        IFDBG(D_NONE, FN; STRLIT("Got enable_arbitrator from client");
              SYCEXP(p->synode););
        ARBITRATOR_HACK = 1;
        reply->op = xcom_client_reply;
        reply->cli_err = REQUEST_OK;
        SEND_REPLY;
        break;
      }
      if (p->a && (p->a->body.c_t == disable_arbitrator)) {
        CREATE_REPLY(p);
        IFDBG(D_NONE, FN; STRLIT("Got disable_arbitrator from client");
              SYCEXP(p->synode););
        ARBITRATOR_HACK = 0;
        reply->op = xcom_client_reply;
        reply->cli_err = REQUEST_OK;
        SEND_REPLY;
        break;
      }
      if (p->a && (p->a->body.c_t == set_cache_limit)) {
        CREATE_REPLY(p);
        IFDBG(D_NONE, FN; STRLIT("Got set_cache_limit from client");
              SYCEXP(p->synode););
        if (the_app_xcom_cfg) {
          set_max_cache_size(p->a->body.app_u_u.cache_limit);
          reply->cli_err = REQUEST_OK;
        } else {
          reply->cli_err = REQUEST_FAIL;
        }
        reply->op = xcom_client_reply;
        SEND_REPLY;
        break;
      }
      if (p->a && (p->a->body.c_t == set_flp_timeout)) {
        CREATE_REPLY(p);
        if (the_app_xcom_cfg) {
          the_app_xcom_cfg->m_flp_timeout = p->a->body.app_u_u.flp_timeout;
          reply->cli_err = REQUEST_OK;
        } else {
          reply->cli_err = REQUEST_FAIL;
        }
        reply->op = xcom_client_reply;
        SEND_REPLY;
        break;
      }
      if (p->a && (p->a->body.c_t == x_terminate_and_exit)) {
        /* purecov: begin deadcode */
        CREATE_REPLY(p);
        IFDBG(D_NONE, FN; STRLIT("Got terminate_and_exit from client");
              SYCEXP(p->synode););
        reply->op = xcom_client_reply;
        reply->cli_err = REQUEST_OK;
        SEND_REPLY;
        /*
          The function frees sites which is used by SEND_REPLY,
          so it should be called after SEND_REPLY.
        */
        IFDBG(D_NONE, FN; STRLIT("terminate_and_exit"));
        ADD_DBG(D_FSM, add_event(EVENT_DUMP_PAD, string_arg("terminating"));)
        terminate_and_exit();
        break;
        /* purecov: end */
      }
      if (p->a && (p->a->body.c_t == get_event_horizon_type)) {
        dispatch_get_event_horizon(site, p, reply_queue);
        break;
      }
      if (p->a && (p->a->body.c_t == get_synode_app_data_type)) {
        dispatch_get_synode_app_data(site, p, reply_queue);
        break;
      }
      if (p->a && (p->a->body.c_t == add_node_type ||
                   p->a->body.c_t == remove_node_type ||
                   p->a->body.c_t == force_config_type ||
                   p->a->body.c_t == set_event_horizon_type)) {
        client_reply_code cli_err;
        CREATE_REPLY(p);
        reply->op = xcom_client_reply;
        reply->cli_err = cli_err = can_execute_cfgchange(p);
        SEND_REPLY;
        if (cli_err != REQUEST_OK) {
          break;
        }
      }
      if (p->a && p->a->body.c_t == unified_boot_type) {
        IFDBG(D_NONE, FN; STRLIT("Got unified_boot from client");
              SYCEXP(p->synode););
        IFDBG(D_NONE, FN;
              COPY_AND_FREE_GOUT(dbg_list(&p->a->body.app_u_u.nodes)););
        IFDBG(D_NONE, STRLIT("handle_client_msg "); NDBG(p->a->group_id, x));
        G_INFO("x_fsm_net_boot is set in dispatch_op");
        XCOM_FSM(x_fsm_net_boot, void_arg(p->a));
      }
      if (p->a && p->a->body.c_t == add_node_type) {
        IFDBG(D_NONE, FN; STRLIT("Got add_node from client");
              SYCEXP(p->synode););
        IFDBG(D_NONE, FN;
              COPY_AND_FREE_GOUT(dbg_list(&p->a->body.app_u_u.nodes)););
        IFDBG(D_NONE, STRLIT("handle_client_msg "); NDBG(p->a->group_id, x));
        assert(get_site_def());
      }
      if (p->a && p->a->body.c_t == remove_node_type) {
        IFDBG(D_NONE, FN; STRLIT("Got remove_node from client");
              SYCEXP(p->synode););
        IFDBG(D_NONE, FN;
              COPY_AND_FREE_GOUT(dbg_list(&p->a->body.app_u_u.nodes)););
        IFDBG(D_NONE, STRLIT("handle_client_msg "); NDBG(p->a->group_id, x));
        assert(get_site_def());
      }
      if (p->a && p->a->body.c_t == set_event_horizon_type) {
        IFDBG(D_NONE, FN; STRLIT("Got set_event_horizon from client");
              SYCEXP(p->synode););
        IFDBG(D_NONE, FN; NDBG(p->a->body.app_u_u.event_horizon, u));
        IFDBG(D_NONE, STRLIT("handle_client_msg "); NDBG(p->a->group_id, x));
        assert(get_site_def());
      }
      if (p->a && p->a->body.c_t == force_config_type) {
        IFDBG(D_NONE, FN; STRLIT("Got new force config from client");
              SYCEXP(p->synode););
        IFDBG(D_NONE, FN;
              COPY_AND_FREE_GOUT(dbg_list(&p->a->body.app_u_u.nodes)););
        IFDBG(D_NONE, STRLIT("handle_client_msg "); NDBG(p->a->group_id, x));
        assert(get_site_def());
        XCOM_FSM(x_fsm_force_config, void_arg(p->a));
      }
      handle_client_msg(p);
      break;
    case initial_op:
      break;
    case read_op:
      pm = get_cache(p->synode);
      assert(pm);

      handle_read(site, pm, reply_queue, p);
      break;
    case prepare_op:
      pm = get_cache(p->synode);
      assert(pm);
      if (p->force_delivery) pm->force_delivery = 1;
      IFDBG(D_NONE, FN; dbg_pax_msg(p));

      /*
       We can only be a productive Paxos Acceptor if we have been booted, i.e.
       added to the group and received an up-to-date snapshot from some member.

       We do not allow non-booted members to participate in Paxos because they
       might be a reincarnation of a member that crashed and was then brought up
       without having gone through the remove+add node path.
       Since the pre-crash incarnation may have accepted a value for a given
       synod but the post-crash incarnation has forgotten that fact, the
       post-crash incarnation will fail to propagate the previously accepted
       value to a higher ballot. Since majorities can overlap on a single node,
       if the overlap node is the post-crash incarnation which has forgotten
       about the previously accepted value, the higher ballot proposer may get
       a different value accepted, leading to conflicting values to be accepted
       for different proposers, which is a violation of the safety requirements
       of the Paxos protocol.
      */
      if (ALWAYS_HANDLE_CONSENSUS || client_boot_done) {
        handle_prepare(site, pm, reply_queue, p);
      }
      break;
    case ack_prepare_op:
    case ack_prepare_empty_op:
      if (in_front || !is_cached(p->synode)) break;
      pm = get_cache(p->synode);
      if (p->force_delivery) pm->force_delivery = 1;
      if (!pm->proposer.msg) break;
      assert(pm && pm->proposer.msg);
      handle_ack_prepare(site, pm, p);
      break;
    case accept_op:
      pm = get_cache(p->synode);
      assert(pm);
      if (p->force_delivery) pm->force_delivery = 1;
      IFDBG(D_NONE, FN; dbg_pax_msg(p));

      /*
       We can only be a productive Paxos Acceptor if we have been booted, i.e.
       added to the group and received an up-to-date snapshot from some member.

       We do not allow non-booted members to participate in Paxos because they
       might be a reincarnation of a member that crashed and was then brought up
       without having gone through the remove+add node path.
       Since the pre-crash incarnation may have accepted a value for a given
       synod but the post-crash incarnation has forgotten that fact, the
       post-crash incarnation will fail to propagate the previously accepted
       value to a higher ballot. Since majorities can overlap on a single node,
       if the overlap node is the post-crash incarnation which has forgotten
       about the previously accepted value, the higher ballot proposer may get
       a different value accepted, leading to conflicting values to be accepted
       for different proposers, which is a violation of the safety requirements
       of the Paxos protocol.
      */
      if (ALWAYS_HANDLE_CONSENSUS || client_boot_done) {
        handle_alive(site, reply_queue, p);

        handle_accept(site, pm, reply_queue, p);
      }
      break;
    case ack_accept_op:
      if (in_front || !is_cached(p->synode)) break;
      pm = get_cache(p->synode);
      if (p->force_delivery) pm->force_delivery = 1;
      if (!pm->proposer.msg) break;
      assert(pm && pm->proposer.msg);
      handle_ack_accept(site, pm, p);
      break;
    case recover_learn_op:
      IFDBG(D_NONE, FN; STRLIT("recover_learn_op receive "); SYCEXP(p->synode));
      pm = get_cache(p->synode);
      assert(pm);
      if (p->force_delivery) pm->force_delivery = 1;
      update_max_synode(p);
      {
        IFDBG(D_NONE, FN; STRLIT("recover_learn_op learn "); SYCEXP(p->synode));
        p->op = learn_op;
        handle_learn(site, pm, p);
      }
      break;
    case learn_op:
    learnop:
      pm = get_cache(p->synode);
      assert(pm);
      if (p->force_delivery) pm->force_delivery = 1;
      update_max_synode(p);
      handle_learn(site, pm, p);
      break;
    case tiny_learn_op:
      if (p->msg_type == no_op) goto learnop;
      pm = get_cache(p->synode);
      assert(pm);
      if (p->force_delivery) pm->force_delivery = 1;
      handle_tiny_learn(site, pm, p);
      break;
    case skip_op:
      pm = get_cache(p->synode);
      assert(pm);
      if (p->force_delivery) pm->force_delivery = 1;
      handle_skip(site, pm, p);
      break;
    case i_am_alive_op:
      /* Update max_synode, but use only p->max_synode, ignore p->synode */
      if (!is_dead_site(p->group_id)) {
        if (max_synode.group_id == p->synode.group_id &&
            synode_gt(p->max_synode, max_synode)) {
          set_max_synode(p->max_synode);
        }
      }
      handle_alive(site, reply_queue, p);
      break;
    case are_you_alive_op:
      handle_alive(site, reply_queue, p);
      break;
    case need_boot_op:
      /* purecov: begin deadcode */
      /* Only in run state. Test state and do it here because we need to use
       * reply queue */
      if (can_send_snapshot() &&
          !synode_eq(get_site_def()->boot_key, null_synode)) {
        handle_boot(site, reply_queue, p);
      }
      /* Wake senders waiting to connect, since new node has appeared */
      wakeup_sender();
      break;
    /* purecov: end */
    case gcs_snapshot_op:
      /* Avoid duplicate snapshots and snapshots from zombies */
      IFDBG(D_BUG, FN; SYCEXP(executed_msg););
      IFDBG(D_BUG, FN; SYCEXP(start_config););
      if (!synode_eq(start_config, get_highest_boot_key(p->gcs_snap)) &&
          !is_dead_site(p->group_id)) {
        update_max_synode(p);
        /* For incoming messages, note delivery of snapshot from sender node */
        note_snapshot(p->from);
        XCOM_FSM(x_fsm_snapshot, void_arg(p->gcs_snap));
      }
      break;
    case die_op:
      /* assert("die horribly" == "need coredump"); */
      {
        GET_GOUT;
        FN;
        STRLIT("die_op ");
        SYCEXP(executed_msg);
        SYCEXP(delivered_msg);
        SYCEXP(p->synode);
        SYCEXP(p->delivered_msg);
        SYCEXP(p->max_synode);
        PRINT_GOUT;
        FREE_GOUT;
      }
      /*
      If the message with the number in  the  incoming  die_op  message
      already  has  been  executed  (delivered),  then it means that we
      actually got consensus on it, since otherwise we would  not  have
      delivered it.Such a situation could arise if one of the nodes has
      expelled the message from its cache, but others have not. So when
      sending  out  a  request, we might get two different answers, one
      indicating that we are too far behind  and  should  restart,  and
      another  with  the  actual  consensus value. If the value arrives
      first, we will deliver it, and then the die_op may arrive  later.
      But  it this case it does not matter, since we got what we needed
      anyway. It is only a partial guard against exiting without really
      needing  it  of course, since the die_op may arrive first, and we
      do not wait for a die_op from all the other nodes.  We  could  do
      that  with  some extra housekeeping in the pax_machine (a new bit
      vector), but I am not convinced that it is worth the effort.
      */
      if (!synode_lt(p->synode, executed_msg)) {
        ADD_DBG(D_FSM, add_event(EVENT_DUMP_PAD, string_arg("terminating"));)
        g_critical("Node %u is unable to get message {%x %" PRIu64
                   " %u}, since the group is too far "
                   "ahead. Node will now exit.",
                   get_nodeno(site), SY_MEM(p->synode));
        terminate_and_exit();
      }
    default:
      break;
  }
  if (oom_abort) {
    g_critical("Node %u has run out of memory and will now exit.",
               get_nodeno(site));
    terminate_and_exit();
  }
  return (p);
}

/* Acceptor-learner task */
#define SERIALIZE_REPLY(msg)                \
  msg->to = ep->p->from;                    \
  msg->from = ep->p->to;                    \
  msg->delivered_msg = get_delivered_msg(); \
  msg->max_synode = get_max_synode();       \
  serialize_msg(msg, ep->rfd.x_proto, &ep->buflen, &ep->buf);

#define WRITE_REPLY                                                    \
  if (ep->buflen) {                                                    \
    int64_t sent;                                                      \
    IFDBG(D_TRANSPORT, FN; STRLIT("task_write "); NDBG(ep->rfd.fd, d); \
          NDBG(ep->buflen, u));                                        \
    TASK_CALL(task_write(&ep->rfd, ep->buf, ep->buflen, &sent));       \
    send_count[ep->p->op]++;                                           \
    send_bytes[ep->p->op] += ep->buflen;                               \
    X_FREE(ep->buf);                                                   \
  }                                                                    \
  ep->buf = NULL;

static inline void update_srv(server **target, server *srv) {
  if (srv) srv_ref(srv);
  if (*target) srv_unref(*target);
  *target = srv;
}

/* A message is harmless if it cannot change the outcome of a consensus round.
 * learn_op does change the value, but we trust that the sender has correctly
 * derived the value from a majority of the acceptors, so in that sense it is
 * harmless. */
static int harmless(pax_msg const *p) {
  if (p->synode.msgno == 0) return 1;
  switch (p->op) {
    case i_am_alive_op:
    case are_you_alive_op:
    case need_boot_op:
    case gcs_snapshot_op:
    case learn_op:
    case recover_learn_op:
    case tiny_learn_op:
    case die_op:
      return 1;
    default:
      return 0;
  }
}

static int wait_for_cache(pax_machine **pm, synode_no synode, double timeout) {
  DECL_ENV
  double now;
  END_ENV;

  TASK_BEGIN
  ep->now = task_now();
  while ((*pm = get_cache(synode)) == NULL) {
    /* Wait for executor to make progress */
    TIMED_TASK_WAIT(&exec_wait, 0.5);
    if (task_now() - ep->now > timeout) break; /* Timeout, return NULL. */
  }
  FINALLY
  TASK_END;
}

/*
  Verify if we need to poll the cache before calling dispatch_op.
  Avoid waiting for a machine if it is not going to be used.
 */
static bool_t should_poll_cache(pax_op op) {
  if (op == die_op || op == gcs_snapshot_op || op == initial_op ||
      op == client_msg)
    return FALSE;
  return TRUE;
}

static int msdiff(double current, double time) {
  return (int)(1000.5 * (current - time));
}

int acceptor_learner_task(task_arg arg) {
  DECL_ENV
  connection_descriptor rfd;
  srv_buf *in_buf;

  pax_msg *p;
  u_int buflen;
  char *buf;
  linkage reply_queue;
  int errors;
  server *srv;
  site_def const *site;
  int behind, time_diff;
  int loop_counter;
  double last_record_time;
  END_ENV;

  TASK_BEGIN

  ep->in_buf = (srv_buf *)calloc(1, sizeof(srv_buf));

  ep->rfd.fd = get_int_arg(arg);
#ifndef XCOM_WITHOUT_OPENSSL
  ep->rfd.ssl_fd = 0;
#endif
  ep->p = NULL;
  ep->buflen = 0;
  ep->buf = NULL;
  ep->errors = 0;
  ep->srv = 0;
  ep->behind = FALSE;

  /* We have a connection, make socket non-blocking and wait for request */
  unblock_fd(ep->rfd.fd);
  set_nodelay(ep->rfd.fd);
  wait_io(stack, ep->rfd.fd, 'r');
  TASK_YIELD;

#ifndef XCOM_WITHOUT_OPENSSL
  if (xcom_use_ssl()) {
    ep->rfd.ssl_fd = SSL_new(server_ctx);
    SSL_set_fd(ep->rfd.ssl_fd, ep->rfd.fd);

    {
      int ret_ssl;
      int err;
      ERR_clear_error();
      ret_ssl = SSL_accept(ep->rfd.ssl_fd);
      err = SSL_get_error(ep->rfd.ssl_fd, ret_ssl);

      while (ret_ssl != SSL_SUCCESS) {
        if (err == SSL_ERROR_WANT_READ) {
          wait_io(stack, ep->rfd.fd, 'r');
        } else if (err == SSL_ERROR_WANT_WRITE) {
          wait_io(stack, ep->rfd.fd, 'w');
        } else { /* Some other error, give up */
          break;
        }
        TASK_YIELD;
        SET_OS_ERR(0);
        G_DEBUG("acceptor learner accept retry fd %d", ep->rfd.fd);
        ERR_clear_error();
        ret_ssl = SSL_accept(ep->rfd.ssl_fd);
        err = SSL_get_error(ep->rfd.ssl_fd, ret_ssl);
      }

      if (ret_ssl != SSL_SUCCESS) {
        ssl_free_con(&ep->rfd);
        close_connection(&ep->rfd);
        TERMINATE;
      }
    }

  } else {
    ep->rfd.ssl_fd = 0;
  }
#endif
  set_connected(&ep->rfd, CON_FD);
  G_INFO("set fd:%d connected", ep->rfd.fd)
  link_init(&ep->reply_queue, TYPE_HASH("msg_link"));
  ep->last_record_time = task_now();
  ep->loop_counter = 0;

again:
  while (!xcom_shutdown) {
    int64_t n;
    ep->site = 0;
    unchecked_replace_pax_msg(&ep->p, pax_msg_new_0(null_synode));

    if (use_buffered_read) {
      TASK_CALL(buffered_read_msg(&ep->rfd, ep->in_buf, ep->p, ep->srv, &n));
    } else {
      TASK_CALL(read_msg(&ep->rfd, ep->p, ep->srv, &n));
    }
    ADD_DBG(D_NONE, add_synode_event(ep->p->synode);
            add_event(EVENT_DUMP_PAD, string_arg("ep->p->from"));
            add_event(EVENT_DUMP_PAD, uint_arg(ep->p->from));
            add_event(EVENT_DUMP_PAD, string_arg(pax_op_to_str(ep->p->op))););

    if (ep->srv && !ep->srv->invalid && ((int)ep->p->op != (int)client_msg) &&
        is_connected(&ep->srv->con))
      server_detected(ep->srv);

    if (((int)ep->p->op < (int)client_msg || ep->p->op > LAST_OP)) {
      /* invalid operation, ignore message */
      delete_pax_msg(ep->p);
      ep->p = NULL;
      TASK_YIELD;
      ep->last_record_time = task_now();
      continue;
    }
    if (n <= 0) {
      break;
    }
    ep->site = find_site_def(ep->p->synode);

    /* Handle this connection on a local_server task instead of this
       acceptor_learner_task task. */
    if (ep->p->op == client_msg && ep->p->a &&
        ep->p->a->body.c_t == convert_into_local_server_type) {
      if (local_server_is_setup()) {
        /* Launch local_server task to handle this connection. */
        {
          connection_descriptor *con =
              (connection_descriptor *)malloc(sizeof(connection_descriptor));
          *con = ep->rfd;
          task_new(local_server, void_arg(con), "local_server",
                   XCOM_THREAD_DEBUG);
        }
      }
      /* Reply to client:
         - OK if local_server task is setup, or
         - FAIL otherwise. */
      {
        CREATE_REPLY(ep->p);
        reply->op = xcom_client_reply;
        reply->cli_err = local_server_is_setup() ? REQUEST_OK : REQUEST_FAIL;
        SERIALIZE_REPLY(reply);
        replace_pax_msg(&reply, NULL);
      }
      WRITE_REPLY;
      delete_pax_msg(ep->p);
      ep->p = NULL;
      if (local_server_is_setup()) {
        /* Relinquish ownership of the connection. It is now onwed by the
           launched local_server task. */
        reset_connection(&ep->rfd);
      }
      /* Terminate this task. */
      TERMINATE;
    }

    /*
      Getting a pointer to the server needs to be done after we have
      received a message, since without having received a message, we
      cannot know who it is from. We could peek at the message and de‐
      serialize the message number and from field, but since the server
      does not change, it should be sufficient to cache the server in
      the acceptor_learner task. A cleaner solution would have been to
      move the timestamps out of the server object, and have a map in‐
      dexed by IP/port or UUID to track the timestamps, since this is
      common to both the sender_task, reply_handler_task,  and the ac‐
      ceptor_learner_task.
    */
    update_srv(&ep->srv, get_server(ep->site, ep->p->from));
    ep->p->refcnt = 1; /* Refcnt from other end is void here */
    IFDBG(D_NONE, FN; NDBG(ep->rfd.fd, d); NDBG(task_now(), f);
          COPY_AND_FREE_GOUT(dbg_pax_msg(ep->p)););
    receive_count[ep->p->op]++;
    receive_bytes[ep->p->op] += (uint64_t)n + MSG_HDR_SIZE;
    {
      if (get_maxnodes(ep->site) > 0) {
        ep->behind = ep->p->synode.msgno < delivered_msg.msgno;
      }
      ADD_DBG(D_BASE, add_event(EVENT_DUMP_PAD, string_arg("before dispatch "));
              add_synode_event(ep->p->synode);
              add_event(EVENT_DUMP_PAD, string_arg("ep->p->from"));
              add_event(EVENT_DUMP_PAD, uint_arg(ep->p->from));
              add_event(EVENT_DUMP_PAD, string_arg(pax_op_to_str(ep->p->op)));
              add_event(EVENT_DUMP_PAD,
                        string_arg(pax_msg_type_to_str(ep->p->msg_type)));
              add_event(EVENT_DUMP_PAD, string_arg("is_cached(ep->p->synode)"));
              add_event(EVENT_DUMP_PAD, int_arg(is_cached(ep->p->synode)));
              add_event(EVENT_DUMP_PAD, string_arg("behind"));
              add_event(EVENT_DUMP_PAD, int_arg(ep->behind)););
      /* Special treatment to see if synode number is valid. Return no-op if
       * not. */
      if (ep->p->op == read_op || ep->p->op == prepare_op ||
          ep->p->op == accept_op) {
        if (ep->site) {
          ADD_DBG(
              D_BASE, add_event(EVENT_DUMP_PAD, string_arg("ep->p->synode"));
              add_synode_event(ep->p->synode);
              add_event(EVENT_DUMP_PAD, string_arg("ep->site->start"));
              add_synode_event(ep->site->start); add_event(
                  EVENT_DUMP_PAD, string_arg("ep->site->nodes.node_list_len"));
              add_event(EVENT_DUMP_PAD,
                        uint_arg(ep->site->nodes.node_list_len)););
          if (ep->p->synode.node >= ep->site->nodes.node_list_len) {
            {
              CREATE_REPLY(ep->p);
              create_noop(reply);
              set_learn_type(reply);
              SERIALIZE_REPLY(reply);
              delete_pax_msg(reply); /* Deallocate BEFORE potentially blocking
                                        call which will lose value of reply */
            }
            WRITE_REPLY;
            goto again;
          }
        }
      }
      /* Reject any message that might compromise the integrity of a consensus
       * instance. We do this by not processing any message which may change the
       * outcome if the consensus instance has been evicted from the cache */
      if (harmless(ep->p) ||          /* Harmless message */
          is_cached(ep->p->synode) || /* Already in cache */
          (!ep->behind)) { /* Guard against cache pollution from other nodes */

        if (should_poll_cache(ep->p->op)) {
          pax_machine *pm;
          TASK_CALL(wait_for_cache(&pm, ep->p->synode, 10));
          if (!pm) continue; /* Could not get a machine, discarding message. */
        }

        dispatch_op(ep->site, ep->p, &ep->reply_queue);

        /* Send replies on same fd */
        while (!link_empty(&ep->reply_queue)) {
          {
            msg_link *reply =
                (msg_link *)(link_extract_first(&ep->reply_queue));
            IFDBG(D_DISPATCH, FN; PTREXP(reply);
                  COPY_AND_FREE_GOUT(dbg_linkage(&ep->reply_queue));
                  COPY_AND_FREE_GOUT(dbg_msg_link(reply));
                  COPY_AND_FREE_GOUT(dbg_pax_msg(reply->p)););
            assert(reply->p);
            assert(reply->p->refcnt > 0);
            IFDBG(D_DISPATCH, FN; STRLIT("serialize "); PTREXP(reply));
            SERIALIZE_REPLY(reply->p);
            msg_link_delete(&reply); /* Deallocate BEFORE potentially blocking
                                        call which will lose value of reply */
          }
          WRITE_REPLY;
        }
      } else {
        IFDBG(D_EXEC, FN; STRLIT("rejecting ");
              STRLIT(pax_op_to_str(ep->p->op)); NDBG(ep->p->from, d);
              NDBG(ep->p->to, d); SYCEXP(ep->p->synode);
              BALCEXP(ep->p->proposal));
        if (/* xcom_booted() && */ ep->behind) {
          if (/*ep->p->op == prepare_op && */ was_removed_from_cache(
              ep->p->synode)) {
            IFDBG(D_NONE, FN; STRLIT("send_die ");
                  STRLIT(pax_op_to_str(ep->p->op)); NDBG(ep->p->from, d);
                  NDBG(ep->p->to, d); SYCEXP(ep->p->synode);
                  BALCEXP(ep->p->proposal));
            if (get_maxnodes(ep->site) > 0) {
              {
                pax_msg *np = NULL;
                np = pax_msg_new(ep->p->synode, ep->site);
                np->op = die_op;
                SERIALIZE_REPLY(np);
                IFDBG(D_NONE, FN; STRLIT("sending die_op to node ");
                      NDBG(np->to, d); SYCEXP(executed_msg); SYCEXP(max_synode);
                      SYCEXP(np->synode));
                delete_pax_msg(np); /* Deallocate BEFORE potentially blocking
                                   call which will lose value of np */
              }
              WRITE_REPLY;
            }
          }
        }
      }
    }

    ep->loop_counter++;
    ep->time_diff = msdiff(task_now(), ep->last_record_time);
    if (ep->time_diff >= 10) {
      TASK_YIELD;
      ep->last_record_time = task_now();
      ep->loop_counter = 0;
    } else {
      if (ep->time_diff == 0) {
        if (ep->loop_counter == 10) {
          /* update time */
          (void)seconds();
          ep->loop_counter = 0;
        }
      }
    }
  }

  FINALLY
  IFDBG(D_BUG, FN; STRLIT(" shutdown "); NDBG(ep->rfd.fd, d);
        NDBG(task_now(), f));
  if (ep->reply_queue.suc && !link_empty(&ep->reply_queue))
    empty_msg_list(&ep->reply_queue);
  unchecked_replace_pax_msg(&ep->p, NULL);
  shutdown_connection(&ep->rfd);
  IFDBG(D_NONE, FN; NDBG(xcom_shutdown, d));
  if (ep->buf) X_FREE(ep->buf);
  free(ep->in_buf);

  /* Unref srv to avoid leak */
  update_srv(&ep->srv, 0);

  IFDBG(D_BUG, FN; STRLIT(" shutdown completed"); NDBG(ep->rfd.fd, d);
        NDBG(task_now(), f));
  TASK_END;
}

/* Reply handler task */

static void server_handle_need_snapshot(server *srv, site_def const *s,
                                        node_no node);

int reply_handler_task(task_arg arg) {
  DECL_ENV
  server *s;
  pax_msg *reply;
  double dtime;
  END_ENV;

  TASK_BEGIN

  ep->dtime = INITIAL_CONNECT_WAIT; /* Initial wait is short, to avoid
                                       unnecessary waiting */
  ep->s = (server *)get_void_arg(arg);
  srv_ref(ep->s);
  ep->reply = NULL;

  while (!xcom_shutdown) {
    while (!is_connected(&ep->s->con)) {
      IFDBG(D_NONE, FN; STRLIT("waiting for connection"));
      TASK_DELAY(ep->dtime);
      if (xcom_shutdown) {
        TERMINATE;
      }
      ep->dtime *= CONNECT_WAIT_INCREASE; /* Increase wait time for next try */
      if (ep->dtime > MAX_CONNECT_WAIT) {
        ep->dtime = MAX_CONNECT_WAIT;
      }
    }
    ep->dtime = INITIAL_CONNECT_WAIT;
    {
      int64_t n;
      unchecked_replace_pax_msg(&ep->reply, pax_msg_new_0(null_synode));

      ADD_DBG(D_NONE, add_event(EVENT_DUMP_PAD, string_arg("ep->s->con.fd"));
              add_event(EVENT_DUMP_PAD, int_arg(ep->s->con.fd)););
      TASK_CALL(read_msg(&ep->s->con, ep->reply, ep->s, &n));
      ADD_DBG(D_NONE, add_event(EVENT_DUMP_PAD, string_arg("ep->s->con.fd"));
              add_event(EVENT_DUMP_PAD, int_arg(ep->s->con.fd)););
      ep->reply->refcnt = 1; /* Refcnt from other end is void here */
      if (n <= 0) {
        shutdown_connection(&ep->s->con);
        ep->s->unreachable = DIRECT_ABORT_CONN;
        continue;
      }
      receive_bytes[ep->reply->op] += (uint64_t)n + MSG_HDR_SIZE;
    }
    IFDBG(D_NONE, FN; NDBG(ep->s->con.fd, d); NDBG(task_now(), f);
          COPY_AND_FREE_GOUT(dbg_pax_msg(ep->reply)););
    receive_count[ep->reply->op]++;

    ADD_DBG(D_NONE, add_synode_event(ep->reply->synode);
            add_event(EVENT_DUMP_PAD, string_arg("ep->reply->from"));
            add_event(EVENT_DUMP_PAD, uint_arg(ep->reply->from));
            add_event(EVENT_DUMP_PAD, string_arg(pax_op_to_str(ep->reply->op)));
            add_event(EVENT_DUMP_PAD, string_arg("get_site_def()->boot_key"));
            add_synode_event(get_site_def()->boot_key););
    /* Special test for need_snapshot, since node and site may not be consistent
     */
    if (ep->reply->op == need_boot_op &&
        !synode_eq(get_site_def()->boot_key, null_synode)) {
      pax_msg *p = ep->reply;

      ADD_DBG(D_BASE,
              add_event(EVENT_DUMP_PAD,
                        string_arg("calling server_handle_need_snapshot")););
      if (should_handle_need_boot(find_site_def(p->synode), p)) {
        server_handle_need_snapshot(ep->s, find_site_def(p->synode), p->from);
        /* Wake senders waiting to connect, since new node has appeared */
        wakeup_sender();
      } else {
        G_INFO("we should not process the incoming need_boot_op message");
        ep->s->invalid = 1;
      }
    } else {
      /* We only handle messages from this connection if the server is valid. */
      if (ep->s->invalid == 0)
        dispatch_op(find_site_def(ep->reply->synode), ep->reply, NULL);
    }
    TASK_YIELD;
  }

  FINALLY
  replace_pax_msg(&ep->reply, NULL);

  shutdown_connection(&ep->s->con);
  ep->s->reply_handler = NULL;
  IFDBG(D_BUG, FN; STRLIT(" shutdown "); NDBG(ep->s->con.fd, d);
        NDBG(task_now(), f));
  srv_unref(ep->s);

  TASK_END;
}

/* purecov: begin deadcode */
static inline void xcom_sleep(unsigned int seconds) {
#if defined(_WIN32)
  Sleep((DWORD)seconds * 1000); /* windows sleep takes milliseconds */
#else
  sleep(seconds);
#endif
}
/* purecov: end */

static uint64_t get_time_usec() {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  uint64_t sec = tp.tv_sec * 1000000 + tp.tv_usec;
  return sec;
}

/*
 * Get a unique long as the basis for XCom group id creation.
 *
 * NOTE:
 * As there is no gethostid() on win, we use seconds since epoch instead,
 * so it might fail if you try simultaneous create sites at the same second.
 */
long xcom_unique_long(void) {
#if defined(_WIN32)
  __time64_t ltime;

  _time64(&ltime);
  return (long)(ltime ^ GetCurrentProcessId());
#else
  return (long)(get_time_usec() ^ getpid());
#endif
}

app_data_ptr init_config_with_group(app_data *a, node_list *nl, cargo_type type,
                                    uint32_t group_id) {
  init_app_data(a);
  a->app_key.group_id = a->group_id = group_id;
  a->body.c_t = type;
  init_node_list(nl->node_list_len, nl->node_list_val, &a->body.app_u_u.nodes);
  return a;
}

app_data_ptr init_set_event_horizon_msg(app_data *a, uint32_t group_id,
                                        xcom_event_horizon event_horizon) {
  init_app_data(a);
  a->app_key.group_id = a->group_id = group_id;
  a->body.c_t = set_event_horizon_type;
  a->body.app_u_u.event_horizon = event_horizon;
  return a;
}

app_data_ptr init_get_event_horizon_msg(app_data *a, uint32_t group_id) {
  init_app_data(a);
  a->app_key.group_id = a->group_id = group_id;
  a->body.c_t = get_event_horizon_type;
  return a;
}

app_data_ptr init_app_msg(app_data *a, char *payload, u_int payload_size) {
  init_app_data(a);
  a->body.c_t = app_type;
  a->body.app_u_u.data.data_val = payload; /* Takes ownership of payload. */
  a->body.app_u_u.data.data_len = payload_size;
  return a;
}

app_data_ptr init_terminate_command(app_data *a) {
  init_app_data(a);
  a->body.c_t = x_terminate_and_exit;
  return a;
}

static app_data_ptr init_get_synode_app_data_msg(
    app_data *a, uint32_t group_id, synode_no_array *const synodes) {
  init_app_data(a);
  a->app_key.group_id = a->group_id = group_id;
  a->body.c_t = get_synode_app_data_type;
  /* Move synodes (as in C++ move semantics) into a->body.app_u_u.synodes. */
  synode_array_move(&a->body.app_u_u.synodes, synodes);
  return a;
}

app_data_ptr init_set_cache_size_msg(app_data *a, uint64_t cache_limit) {
  init_app_data(a);
  a->body.c_t = set_cache_limit;
  a->body.app_u_u.cache_limit = cache_limit;
  return a;
}

app_data_ptr init_set_flp_timeout_msg(app_data *a, uint64_t flp_timeout) {
  init_app_data(a);
  a->body.c_t = set_flp_timeout;
  a->body.app_u_u.flp_timeout = flp_timeout;
  return a;
}

app_data_ptr init_convert_into_local_server_msg(app_data *a) {
  init_app_data(a);
  a->body.c_t = convert_into_local_server_type;
  return a;
}

static void server_send_snapshot(server *srv, site_def const *s,
                                 gcs_snapshot *gcs_snap, node_no node) {
  pax_msg *p = pax_msg_new(gcs_snap->log_start, get_site_def());
  ref_msg(p);
  p->op = gcs_snapshot_op;
  p->gcs_snap = gcs_snap;
  send_msg(srv, s->nodeno, node, get_group_id(s), p);
  unref_msg(&p);
}

static void server_push_log(server *srv, synode_no push, node_no node) {
  site_def const *s = get_site_def();
  if (srv && s) {
    while (!synode_gt(push, get_max_synode())) {
      if (is_cached(push)) {
        /* Need to clone message here since pax_machine may be re-used while
         * message is sent */
        pax_machine *p = get_cache_no_touch(push, FALSE);
        if (pm_finished(p)) {
          pax_msg *pm = clone_pax_msg(p->learner.msg);
          if (pm != NULL) {
            ref_msg(pm);
            pm->op = recover_learn_op;
            IFDBG(D_NONE, FN; PTREXP(srv); PTREXP(s););
            send_msg(srv, s->nodeno, node, get_group_id(s), pm);
            unref_msg(&pm);
          }
        }
      }
      push = incr_synode(push);
    }
  }
}

/* purecov: begin deadcode */
static void reply_push_log(synode_no push, linkage *reply_queue) {
  while (!synode_gt(push, get_max_synode())) {
    if (is_cached(push)) {
      /* Need to clone message here since pax_machine may be re-used while
       * message is sent */
      pax_machine *p = get_cache_no_touch(push, FALSE);
      if (pm_finished(p)) {
        pax_msg *reply = clone_pax_msg(p->learner.msg);
        ref_msg(reply);
        reply->op = recover_learn_op;
        {
          msg_link *msg_x = msg_link_new(reply, reply->from);
          IFDBG(D_NONE, FN; PTREXP(msg_x));
          link_into(&(msg_x->l), reply_queue);
        }
        replace_pax_msg(&reply, NULL);
        unref_msg(&reply);
      }
    }
    push = incr_synode(push);
  }
}
/* purecov: end */

static app_snap_getter get_app_snap_cb;
static app_snap_handler handle_app_snap_cb;

static gcs_snapshot *create_snapshot() {
  gcs_snapshot *gs = 0;
  if (get_app_snap_cb) {
    /* purecov: begin deadcode */
    blob app_snap = {
        {0,
         0}}; /* Initialize in case get_app_snap_cb does not assign a value */
    synode_no app_lsn = get_app_snap_cb(&app_snap);

    /* We have a valid callback, abort if it did not return anything */
    if (app_snap.data.data_len == 0) {
      ADD_DBG(D_BASE,
              add_event(EVENT_DUMP_PAD, string_arg("no data, return")););
      return 0;
    }
    gs = export_config();
    if (!gs) return 0;
    ADD_DBG(D_BASE, add_event(EVENT_DUMP_PAD, string_arg("export config ok")););
    gs->app_snap = app_snap;
    IFDBG(D_BUG, FN; SYCEXP(app_lsn); SYCEXP(gs->log_start);
          SYCEXP(gs->log_end));

    /* Set starting point of log to match the snapshot */
    /* If we have a valid synode from application snapshot, see if it should be
     * used */
    if (!synode_eq(null_synode, app_lsn)) {
      /* If log_start is null_synode, always use valid synode from application
       * snapshot */
      if (synode_eq(null_synode, gs->log_start) ||
          !synode_gt(app_lsn, gs->log_start)) {
        gs->log_start = app_lsn;
        IFDBG(D_BUG, FN; STRLIT("using "); SYCEXP(app_lsn));
      }
    }
    ADD_DBG(D_BASE, add_event(EVENT_DUMP_PAD, string_arg("gs->log_start"));
            add_synode_event(gs->log_start);
            add_event(EVENT_DUMP_PAD, string_arg("gs->log_end"));
            add_synode_event(gs->log_end););
    /* purecov: end */
  } else {
    gs = export_config();
    if (!gs) return 0;
    ADD_DBG(D_BASE, add_event(EVENT_DUMP_PAD, string_arg("export config ok")););
    if (!synode_eq(null_synode, last_config_modification_id)) {
      /* No valid valid synode from application snapshot, use
       * last_config_modification_id if not null_synode */
      gs->log_start = last_config_modification_id;
      IFDBG(D_BUG, FN; STRLIT("using "); SYCEXP(last_config_modification_id));
    }
    IFDBG(D_BUG, FN; SYCEXP(gs->log_start); SYCEXP(gs->log_end));
    ADD_DBG(D_BASE, add_event(EVENT_DUMP_PAD, string_arg("gs->log_start"));
            add_synode_event(gs->log_start);
            add_event(EVENT_DUMP_PAD, string_arg("gs->log_end"));
            add_synode_event(gs->log_end););
  }
  IFDBG(D_BUG, FN; SYCEXP(gs->log_start); SYCEXP(gs->log_end));
  return gs;
}

/* purecov: begin deadcode */
static void handle_need_snapshot(linkage *reply_queue, pax_msg *pm) {
  gcs_snapshot *gs = create_snapshot();
  if (gs) {
    pax_msg *reply = clone_pax_msg(pm);
    ref_msg(reply);
    reply->op = gcs_snapshot_op;
    reply->gcs_snap = gs;
    {
      msg_link *msg_x = msg_link_new(reply, reply->from);
      IFDBG(D_NONE, FN; PTREXP(msg_x));
      link_into(&(msg_x->l), reply_queue);
    }
    unref_msg(&reply);
    IFDBG(D_NONE, FN; STRLIT("sent snapshot"););
    reply_push_log(gs->log_start, reply_queue);
    send_global_view();
  }
}
/* purecov: end */

static task_env *x_timer = NULL;

/* Timer for use with the xcom FSM. Will deliver x_fsm_timeout */
static int xcom_timer(task_arg arg) {
  DECL_ENV
  double t;
  END_ENV;

  TASK_BEGIN

  ep->t = get_double_arg(arg);
  TASK_DELAY(ep->t);
  XCOM_FSM(x_fsm_timeout, double_arg(ep->t));
  FINALLY
  if (stack == x_timer) set_task(&x_timer, NULL);
  IFDBG(D_BUG, FN; STRLIT(" timeout "));
  TASK_END;
}

/* Stop the xcom FSM timer */
static void stop_x_timer() {
  if (x_timer) {
    task_terminate(x_timer);
    set_task(&x_timer, NULL);
  }
}

/* Start the xcom FSM timer */
static void start_x_timer(double t) {
  stop_x_timer();
  set_task(&x_timer, task_new(xcom_timer, double_arg(t), "xcom_timer",
                              XCOM_THREAD_DEBUG));
}

/* Deliver x_fsm_complete to xcom FSM */
/* purecov: begin deadcode */
static int x_fsm_completion_task(task_arg arg) {
  DECL_ENV
  int dummy;
  END_ENV;

  TASK_BEGIN

      (void)
  arg;
  XCOM_FSM(x_fsm_complete, null_arg);
  FINALLY
  IFDBG(D_BUG, FN; STRLIT(" delivered "));
  TASK_END;
}
/* purecov: end */

/* Send x_fsm_complete to xcom FSM in the context of the xcom thread. The
 * calling thread and the xcom thread must be in a rendezvous. Using a task to
 * deliver a message is an abstraction inversion, but it's the simplest solution
 * until we get a proper queue-based communication system going. */
/* purecov: begin deadcode */
void send_x_fsm_complete() {
  task_new(x_fsm_completion_task, null_arg, "x_fsm_completion_task",
           XCOM_THREAD_DEBUG);
}
/* purecov: end */

static void server_handle_need_snapshot(server *srv, site_def const *s,
                                        node_no node) {
  gcs_snapshot *gs = create_snapshot();

  if (gs) {
    server_send_snapshot(srv, s, gs, node);
    IFDBG(D_NONE, FN; STRLIT("sent snapshot"););
    server_push_log(srv, gs->log_start, node);
    send_global_view();
  }
}

#define X(b) #b
const char *xcom_actions_name[] = {x_actions};
#undef X

static int snapshots[NSERVERS];

/* Note that we have received snapshot from node */
static void note_snapshot(node_no node) {
  if (node != VOID_NODE_NO) {
    snapshots[node] = 1;
  }
}

/* Reset set of received snapshots */
static void reset_snapshot_mask() {
  int i;
  for (i = 0; i < NSERVERS; i++) {
    snapshots[i] = 0;
  }
}

/* See if we have got a snapshot from every node */
static int got_all_snapshots() {
  node_no i;
  node_no max = get_maxnodes(get_site_def());
  if (0 == max) {
    return 0;
  }
  for (i = 0; i < max; i++) {
    if (!snapshots[i]) {
      return 0;
    }
  }
  return 1;
}

static synode_no log_start_max; /* Initialized by xcom_fsm */
static synode_no log_end_max;   /* Initialized by xcom_fsm */

/* See if this snapshot is better than what we already have */
/* purecov: begin deadcode */
static int better_snapshot(gcs_snapshot *gcs) {
  synode_no boot_key = config_max_boot_key(gcs);
  return synode_gt(boot_key, get_site_def()->boot_key) ||
         (synode_eq(boot_key, get_site_def()->boot_key) &&
          (synode_gt(gcs->log_start, log_start_max) ||
           (synode_eq(gcs->log_start, log_start_max) &&
            synode_gt(gcs->log_end, log_end_max))));
}
/* purecov: end */

/* Install snapshot */
static void handle_x_snapshot(gcs_snapshot *gcs) {
  import_config(gcs);
  if (get_nodeno(get_site_def()) == VOID_NODE_NO) {
    IFDBG(D_BUG, FN; STRLIT("Not member of site, not executing log"));
    gcs->log_end =
        gcs->log_start; /* Avoid executing log if not member of site */
  }
  handle_app_snap_cb(&gcs->app_snap, gcs->log_start, gcs->log_end);
  set_max_synode(gcs->log_end);
  set_executed_msg(incr_synode(gcs->log_start));
  log_start_max = gcs->log_start;
  log_end_max = gcs->log_end;

  set_last_received_config(get_highest_boot_key(gcs));

  IFDBG(D_BUG, FN; SYCEXP(gcs->log_start); SYCEXP(gcs->log_end);
        SYCEXP(last_config_modification_id); SYCEXP(executed_msg););
}

/* Note that we have received snapshot, and install if better than old */
/* purecov: begin deadcode */
static void update_best_snapshot(gcs_snapshot *gcs) {
  if (get_site_def() == 0 || better_snapshot(gcs)) {
    handle_x_snapshot(gcs);
  }
}
/* purecov: end */

/* Send need_boot_op to all nodes in current config */
/* purecov: begin deadcode */
static void send_need_boot() {
  pax_msg *p = pax_msg_new_0(null_synode);
  ref_msg(p);
  p->synode = get_site_def()->start;
  p->op = need_boot_op;
  send_to_all_except_self(get_site_def(), p, "need_boot_op");
  unref_msg(&p);
}
/* purecov: end */

/* Set log_end of snapshot based on log_end in snapshot and max synode */
void set_log_end(gcs_snapshot *gcs) {
  if (synode_gt(get_max_synode(), gcs->log_end)) {
    gcs->log_end = get_max_synode();
  }
}

struct xcom_fsm_state;
typedef struct xcom_fsm_state xcom_fsm_state;

/* Function pointer corresponding to a state. Return 1 if execution should
 * continue, 0 otherwise */
typedef int (*xcom_fsm_fp)(xcom_actions action, task_arg fsmargs,
                           xcom_fsm_state *ctxt);

/* Function pointer and name */
struct xcom_fsm_state {
  xcom_fsm_fp state_fp;
  char const *state_name;
};

#define X_FSM_STATE(s) \
  { s, #s }
#define SET_X_FSM_STATE(s) \
  do {                     \
    ctxt->state_fp = s;    \
    ctxt->state_name = #s; \
  } while (0)

/* The state functions/thunks */
static int xcom_fsm_init(xcom_actions action, task_arg fsmargs,
                         xcom_fsm_state *ctxt);
static int xcom_fsm_start_enter(xcom_actions action, task_arg fsmargs,
                                xcom_fsm_state *ctxt);
static int xcom_fsm_start(xcom_actions action, task_arg fsmargs,
                          xcom_fsm_state *ctxt);
static int xcom_fsm_snapshot_wait_enter(xcom_actions action, task_arg fsmargs,
                                        xcom_fsm_state *ctxt);
static int xcom_fsm_snapshot_wait(xcom_actions action, task_arg fsmargs,
                                  xcom_fsm_state *ctxt);
static int xcom_fsm_recover_wait_enter(xcom_actions action, task_arg fsmargs,
                                       xcom_fsm_state *ctxt);
static int xcom_fsm_recover_wait(xcom_actions action, task_arg fsmargs,
                                 xcom_fsm_state *ctxt);
static int xcom_fsm_run_enter(xcom_actions action, task_arg fsmargs,
                              xcom_fsm_state *ctxt);
static int xcom_fsm_run(xcom_actions action, task_arg fsmargs,
                        xcom_fsm_state *ctxt);

/* You are in a twisting maze of little functions ... */

/* init state */
static int xcom_fsm_init(xcom_actions action, task_arg fsmargs,
                         xcom_fsm_state *ctxt) {
  (void)action;
  (void)fsmargs;
  IFDBG(D_NONE, FN;);
  G_INFO("Init xcom thread");
  /* Initialize basic xcom data */
  xcom_thread_init();
  SET_X_FSM_STATE(xcom_fsm_start_enter);
  return 1;
}

/* start_enter state */
static int xcom_fsm_start_enter(xcom_actions action, task_arg fsmargs,
                                xcom_fsm_state *ctxt) {
  (void)action;
  (void)fsmargs;
  /* push_dbg(D_DETECT | D_FSM | D_FILEOP | D_CONS | D_BASE | D_TRANSPORT);
   */
  push_dbg(D_FSM);
  IFDBG(D_NONE, FN; STRLIT("state x_start"););
  empty_prop_input_queue();
  reset_snapshot_mask();
  set_last_received_config(null_synode);

  SET_X_FSM_STATE(xcom_fsm_start);
  return 1;
}

static int handle_fsm_net_boot(task_arg fsmargs, xcom_fsm_state *ctxt,
                               int cont) {
  app_data *a = (app_data *)get_void_arg(fsmargs);
  install_node_group(a);
  if (is_member(get_site_def())) {
    empty_prop_input_queue();
    {
      synode_no start = get_site_def()->start;
      if (start.msgno == 0) { /* May happen during initial boot */
        start.msgno = 1;
      }
      set_executed_msg(start);
    }
    pop_dbg();
    G_INFO("handle_fsm_net_boot calls xcom_fsm_run_enter");
    SET_X_FSM_STATE(xcom_fsm_run_enter);
    cont = 1;
  }
  return cont;
}

static int handle_fsm_snapshot(task_arg fsmargs, xcom_fsm_state *ctxt) {
  gcs_snapshot *gcs = (gcs_snapshot *)get_void_arg(fsmargs);
  empty_prop_input_queue();
  set_log_end(gcs);
  handle_x_snapshot(gcs);

  /* Get recovery manager going again */
  if (recovery_restart_cb) recovery_restart_cb();

  /* If we run under control of the recovery manager, we need to call
   * recovery_begin_cb to rendezvous with the recovery manager */
  if (recovery_begin_cb) recovery_begin_cb();

  /* If we run under control of the recovery manager, we need to call
   * recovery_end_cb to rendezvous with the recovery manager */
  if (recovery_end_cb) recovery_end_cb();

  /* If we are here, it means that we are recovering from another node
   */
  /* Do not bother to wait for more snapshots, just handle it and
  enter run state */
  pop_dbg();
  G_INFO("handle_fsm_snapshot calls xcom_fsm_run_enter");
  SET_X_FSM_STATE(xcom_fsm_run_enter);
  return 1;
}

/* purecov: begin deadcode */
static int handle_fsm_snapshot_wait(xcom_fsm_state *ctxt) {
  empty_prop_input_queue();
  start_x_timer(SNAPSHOT_WAIT_TIME);
  pop_dbg();
  SET_X_FSM_STATE(xcom_fsm_snapshot_wait_enter);
  return 1;
}
/* purecov: end */

static void handle_fsm_exit() {
  /* Xcom is finished when we get here */
  push_dbg(D_BUG);
  bury_site(get_group_id(get_site_def()));
  task_terminate_all(); /* Kill, kill, kill, kill, kill, kill. This is
                           the end. */

  /* init_xcom_base(); */ /* Reset shared variables */
  init_tasks();           /* Reset task variables */
  free_site_defs();
  free_forced_config_site_def();
  wait_forced_config = 0;
  garbage_collect_servers();
  IFDBG(D_NONE, FN; STRLIT("shutting down"));
  xcom_shutdown = 1;
  start_config = null_synode;
  G_INFO("Exiting xcom thread");
}

/* start state */
static int xcom_fsm_start(xcom_actions action, task_arg fsmargs,
                          xcom_fsm_state *ctxt) {
  static int need_init_cache = 0;
  int cont = 0; /* Set to 1 if we should continue execution */

  switch (action) {
    case x_fsm_init:
      xcom_shutdown = 0;
      sent_alive = 0.0;
      oom_abort = 0;
      if (need_init_cache) init_cache();
      break;

    case x_fsm_net_boot:
      cont = handle_fsm_net_boot(fsmargs, ctxt, cont);
      break;

    case x_fsm_snapshot:
      cont = handle_fsm_snapshot(fsmargs, ctxt);
      break;

    /* This is the entry point for the initial recovery after the process
     * has started when running under an external recovery manager. */
    /* If we get x_fsm_snapshot_wait, we are called from the recovery
     * manager thread */
    /* purecov: begin deadcode */
    case x_fsm_snapshot_wait:
      cont = handle_fsm_snapshot_wait(ctxt);
      break;
      /* purecov: end */

    case x_fsm_exit:
      handle_fsm_exit();
      break;

    default:
      break;
  }
  need_init_cache = 1;
  return cont;
}

/* snapshot_wait_enter state */
/* purecov: begin deadcode */
static int xcom_fsm_snapshot_wait_enter(xcom_actions action, task_arg fsmargs,
                                        xcom_fsm_state *ctxt) {
  (void)action;
  (void)fsmargs;
  push_dbg(D_DETECT | D_FSM | D_FILEOP | D_CONS | D_BASE | D_TRANSPORT);
  IFDBG(D_NONE, FN; STRLIT("state x_snapshot_wait"););
  log_start_max = null_synode;
  log_end_max = null_synode;
  SET_X_FSM_STATE(xcom_fsm_snapshot_wait);
  return 0;
}
/* purecov: end */

/* purecov: begin deadcode */
static int handle_local_snapshot(task_arg fsmargs, xcom_fsm_state *ctxt) {
  update_best_snapshot((gcs_snapshot *)get_void_arg(fsmargs));
  /* When recovering locally, fetch node number from site_def after
   * processing the snapshot */
  note_snapshot(get_site_def()->nodeno);
  send_need_boot();
  pop_dbg();
  SET_X_FSM_STATE(xcom_fsm_recover_wait_enter);
  return 1;
}
/* purecov: end */

/* purecov: begin deadcode */
static int handle_snapshot(task_arg fsmargs, xcom_fsm_state *ctxt) {
  /* Snapshot from another node */
  gcs_snapshot *gcs = (gcs_snapshot *)get_void_arg(fsmargs);
  set_log_end(gcs);
  update_best_snapshot(gcs);
  /* We now have a site, so note that we have processed the local
   * snapshot even if we have not seen one, since if we are here, no
   * local snapshot will ever arrive. This simplifies the test in
   * got_all_snapshots() */
  note_snapshot(get_site_def()->nodeno);
  send_need_boot();
  pop_dbg();
  SET_X_FSM_STATE(xcom_fsm_recover_wait_enter);
  return 1;
}
/* purecov: end */

/* snapshot_wait state */
/* purecov: begin deadcode */
static int xcom_fsm_snapshot_wait(xcom_actions action, task_arg fsmargs,
                                  xcom_fsm_state *ctxt) {
  switch (action) {
      /* If we get x_fsm_local_snapshot, we are called from the recovery
       * manager thread */
    case x_fsm_local_snapshot:
      return handle_local_snapshot(fsmargs, ctxt);

    case x_fsm_snapshot:
      return handle_snapshot(fsmargs, ctxt);

    case x_fsm_timeout:
      /* Will time out if no snapshot available */
      /* If we run under control of the recovery manager, we need to call
       * recovery_end_cb to rendezvous with the recovery manager */
      if (recovery_end_cb) recovery_end_cb();
      pop_dbg();
      SET_X_FSM_STATE(xcom_fsm_start_enter);
      return 1;

    default:
      break;
  }
  return 0;
}
/* purecov: end */

/* recover_wait_enter state */
/* purecov: begin deadcode */
static int xcom_fsm_recover_wait_enter(xcom_actions action, task_arg fsmargs,
                                       xcom_fsm_state *ctxt) {
  (void)action;
  (void)fsmargs;
  push_dbg(D_DETECT | D_FSM | D_FILEOP | D_CONS | D_BASE | D_TRANSPORT);
  IFDBG(D_NONE, FN; STRLIT("state x_recover_wait"););
  if (got_all_snapshots()) {
    /* Need to send message to trigger transition in context of xcom
     * thread */
    send_x_fsm_complete();
  }
  SET_X_FSM_STATE(xcom_fsm_recover_wait);
  return 0;
}
/* purecov: end */

/* recover_wait state */
/* purecov: begin deadcode */
static int xcom_fsm_recover_wait(xcom_actions action, task_arg fsmargs,
                                 xcom_fsm_state *ctxt) {
  if (action == x_fsm_snapshot) {
    gcs_snapshot *gcs = (gcs_snapshot *)get_void_arg(fsmargs);
    set_log_end(gcs);
    update_best_snapshot(gcs);
  } else if (action == x_fsm_timeout || action == x_fsm_complete) {
    /* Wait terminated by timeout or because all nodes have sent a
     * snapshot */
    /* If we run under control of the recovery manager, we need to call
     * recovery_end_cb to rendezvous with the recovery manager */
    if (recovery_end_cb) recovery_end_cb();
    pop_dbg();
    G_INFO("xcom_fsm_recover_wait calls xcom_fsm_run_enter");
    SET_X_FSM_STATE(xcom_fsm_run_enter);
    return 1;
  }
  if (got_all_snapshots()) {
    /* Need to send message to trigger transition in context of xcom
     * thread */
    send_x_fsm_complete();
  }
  return 0;
}
/* purecov: end */

/* run_enter state */
static int xcom_fsm_run_enter(xcom_actions action, task_arg fsmargs,
                              xcom_fsm_state *ctxt) {
  (void)action;
  (void)fsmargs;
  start_config = get_site_def()->boot_key;

  /* Final sanity check of executed_msg */
  if (find_site_def(executed_msg) == 0) {
    /* No site_def matches executed_msg, set it to site->start */
    set_executed_msg(get_site_def()->start);
  }

  IFDBG(D_NONE, FN; STRLIT("state x_run"););
  IFDBG(D_BUG, FN; SYCEXP(executed_msg););
  IFDBG(D_BUG, FN; SYCEXP(start_config););
  stop_x_timer();
  if (xcom_run_cb) xcom_run_cb(0);
  client_boot_done = 1;
  G_INFO("set client_boot_done true");
  netboot_ok = 1;
  set_proposer_startpoint();
  create_proposers();
  set_task(&executor, task_new(executor_task, null_arg, "executor_task",
                               XCOM_THREAD_DEBUG));
  set_task(&sweeper,
           task_new(sweeper_task, null_arg, "sweeper_task", XCOM_THREAD_DEBUG));
  set_task(&detector, task_new(detector_task, null_arg, "detector_task",
                               XCOM_THREAD_DEBUG));
  set_task(&alive_t,
           task_new(alive_task, null_arg, "alive_task", XCOM_THREAD_DEBUG));
  set_task(&cache_task, task_new(cache_manager_task, null_arg,
                                 "cache_manager_task", XCOM_THREAD_DEBUG));

  push_dbg(D_FSM /* | D_EXEC | D_BASE | D_TRANSPORT */);
  SET_X_FSM_STATE(xcom_fsm_run);
  return 1;
}

static int handle_fsm_terminate(task_arg fsmargs, xcom_fsm_state *ctxt) {
  dump_debug_exec_state();
  client_boot_done = 0;
  netboot_ok = 0;
  oom_abort = 0;
  terminate_proposers();
  init_proposers();
  task_terminate(executor);
  set_task(&executor, NULL);
  task_terminate(sweeper);
  set_task(&sweeper, NULL);
  task_terminate(detector);
  set_task(&detector, NULL);
  task_terminate(alive_t);
  set_task(&alive_t, NULL);
  task_terminate(cache_task);
  set_task(&cache_task, NULL);

  init_xcom_base(); /* Reset shared variables */
  free_site_defs();
  free_forced_config_site_def();
  wait_forced_config = 0;
  garbage_collect_servers();
  if (xcom_terminate_cb) xcom_terminate_cb(get_int_arg(fsmargs));
  pop_dbg();
  SET_X_FSM_STATE(xcom_fsm_start_enter);
  return 1;
}

static void handle_fsm_force_config(task_arg fsmargs) {
  app_data *a = (app_data *)get_void_arg(fsmargs);
  site_def *s = create_site_def_with_start(a, executed_msg);

  s->boot_key = executed_msg;
  invalidate_servers(get_site_def(), s);
  start_force_config(s, 1);
  wait_forced_config = 1; /* Note that forced config has not yet arrived */
}

/* run state */
static int xcom_fsm_run(xcom_actions action, task_arg fsmargs,
                        xcom_fsm_state *ctxt) {
  switch (action) {
    case x_fsm_terminate:
      return handle_fsm_terminate(fsmargs, ctxt);

    /* purecov: begin deadcode */
    case x_fsm_need_snapshot:
      IFDBG(D_NONE, STRLIT("got snapshot request in x_run state"));
      break;
      /* purecov: end */

    case x_fsm_force_config:
      handle_fsm_force_config(fsmargs);
      break;

    default:
      break;
  }
  return 0;
}

/* Trampoline which loops calling thunks pointed to by ctxt.state_fp until 0 is
 * returned. Return pointer to ctxt. */
xcom_fsm_state *xcom_fsm_impl(xcom_actions action, task_arg fsmargs) {
  static xcom_fsm_state ctxt = X_FSM_STATE(xcom_fsm_init);

  G_INFO("%f pid %d xcom_id %x state %s action %s", seconds(), xpid(),
         get_my_xcom_id(), ctxt.state_name, xcom_actions_name[action]);
  ADD_DBG(D_FSM, add_event(EVENT_DUMP_PAD, string_arg("state"));
          add_event(EVENT_DUMP_PAD, string_arg(ctxt.state_name));
          add_event(EVENT_DUMP_PAD, string_arg("action"));
          add_event(EVENT_DUMP_PAD, string_arg(xcom_actions_name[action]));
          add_event(EVENT_DUMP_PAD, string_arg("executed_msg"));
          add_synode_event(executed_msg););
#ifdef TASK_EVENT_TRACE
  dump_task_events();
#endif
  /* Crank the state machine until it stops */
  IFDBG(D_BUG, FN; STREXP(ctxt.state_name); STREXP(xcom_actions_name[action]));
  while (ctxt.state_fp(action, fsmargs, &ctxt)) {
    IFDBG(D_BUG, FN; STREXP(ctxt.state_name);
          STREXP(xcom_actions_name[action]));
  }
  return &ctxt;
}

/* Call FSM trampoline and return state name of resulting state */
char const *xcom_fsm(xcom_actions action, task_arg fsmargs) {
  xcom_fsm_state *s = xcom_fsm_impl(action, fsmargs);
  return s->state_name;
}

/* See if we can send a snapshot to another node */
/* purecov: begin deadcode */
static int can_send_snapshot() {
  xcom_fsm_state *state = xcom_fsm_impl(x_fsm_need_snapshot, null_arg);
  return state->state_fp == xcom_fsm_run;
}
/* purecov: end */

void set_app_snap_handler(app_snap_handler x) { handle_app_snap_cb = x; }

/* purecov: begin deadcode */
void set_app_snap_getter(app_snap_getter x) { get_app_snap_cb = x; }
/* purecov: end */

static result checked_create_socket(int domain, int type, int protocol) {
  result retval = {0, 0};
  int nr_attempts = 1005;

  do {
    SET_OS_ERR(0);
    retval.val = (int)socket(domain, type, protocol);
    retval.funerr = to_errno(GET_OS_ERR);
    if (nr_attempts % 10 == 0) xcom_sleep(1);
  } while (--nr_attempts && retval.val == -1 &&
           (from_errno(retval.funerr) == SOCK_EAGAIN));

  if (retval.val == -1) {
    task_dump_err(retval.funerr);
#if defined(_WIN32)
    G_MESSAGE("Socket creation failed with error %d.", retval.funerr);
#else
    G_MESSAGE("Socket creation failed with error %d - %s.", retval.funerr,
              strerror(retval.funerr));
#endif
  }
  return retval;
}

/* Read max n bytes from socket fd into buffer buf */
static result socket_read(connection_descriptor *rfd, void *buf, int n) {
  result ret = {0, 0};

  assert(n >= 0);

  do {
    ret = con_read(rfd, buf, n);
    task_dump_err(ret.funerr);
  } while (ret.val < 0 && can_retry_read(ret.funerr));
  return ret;
}

/* Read exactly n bytes from socket fd into buffer buf */
static int64_t socket_read_bytes(connection_descriptor *rfd, char *p,
                                 uint32_t n) {
  uint32_t left = n;
  char *bytes = p;

  result nread = {0, 0};

  while (left > 0) {
    /*
      socket_read just reads no more than INT_MAX bytes. We should not pass
      a length more than INT_MAX to it.
    */
    int r = (int)MIN(left, INT_MAX);

    nread = socket_read(rfd, bytes, r);
    if (nread.val == 0) {
      return 0;
    } else if (nread.val < 0) {
      return -1;
    } else {
      bytes += nread.val;
      left -= (uint32_t)nread.val;
    }
  }
  assert(left == 0);
  return n;
}

/* Write n bytes from buffer buf to socket fd */
static int64_t socket_write(connection_descriptor *wfd, void *_buf,
                            uint32_t n) {
  char *buf = (char *)_buf;
  result ret = {0, 0};

  uint32_t total; /* Keeps track of number of bytes written so far */

  total = 0;
  while (total < n) {
    int w = (int)MIN(n - total, INT_MAX);

    while ((ret = con_write(wfd, buf + total, w)).val < 0 &&
           can_retry_write(ret.funerr)) {
      task_dump_err(ret.funerr);
      IFDBG(D_NONE, FN; STRLIT("retry "); NEXP(total, d); NEXP(n, d));
    }
    if (ret.val <= 0) { /* Something went wrong */
      task_dump_err(ret.funerr);
      return -1;
    } else {
      total += (uint32_t)ret.val; /* Add number of bytes written to total */
    }
  }
  IFDBG(D_TRANSPORT, FN; NEXP(total, u); NEXP(n, u));
  assert(total == n);
  return (total);
}

static inline result xcom_close_socket(int *sock) {
  result res = {0, 0};
  if (*sock != -1) {
    IFDBG(D_FILEOP, FN; STRLIT("closing socket "); NDBG(*sock, d));
    do {
      SET_OS_ERR(0);
      res.val = CLOSESOCKET(*sock);
      res.funerr = to_errno(GET_OS_ERR);
    } while (res.val == -1 && from_errno(res.funerr) == SOCK_EINTR);
    *sock = -1;
  }
  return res;
}

static inline result xcom_shut_close_socket(int *sock) {
  result res = {0, 0};
  if (*sock >= 0) {
    shutdown_socket(sock);
    res = xcom_close_socket(sock);
  }
  return res;
}

#define CONNECT_FAIL \
  ret_fd = -1;       \
  goto end

/*

*/

/**
  @brief Retreives a node IPv4 address, if it exists.

  If a node is v4 reachable, means one of two:
  - The raw address is V4
  - a name was resolved to a V4/V6 address

  If the later is the case, we are going to prefer the first v4
  address in the list, since it is the common language between
  old and new version. If you want exclusive V6, please configure your
  DNS server to serve V6 names

  @param retrieved a previously retrieved struct addrinfo
  @return struct addrinfo* An addrinfo of the first IPv4 address. Else it will
                           return the entry parameter.
 */
struct addrinfo *does_node_have_v4_address(struct addrinfo *retrieved) {
  struct addrinfo *cycle = NULL;

  int v4_reachable = is_node_v4_reachable_with_info(retrieved);

  if (v4_reachable) {
    cycle = retrieved;
    while (cycle) {
      if (cycle->ai_family == AF_INET) {
        return cycle;
      }
      cycle = cycle->ai_next;
    }
  }

  /* If something goes really wrong... we fallback to avoid crashes */
  return retrieved;
}

static int timed_connect_msec(int fd, struct sockaddr *sock_addr,
                              socklen_t sock_size, int timeout) {
  int ret_fd = fd;
  int syserr;
  int sysret;
  struct pollfd fds;

  fds.fd = fd;
  fds.events = POLLOUT;
  fds.revents = 0;

  /* Set non-blocking */
  if (unblock_fd(fd) < 0) return -1;

  /* Trying to connect with timeout */
  SET_OS_ERR(0);
  sysret = connect(fd, sock_addr, sock_size);

  if (is_socket_error(sysret)) {
    syserr = GET_OS_ERR;
    /* If the error is SOCK_EWOULDBLOCK or SOCK_EINPROGRESS or SOCK_EALREADY,
     * wait. */
    switch (syserr) {
      case SOCK_EWOULDBLOCK:
      case SOCK_EINPROGRESS:
      case SOCK_EALREADY:
        break;
      default:
        G_DEBUG(
            "connect - Error connecting "
            "(socket=%d, error=%d).",
            fd, GET_OS_ERR);
        CONNECT_FAIL;
    }

    SET_OS_ERR(0);
    IFDBG(D_TRANSPORT, FN; STRLIT("poll - Starting. "); NEXP(timeout, d);
          NEXP(sysret, d));
    while ((sysret = poll(&fds, 1, timeout)) < 0) {
      syserr = GET_OS_ERR;
      if (syserr != SOCK_EINTR && syserr != SOCK_EINPROGRESS) break;
      SET_OS_ERR(0);
    }
    IFDBG(D_TRANSPORT, FN; STRLIT("poll - Finished. "); NEXP(timeout, d);
          NEXP(sysret, d));

    if (sysret == 0) {
      G_DEBUG(
          "Timed out while waiting for connection to be established! "
          "Cancelling connection attempt. (socket= %d, error=%d)",
          fd, sysret);
      /* G_WARNING("poll - Timeout! Cancelling connection..."); */
      CONNECT_FAIL;
    }

    if (is_socket_error(sysret)) {
      G_DEBUG(
          "poll - Error while connecting! "
          "(socket= %d, error=%d)",
          fd, GET_OS_ERR);
      CONNECT_FAIL;
    }

    {
      int socket_errno = 0;
      socklen_t socket_errno_len = sizeof(socket_errno);

      if ((fds.revents & POLLOUT) == 0) {
        IFDBG(D_NONE, FN; STRLIT("POLLOUT not set - Socket failure!"););
        ret_fd = -1;
      }

      if (fds.revents & (POLLERR | POLLHUP | POLLNVAL)) {
        IFDBG(D_NONE, FN;
              STRLIT("POLLERR | POLLHUP | POLLNVAL set - Socket failure!"););
        ret_fd = -1;
      }
      if (getsockopt(fd, SOL_SOCKET, SO_ERROR, (xcom_buf *)&socket_errno,
                     &socket_errno_len) != 0) {
        G_DEBUG("getsockopt socket %d failed.", fd);
        ret_fd = -1;
      } else {
        if (socket_errno != 0) {
          G_DEBUG("Connection to socket %d failed with error %d.", fd,
                  socket_errno);
          ret_fd = -1;
        }
      }
    }
  }

end:
  /* Set blocking */
  SET_OS_ERR(0);
  if (block_fd(fd) < 0) {
    G_DEBUG(
        "Unable to set socket back to blocking state. "
        "(socket=%d, error=%d).",
        fd, GET_OS_ERR);
    return -1;
  }
  return ret_fd;
}

static int timed_connect(int fd, struct sockaddr *sock_addr,
                         socklen_t sock_size) {
  return timed_connect_msec(fd, sock_addr, sock_size, 3000);
}

/* purecov: begin deadcode */
int timed_connect_sec(int fd, struct sockaddr *sock_addr, socklen_t sock_size,
                      int timeout) {
  return timed_connect_msec(fd, sock_addr, sock_size, timeout * 1000);
}
/* purecov: end */

/* Connect to server on given port */
#ifndef XCOM_WITHOUT_OPENSSL
static connection_descriptor *connect_xcom(char const *server, xcom_port port,
                                           int use_ssl) {
#else
static connection_descriptor *connect_xcom(char const *server, xcom_port port) {
#endif
  result fd = {0, 0};
  result ret = {0, 0};
  connection_descriptor *cd = NULL;
  char buf[SYS_STRERROR_SIZE];

  IFDBG(D_NONE, FN; STREXP(server); NEXP(port, d));
  G_INFO("connecting to %s %d", server, port);

  {
    struct addrinfo *addr = NULL, *from_ns = NULL;

    char buffer[20];
    sprintf(buffer, "%d", port);

    checked_getaddrinfo(server, buffer, 0, &from_ns);

    if (from_ns == NULL) {
      /* purecov: begin inspected */
      G_ERROR("Error retrieving server information.");
      goto end;
      /* purecov: end */
    }

    addr = does_node_have_v4_address(from_ns);

    /* Create socket after knowing the family that we are dealing with
       getaddrinfo returns a list of possible addresses. We will alays default
       to the first one in the list, which is V4 if applicable.
     */
    if ((fd = checked_create_socket(addr->ai_family, SOCK_STREAM, IPPROTO_TCP))
            .val < 0) {
      /* purecov: begin inspected */
      G_ERROR(
          "Error creating socket in local GR->GCS connection to address %s.",
          server);
      goto end;
      /* purecov: end */
    }

    /* Connect socket to address */

    SET_OS_ERR(0);

    if (timed_connect(fd.val, addr->ai_addr, (socklen_t)addr->ai_addrlen) ==
        -1) {
      fd.funerr = to_errno(GET_OS_ERR);
      G_DEBUG(
          "Connecting socket to address %s in port %d failed with error %d - "
          "%s.",
          server, port, fd.funerr, strerr_msg(buf, sizeof(buf), fd.funerr));
      xcom_close_socket(&fd.val);
      goto end;
    }
    {
      int peer = 0;
      int same_ip;
      /* Sanity check before return */
      SET_OS_ERR(0);
      {
        socklen_t ai_addrlen = (socklen_t)addr->ai_addrlen;
        ret.val = peer = xcom_getpeername(fd.val, addr->ai_addr, &ai_addrlen);
      }
      ret.funerr = to_errno(GET_OS_ERR);
      if (peer >= 0) {
        if (!check_tcp_connection_valid(fd.val, &same_ip)) {
          xcom_shut_close_socket(&fd.val);
          goto end;
        }
        ret = set_nodelay(fd.val);
        if (ret.val < 0) {
          /* purecov: begin inspected */
          task_dump_err(ret.funerr);
          xcom_shut_close_socket(&fd.val);
#if defined(_WIN32)
          G_DEBUG(
              "Setting node delay failed  while connecting to %s with error "
              "%d.",
              server, ret.funerr);
#else
          G_DEBUG(
              "Setting node delay failed  while connecting to %s with error %d "
              "- "
              "%s.",
              server, ret.funerr, strerror(ret.funerr));
#endif
          goto end;
          /* purecov: end */
        }
        G_DEBUG("client connected to %s %d fd %d", server, port, fd.val);
      } else {
        /* Something is wrong */
        /* purecov: begin inspected */
        socklen_t errlen = sizeof(ret.funerr);
        IFDBG(D_NONE, FN; STRLIT("xcom_getpeername failed"););
        if (ret.funerr) {
          IFDBG(D_NONE, FN; NEXP(from_errno(ret.funerr), d);
                STRLIT(strerror(from_errno(ret.funerr))));
        }
        getsockopt(fd.val, SOL_SOCKET, SO_ERROR, (xcom_buf *)&ret.funerr,
                   &errlen);
        if (ret.funerr == 0) {
          ret.funerr = to_errno(SOCK_ECONNREFUSED);
        }
        xcom_shut_close_socket(&fd.val);
#if defined(_WIN32)
        G_DEBUG(
            "Getting the peer name failed while connecting to server %s with "
            "error %d.",
            server, ret.funerr);
#else
        G_DEBUG(
            "Getting the peer name failed while connecting to server %s with "
            "error %d -%s.",
            server, ret.funerr, strerror(ret.funerr));
#endif
        goto end;
        /* purecov: end */
      }

#ifndef XCOM_WITHOUT_OPENSSL
      if (use_ssl && xcom_use_ssl()) {
        SSL *ssl = SSL_new(client_ctx);
        G_INFO("Trying to connect using SSL.")
        SSL_set_fd(ssl, fd.val);

        ERR_clear_error();
        ret.val = SSL_connect(ssl);
        ret.funerr = to_ssl_err(SSL_get_error(ssl, ret.val));

        if (ret.val != SSL_SUCCESS) {
          /* purecov: begin inspected */
          G_MESSAGE("Error connecting using SSL %d %d.", ret.funerr,
                    SSL_get_error(ssl, ret.val));
          task_dump_err(ret.funerr);
          SSL_shutdown(ssl);
          SSL_free(ssl);
          xcom_shut_close_socket(&fd.val);

          goto end;
          /* purecov: end */
        }
        IFDBG(D_NONE, FN; STRLIT("ssl connected to "); STRLIT(server);
              NDBG(port, d); NDBG(fd.val, d); PTREXP(ssl));

        if (ssl_verify_server_cert(ssl, server)) {
          /* purecov: begin inspected */
          G_MESSAGE("Error validating certificate and peer.");
          task_dump_err(ret.funerr);
          SSL_shutdown(ssl);
          SSL_free(ssl);
          xcom_shut_close_socket(&fd.val);

          goto end;
          /* purecov: end */
        }

        cd = new_connection(fd.val, ssl);
        set_connected(cd, CON_FD);
        G_INFO("Success connecting using SSL for fd:%d", fd.val)

        goto end;
      } else {
        cd = new_connection(fd.val, 0);
        set_connected(cd, CON_FD);
        G_INFO("Success connecting for fd:%d", fd.val)

        goto end;
      }
#else
      {
        cd = new_connection(fd.val);
        set_connected(cd, CON_FD);
        G_INFO("Success connecting for fd:%d", fd.val)

        goto end;
      }
#endif
    }

  end:
    if (from_ns) freeaddrinfo(from_ns);
  }
  return cd;
}

connection_descriptor *xcom_open_client_connection(char const *server,
                                                   xcom_port port) {
#ifndef XCOM_WITHOUT_OPENSSL
  return connect_xcom(server, port, TRUE);
#else
  return connect_xcom(server, port);
#endif
}

/* Send a protocol negotiation message on connection con */
static int xcom_send_proto(connection_descriptor *con, xcom_proto x_proto,
                           x_msg_type x_type, unsigned int tag) {
  char buf[MSG_HDR_SIZE];
  memset(buf, 0, MSG_HDR_SIZE);

  if (con->fd >= 0) {
    con->snd_tag = tag;
    write_protoversion(VERS_PTR((unsigned char *)buf), x_proto);
    put_header_1_0((unsigned char *)buf, 0, x_type, tag);
    {
      int sent;
      sent = (int)socket_write(con, buf, MSG_HDR_SIZE);
      if (con->fd < 0) {
        return -1;
      }
      return sent;
    }
  } else {
    return -1;
  }
}

static int xcom_recv_proto(connection_descriptor *rfd, xcom_proto *x_proto,
                           x_msg_type *x_type, unsigned int *tag) {
  int n;
  unsigned char header_buf[MSG_HDR_SIZE];
  uint32_t msgsize;

  /* Read length field, protocol version, and checksum */
  n = (int)socket_read_bytes(rfd, (char *)header_buf, MSG_HDR_SIZE);

  if (n != MSG_HDR_SIZE) {
    IFDBG(D_NONE, FN; NDBG(n, d));
    return -1;
  }

  *x_proto = read_protoversion(VERS_PTR(header_buf));
  get_header_1_0(header_buf, &msgsize, x_type, tag);

  return n;
}

enum { TAG_START = 313 };

/**
 * @brief Checks if a given app_data is from a given cargo_type.
 *
 * @param a the app_data
 * @param t the cargo type
 * @return int TRUE (1) if app_data a is from cargo_type t
 */

static inline int is_cargo_type(app_data_ptr a, cargo_type t) {
  return a ? (a->body.c_t == t) : 0;
}

/**
 * @brief Retrieves the address that was used in the add_node request
 *
 * @param a app data containing the node to add
 * @param member address we used to present ourselves to other nodes
 * @return char* a pointer to the address being added.
 */
static char *get_add_node_address(app_data_ptr a, unsigned int *member) {
  char *retval = NULL;
  if (!is_cargo_type(a, add_node_type)) return NULL;

  if ((*member) < a->body.app_u_u.nodes.node_list_len) {
    retval = a->body.app_u_u.nodes.node_list_val[(*member)].address;
    (*member)++;
  }

  return retval;
}

int is_node_v4_reachable_with_info(struct addrinfo *retrieved_addr_info) {
  int v4_reachable = 0;

  /* Verify if we are reachable either by V4 and by V6 with the provided
     address. */
  struct addrinfo *my_own_information_loop = NULL;

  my_own_information_loop = retrieved_addr_info;
  while (!v4_reachable && my_own_information_loop) {
    if (my_own_information_loop->ai_family == AF_INET) {
      v4_reachable = 1;
    }
    my_own_information_loop = my_own_information_loop->ai_next;
  }

  return v4_reachable;
}

int is_node_v4_reachable(char *node_address) {
  int v4_reachable = 0;

  /* Verify if we are reachable either by V4 and by V6 with the provided
     address. */
  struct addrinfo *my_own_information = NULL;

  checked_getaddrinfo(node_address, NULL, NULL, &my_own_information);
  if (my_own_information == NULL) {
    return v4_reachable;
  }

  v4_reachable = is_node_v4_reachable_with_info(my_own_information);

  if (my_own_information) freeaddrinfo(my_own_information);

  return v4_reachable;
}

int are_we_allowed_to_upgrade_to_v6(app_data_ptr a) {
  /* This should the address we used to present ourselves to other nodes. */
  unsigned int list_member = 0;
  char *added_node = NULL;

  int is_v4_reachable = 0;
  while ((added_node = get_add_node_address(a, &list_member)) != NULL) {
    xcom_port my_own_port;
    char my_own_address[IP_MAX_SIZE];
    int ip_and_port_error =
        get_ip_and_port(added_node, my_own_address, &my_own_port);

    if (ip_and_port_error) {
      G_DEBUG("Error retrieving IP and Port information");
      return 0;
    }

    /* Verify if we are reachable either by V4 and by V6 with the provided
       address.
       This means that the other side won't be able to contact us since we
       do not provide a public V4 address */
    if (!(is_v4_reachable = is_node_v4_reachable(my_own_address))) {
      G_ERROR(
          "Unable to add node to a group of older nodes. Please "
          "reconfigure "
          "you local address to an IPv4 address or configure your DNS to "
          "provide "
          "an IPv4 address");
      return 0;
    }
  }

  return is_v4_reachable;
}

int64_t xcom_send_client_app_data(connection_descriptor *fd, app_data_ptr a,
                                  int force) {
  pax_msg *msg = pax_msg_new(null_synode, 0);
  uint32_t buflen = 0;
  char *buf = 0;
  int64_t retval = 0;
  int serialized = 0;

  if (!proto_done(fd)) {
    xcom_proto x_proto;
    x_msg_type x_type;
    unsigned int tag;
    retval = xcom_send_proto(fd, my_xcom_version, x_version_req, TAG_START);
    G_DEBUG("client sent negotiation request for protocol %d", my_xcom_version);
    if (retval < 0) goto end;
    retval = xcom_recv_proto(fd, &x_proto, &x_type, &tag);
    if (retval < 0) goto end;
    if (tag != TAG_START) {
      retval = -1;
      goto end;
    }
    if (x_type != x_version_reply) {
      retval = -1;
      goto end;
    }

    if (x_proto == x_unknown_proto) {
      G_DEBUG("no common protocol, returning error");
      retval = -1;
      goto end;
    }

    /* This code will check if, in case of an upgrade if:
       - We are a node able to speak IPv6.
       - If we are connecting to a group that does not speak IPv6.
       - If our address is IPv4-compatible in order for the old group to be able
       to contact us back. */
    if (is_cargo_type(a, add_node_type) && x_proto < minimum_ipv6_version() &&
        !are_we_allowed_to_upgrade_to_v6(a)) {
      retval = -1;
      goto end;
    }

    G_DEBUG("client connection will use protocol version %d", x_proto);
    IFDBG(D_NONE, STRLIT("client connection will use protocol version ");
          NDBG(x_proto, u); STRLIT(xcom_proto_to_str(x_proto)));
    fd->x_proto = x_proto;
    set_connected(fd, CON_PROTO);
    G_INFO("xcom_send_client_app_data sets CON_PROTO for fd:%d", fd->fd)
  }
  msg->a = a;
  msg->to = VOID_NODE_NO;
  msg->op = client_msg;
  msg->force_delivery = force;

  serialized = serialize_msg(msg, fd->x_proto, &buflen, &buf);
  if (serialized) {
    retval = socket_write(fd, buf, buflen);
    if (buflen != retval) {
      IFDBG(D_NONE, FN; STRLIT("write failed "); NDBG(fd->fd, d);
            NDBG(buflen, d); NDBG64(retval));
    }
  } else {
    /* Failed to serialize, set retval accordingly. */
    retval = -1;
  }
  X_FREE(buf);
end:
  msg->a = 0; /* Do not deallocate a */
  XCOM_XDR_FREE(xdr_pax_msg, msg);
  return retval;
}

/* purecov: begin tested */
/*
 * Tested by TEST_F(XComMultinodeSmokeTest,
 * 3_nodes_member_crashes_with_dieop_and_joins_again_immediately) GCS smoke test
 */
int64_t xcom_client_send_die(connection_descriptor *fd) {
  uint32_t buflen = 0;
  char *buf = 0;
  int64_t retval = 0;
  app_data a;
  pax_msg *msg = pax_msg_new(null_synode, 0);

  if (!proto_done(fd)) {
    xcom_proto x_proto;
    x_msg_type x_type;
    unsigned int tag;
    retval = xcom_send_proto(fd, my_xcom_version, x_version_req, TAG_START);
    G_DEBUG("client sent negotiation request for protocol %d", my_xcom_version);
    if (retval < 0) goto end;
    retval = xcom_recv_proto(fd, &x_proto, &x_type, &tag);
    if (retval < 0) goto end;
    if (tag != TAG_START) {
      retval = -1;
      goto end;
    }
    if (x_type != x_version_reply) {
      retval = -1;
      goto end;
    }

    if (x_proto == x_unknown_proto) {
      G_DEBUG("no common protocol, returning error");
      retval = -1;
      goto end;
    }
    G_DEBUG("client connection will use protocol version %d", x_proto);
    IFDBG(D_NONE, STRLIT("client connection will use protocol version ");
          NDBG(x_proto, u); STRLIT(xcom_proto_to_str(x_proto)));
    fd->x_proto = x_proto;
    set_connected(fd, CON_PROTO);
    G_INFO("xcom_client_send_die sets CON_PROTO for fd:%d", fd->fd)
  }
  init_app_data(&a);
  a.body.c_t = app_type;
  msg->a = &a;
  msg->op = die_op;
  /*
    Set the msgno to a value that ensures the die_op will be processed by
    XCom when it is received (it needs to be higher than the msgno of the
    executed_msg, otherwise XCom will simply ignore it).
   */
  msg->synode.msgno = UINT64_MAX;

  serialize_msg(msg, fd->x_proto, &buflen, &buf);
  if (buflen) {
    retval = socket_write(fd, buf, buflen);
    if (buflen != retval) {
      IFDBG(D_NONE, FN; STRLIT("write failed "); NDBG(fd->fd, d);
            NDBG(buflen, d); NDBG64(retval));
    }
    X_FREE(buf);
  }
  xdr_free((xdrproc_t)xdr_app_data, (char *)&a);
end:
  msg->a = 0;
  XCOM_XDR_FREE(xdr_pax_msg, msg);
  return retval > 0 && retval == buflen ? 1 : 0;
}
/* purecov: end */

/* purecov: begin deadcode */
int64_t xcom_client_send_data(uint32_t size, char *data,
                              connection_descriptor *fd) {
  app_data a;
  int64_t retval = 0;
  init_app_data(&a);
  a.body.c_t = app_type;
  a.body.app_u_u.data.data_len = size;
  a.body.app_u_u.data.data_val = data;
  retval = xcom_send_client_app_data(fd, &a, 0);
  xdr_free((xdrproc_t)xdr_app_data, (char *)&a);
  return retval;
}
/* purecov: end */

#ifndef _WIN32
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif

/* Output warning in log periodically if we receive messages
with a protocol version that does not match our own */
/* purecov: begin inspected */
void warn_protoversion_mismatch(connection_descriptor *rfd) {
  struct sockaddr_storage sock_addr;
  socklen_t sock_size = sizeof(sock_addr);

  if (task_now() - protoversion_warning_time > PROTOVERSION_WARNING_TIMEOUT) {
    if (0 ==
        xcom_getpeername(rfd->fd, (struct sockaddr *)&sock_addr, &sock_size)) {
      char buf[INET6_ADDRSTRLEN + 1];
      struct sockaddr_in *s4 = (struct sockaddr_in *)&sock_addr;
      struct sockaddr_in6 *s6 = (struct sockaddr_in6 *)&sock_addr;
      char const *ok;

      memset((void *)buf, 0, sizeof(buf));
      if (sock_addr.ss_family == AF_INET) {
        ok = inet_ntop(sock_addr.ss_family, (void *)&s4->sin_addr, buf,
                       sizeof(buf));
      } else {
        ok = inet_ntop(sock_addr.ss_family, (void *)&s6->sin6_addr, buf,
                       sizeof(buf));
      }
      if (ok) {
        G_WARNING(
            "Detected incorrect xcom protocol version in connection from %s "
            "indicates "
            "missing cleanup of, or incorrect, xcom group definition on remote "
            "host. Please upgrade the process running on %s to a compatible "
            "version or stop it.",
            buf, buf);
        protoversion_warning_time = task_now();
      }
    }
  }
}
/* purecov: end */

static pax_msg *socket_read_msg(connection_descriptor *rfd, pax_msg *p)
/* Should buffer reads as well */
{
  int64_t n;
  char *bytes;
  unsigned char header_buf[MSG_HDR_SIZE];
  xcom_proto x_version;
  uint32_t msgsize;
  x_msg_type x_type;
  unsigned int tag;
  int deserialize_ok = 0;

  bytes = NULL;

  /* Read version, length, type, and tag */
  n = socket_read_bytes(rfd, (char *)header_buf, MSG_HDR_SIZE);

  if (n <= 0) {
    IFDBG(D_NONE, FN; NDBG64(n));
    return 0;
  }
  assert(n == MSG_HDR_SIZE);
  x_version = (xcom_proto)get_32(VERS_PTR(header_buf));
/* Check the protocol version before doing anything else */
#ifdef XCOM_PARANOID
  assert(check_protoversion(x_version, rfd->x_proto));
#endif
  if (!check_protoversion(x_version, rfd->x_proto)) {
    /* purecov: begin inspected */
    warn_protoversion_mismatch(rfd);
    return 0;
    /* purecov: end */
  }

  /* OK, we can grok this version */

  get_header_1_0(header_buf, &msgsize, &x_type, &tag);

  /* Allocate buffer space for message */
  bytes = (char *)calloc(1, msgsize);

  /* Read message */
  n = socket_read_bytes(rfd, bytes, msgsize);

  if (n > 0) {
    /* Deserialize message */
    deserialize_ok = deserialize_msg(p, rfd->x_proto, bytes, msgsize);
    IFDBG(D_NONE, FN; STRLIT(" deserialized message"));
  }
  /* Deallocate buffer */
  X_FREE(bytes);
  if (n <= 0 || deserialize_ok == 0) {
    IFDBG(D_NONE, FN; NDBG64(n));
    return 0;
  }
  return (p);
}

int xcom_close_client_connection(connection_descriptor *connection) {
  int retval = 0;

#ifndef XCOM_WITHOUT_OPENSSL
  if (connection->ssl_fd) {
    SSL_shutdown(connection->ssl_fd);
    ssl_free_con(connection);
  }
#endif
  retval = xcom_shut_close_socket(&connection->fd).val;
  free(connection);
  return retval;
}

/* purecov: begin deadcode */
int xcom_client_boot(connection_descriptor *fd, node_list *nl,
                     uint32_t group_id) {
  app_data a;
  int retval = 0;
  retval = (int)xcom_send_client_app_data(
      fd, init_config_with_group(&a, nl, unified_boot_type, group_id), 0);
  xdr_free((xdrproc_t)xdr_app_data, (char *)&a);
  return retval;
}
/* purecov: end */

enum xcom_send_app_wait_result {
  SEND_REQUEST_FAILED = 0,
  RECEIVE_REQUEST_FAILED,
  REQUEST_BOTCHED,
  RETRIES_EXCEEDED,
  REQUEST_OK_RECEIVED,
  REQUEST_FAIL_RECEIVED
};
typedef enum xcom_send_app_wait_result xcom_send_app_wait_result;

/**
 * Send a message and wait for response.
 *
 * The caller is reponsible for freeing p after calling this function,
 * i.e. xdr_free((xdrproc_t)xdr_pax_msg, (char *)p)
 */
static xcom_send_app_wait_result xcom_send_app_wait_and_get(
    connection_descriptor *fd, app_data *a, int force, pax_msg *p) {
  int retval = 0;
  int retry_count = 1; /* Same as 'connection_attempts' */
  pax_msg *rp = 0;

  do {
    retval = (int)xcom_send_client_app_data(fd, a, force);
    memset(p, 0, sizeof(*p)); /* before return so caller can free p */
    if (retval < 0) return SEND_REQUEST_FAILED;
    rp = socket_read_msg(fd, p);
    if (rp) {
      client_reply_code cli_err = rp->cli_err;
      switch (cli_err) {
        case REQUEST_OK:
          return REQUEST_OK_RECEIVED;
        case REQUEST_FAIL:

          G_DEBUG("cli_err %d", cli_err);
          return REQUEST_FAIL_RECEIVED;
        case REQUEST_RETRY:
          G_DEBUG("cli_err %d", cli_err);
          if (retry_count > 1) xdr_free((xdrproc_t)xdr_pax_msg, (char *)p);
          break;
        default:
          G_WARNING("client protocol botched");
          return REQUEST_BOTCHED;
      }
    } else {
      G_WARNING("read failed");
      return RECEIVE_REQUEST_FAILED;
    }
  } while (--retry_count);
  /* Timeout after REQUEST_RETRY has been received 'retry_count' times */
  G_MESSAGE(
      "Request failed: maximum number of retries (1) has been exhausted.");
  return RETRIES_EXCEEDED;
}

int xcom_send_app_wait(connection_descriptor *fd, app_data *a, int force) {
  pax_msg p;
  int result = 0;
  xcom_send_app_wait_result res = xcom_send_app_wait_and_get(fd, a, force, &p);
  switch (res) {
    case SEND_REQUEST_FAILED:
    case RECEIVE_REQUEST_FAILED:
    case REQUEST_BOTCHED:
    case RETRIES_EXCEEDED:
    case REQUEST_FAIL_RECEIVED:
      result = 0;
      break;
    case REQUEST_OK_RECEIVED:
      result = 1;
      break;
  }
  xdr_free((xdrproc_t)xdr_pax_msg, (char *)&p);
  return result;
}

int xcom_send_cfg_wait(connection_descriptor *fd, node_list *nl,
                       uint32_t group_id, cargo_type ct, int force) {
  app_data a;
  int retval = 0;
  IFDBG(D_NONE, FN; COPY_AND_FREE_GOUT(dbg_list(nl)););
  retval = xcom_send_app_wait(fd, init_config_with_group(&a, nl, ct, group_id),
                              force);
  xdr_free((xdrproc_t)xdr_app_data, (char *)&a);
  return retval;
}

int xcom_client_add_node(connection_descriptor *fd, node_list *nl,
                         uint32_t group_id) {
  u_int i;
  for (i = 0; i < nl->node_list_len; i++) {
    assert(nl->node_list_val[i].proto.max_proto > x_unknown_proto);
  }
  return xcom_send_cfg_wait(fd, nl, group_id, add_node_type, 0);
}

int xcom_client_remove_node(connection_descriptor *fd, node_list *nl,
                            uint32_t group_id) {
  return xcom_send_cfg_wait(fd, nl, group_id, remove_node_type, 0);
}

/* purecov: begin deadcode */
int xcom_client_get_event_horizon(connection_descriptor *fd, uint32_t group_id,
                                  xcom_event_horizon *event_horizon) {
  pax_msg p;
  app_data a;
  int result = 0;

  xcom_send_app_wait_result res = xcom_send_app_wait_and_get(
      fd, init_get_event_horizon_msg(&a, group_id), 0, &p);

  switch (res) {
    case RECEIVE_REQUEST_FAILED:
    case REQUEST_BOTCHED:
    case RETRIES_EXCEEDED:
    case SEND_REQUEST_FAILED:
    case REQUEST_FAIL_RECEIVED:
      result = 0;
      break;
    case REQUEST_OK_RECEIVED:
      *event_horizon = p.event_horizon;
      result = 1;
      break;
  }

  xdr_free((xdrproc_t)xdr_pax_msg, (char *)&p);
  xdr_free((xdrproc_t)xdr_app_data, (char *)&a);

  return result;
}
/* purecov: end */

/* purecov: begin deadcode */
int xcom_client_set_event_horizon(connection_descriptor *fd, uint32_t group_id,
                                  xcom_event_horizon event_horizon) {
  app_data a;
  int retval = 0;
  retval = xcom_send_app_wait(
      fd, init_set_event_horizon_msg(&a, group_id, event_horizon), 0);
  xdr_free((xdrproc_t)xdr_app_data, (char *)&a);
  return retval;
}
/* purecov: end */

int xcom_client_get_synode_app_data(connection_descriptor *const fd,
                                    uint32_t group_id,
                                    synode_no_array *const synodes,
                                    synode_app_data_array *const reply) {
  bool_t const success = TRUE;
  bool_t const failure = FALSE;
  bool_t result = failure;
  pax_msg p;
  app_data a;
  u_int const nr_synodes_requested = synodes->synode_no_array_len;

  /* This call moves, as in C++ move semantics, synodes into app_data a. */
  init_get_synode_app_data_msg(&a, group_id, synodes);

  {
    xcom_send_app_wait_result res = xcom_send_app_wait_and_get(fd, &a, 0, &p);
    switch (res) {
      case RECEIVE_REQUEST_FAILED:
      case REQUEST_BOTCHED:
      case RETRIES_EXCEEDED:
      case SEND_REQUEST_FAILED:
      case REQUEST_FAIL_RECEIVED: {
        G_TRACE(
            "xcom_client_get_synode_app_data: XCom did not have the required "
            "%u "
            "synodes.",
            nr_synodes_requested);
        break;
      }
      case REQUEST_OK_RECEIVED: {
        u_int const nr_synodes_received =
            p.requested_synode_app_data.synode_app_data_array_len;
        G_TRACE(
            "xcom_client_get_synode_app_data: Got %u synode payloads, we asked "
            "for %u.",
            nr_synodes_received, nr_synodes_requested);

        /* This should always be TRUE.
         * But rather than asserting it, let's treat an unexpected number of
         * synode payloads in the reply as a failure. */
        if (nr_synodes_received == nr_synodes_requested) {
          /* Move (as in C++ move semantics) into reply */
          synode_app_data_array_move(reply, &p.requested_synode_app_data);
          result = success;
        }
        break;
      }
    }
  }

  xdr_free((xdrproc_t)xdr_pax_msg, (char *)&p);
  xdr_free((xdrproc_t)xdr_app_data, (char *)&a);

  return result;
}

#ifdef NOTDEF
/* Not completely implemented, need to be handled properly
   when received as a client message in dispatch_op.
   Should have separate opcode from normal add/remove,
   like force config_type */
int xcom_client_force_add_node(connection_descriptor *, node_list *nl,
                               uint32_t group_id) {
  return xcom_send_cfg_wait(fd, nl, group_id, add_node_type, 1);
}

int xcom_client_force_remove_node(connection_descriptor *, node_list *nl,
                                  uint32_t group_id) {
  return xcom_send_cfg_wait(fd, nl, group_id, remove_node_type, 1);
}
#endif

int xcom_client_force_config(connection_descriptor *fd, node_list *nl,
                             uint32_t group_id) {
  return xcom_send_cfg_wait(fd, nl, group_id, force_config_type, 1);
}

/* purecov: begin deadcode */
int xcom_client_enable_arbitrator(connection_descriptor *fd) {
  app_data a;
  int retval = 0;
  init_app_data(&a);
  a.body.c_t = enable_arbitrator;
  retval = xcom_send_app_wait(fd, &a, 0);
  xdr_free((xdrproc_t)xdr_app_data, (char *)&a);
  return retval;
}
/* purecov: end */

/* purecov: begin deadcode */
int xcom_client_disable_arbitrator(connection_descriptor *fd) {
  app_data a;
  int retval = 0;
  init_app_data(&a);
  a.body.c_t = disable_arbitrator;
  retval = xcom_send_app_wait(fd, &a, 0);
  xdr_free((xdrproc_t)xdr_app_data, (char *)&a);
  return retval;
}
/* purecov: end */

/* purecov: begin deadcode */
int xcom_client_terminate_and_exit(connection_descriptor *fd) {
  app_data a;
  int retval = 0;
  init_app_data(&a);
  a.body.c_t = x_terminate_and_exit;
  retval = xcom_send_app_wait(fd, &a, 0);
  xdr_free((xdrproc_t)xdr_app_data, (char *)&a);
  return retval;
}
/* purecov: end */

/* purecov: begin deadcode */
int xcom_client_set_cache_limit(connection_descriptor *fd,
                                uint64_t cache_limit) {
  app_data a;
  int retval = 0;
  init_app_data(&a);
  a.body.c_t = set_cache_limit;
  a.body.app_u_u.cache_limit = cache_limit;
  retval = xcom_send_app_wait(fd, &a, 0);
  xdr_free((xdrproc_t)xdr_app_data, (char *)&a);
  return retval;
}
/* purecov: end */

int xcom_client_convert_into_local_server(connection_descriptor *const fd) {
  app_data a;
  int retval = 0;
  retval = xcom_send_app_wait(fd, init_convert_into_local_server_msg(&a), 0);
  xdr_free((xdrproc_t)xdr_app_data, (char *)&a);
  return retval;
}
