/* Copyright (c) 2015, 2022, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2023, 2024, GreatDB Software Co., Ltd.

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
#include <stdlib.h>
#ifdef _MSC_VER
#include <stdint.h>
#endif

#include "xcom/app_data.h"
#include "xcom/xcom_profile.h"
#ifndef XCOM_STANDALONE
#include "my_compiler.h"
#endif
#include "xcom/node_list.h"
#include "xcom/node_no.h"
#include "xcom/node_set.h"
#include "xcom/pax_msg.h"
#include "xcom/server_struct.h"
#include "xcom/simset.h"
#include "xcom/site_def.h"
#include "xcom/site_struct.h"
#include "xcom/sock_probe.h"
#include "xcom/synode_no.h"
#include "xcom/task.h"
#include "xcom/task_debug.h"
#include "xcom/x_platform.h"
#include "xcom/xcom_base.h"
#include "xcom/xcom_cfg.h"
#include "xcom/xcom_common.h"
#include "xcom/xcom_detector.h"
#include "xcom/xcom_interface.h"
#include "xcom/xcom_limits.h"
#include "xcom/xcom_transport.h"
#include "xdr_gen/xcom_vp.h"

extern task_env *detector;
extern int xcom_shutdown;
extern linkage detector_wait;
extern int all_nodes_valid;

/* See if node has been suspiciously still for some time */
int may_be_dead(detector_state const ds, node_no i, double seconds, int silent,
                int unreachable) {
  if (ds[i] == 0.0) {
    return 0;
  }
  if (unreachable) {
    return 1;
  } else {
    return ds[i] < seconds - silent;
  }
}

static int detect_node_timeout(site_def const *site, node_no node) {
  int alive = 1;
  ulong timeout = DEFAULT_DETECTOR_LIVE_TIMEOUT;
  double current = task_now();

  if (site->servers[node]->unreachable >= MAX_CONNECT_FAIL_TIMES) {
    timeout = 1;
  } else if (site->servers[node]->unreachable == DIRECT_ABORT_CONN) {
    alive = 0;
  } else {
    if (the_app_xcom_cfg) {
      timeout = the_app_xcom_cfg->m_flp_timeout;
    }
    double diff = current - site->servers[node]->large_transfer_detected;
    if (diff < timeout) {
      timeout = timeout << 1;
    }
  }
  if (alive) {
    return (node == get_nodeno(site)) ||
           (site->detected[node] + timeout > current);
  } else {
    return 0;
  }
}

void init_detector(detector_state ds) {
  int i = 0;
  for (i = 0; i < NSERVERS; i++) {
    ds[i] = 0.0;
  }
}

int note_detected(site_def const *site, node_no node) {
  int retval = 1;
  /* IFDBG(D_DETECT, FN; NDBG(node,d);); */

  /* site->servers's size is NSERVERS. */
  assert(site->nodes.node_list_len <= NSERVERS);

  if (site && node < site->nodes.node_list_len) {
    retval = detect_node_timeout(site, node);
    server_detected(site->servers[node]);
  }
  return retval;
}

/**
 * @brief States if a server is still physically connected to another server.
 * This will test the connection state to that node.
 *
 * @param site site definition that contains the server collections.
 * @param node node index that we want to test for connectivity
 *
 * @return 1 if the server is connected. 0 otherwise.
 */
int is_server_connected(struct site_def const *site, node_no node) {
  int retval = 0;

  if (site) {
    if (get_nodeno(site) == node) {  // Me to myself... i'm always connected
      retval = 1;
    } else if (node < site->nodes.node_list_len) {
      retval = is_connected(site->servers[node]->con);
    }
  }

  return retval;
}

static void reset_detected(site_def const *site, u_int node) {
  IFDBG(D_DETECT, FN; PTREXP(site); NDBG(node, d););
  /* site->servers's size is NSERVERS. */
  assert(site->nodes.node_list_len <= NSERVERS);
  if (site && node < site->nodes.node_list_len) {
    site->servers[node]->detected = 0.0;
  }
}

void reset_disjunct_servers(struct site_def const *old_site,
                            struct site_def const *new_site) {
  u_int node;

  if (old_site && new_site) {
    /* Reset nodes which are not in new site (removed) */
    for (node = 0; node < old_site->nodes.node_list_len; node++) {
      if (!node_exists(&old_site->nodes.node_list_val[node], &new_site->nodes))
        reset_detected(old_site, node);
    }
  }
}

static void dbg_detected(site_def *site) {
  u_int node;

  if (site) {
    for (node = 0; node < site->nodes.node_list_len; node++) {
      IFDBG(D_DETECT, FN; NDBG(node, d); NDBG(site->detected[node], f);
            NDBG(site->servers[node]->detected, f));
    }
  }
}

void update_detected(site_def *site, double conn_rtt) {
  u_int node;

  if (site) {
    bool_t changed = FALSE;
    for (node = 0; node < site->nodes.node_list_len; node++) {
      IFDBG(D_DETECT, FN; NDBG(node, d); NDBG(site->detected[node], f);
            NDBG(site->servers[node]->detected, f));
      if (site->detected[node] != site->servers[node]->detected) changed = TRUE;
      site->detected[node] = site->servers[node]->detected;
    }
    site->detector_updated = 1;

    if (conn_rtt > 0 && site->max_conn_rtt < conn_rtt) {
      site->max_conn_rtt = conn_rtt;
    }

    if (changed) {
      dbg_detected(site);
    }
  }
}

int enough_live_nodes(site_def *site, ulong basic_timeout) {
  node_no i = 0;
  double t = task_now();
  node_no n = 0;
  node_no maxnodes = get_maxnodes(site);
  node_no self = get_nodeno(site);

  update_detected(site, 0);

  /* IFDBG(D_DETECT, FN; NDBG(maxnodes,d); );*/
  if (maxnodes == 0) return 0;
  for (i = 0; i < maxnodes; i++) {
    ulong timeout = basic_timeout;
    double diff = task_now() - site->servers[i]->large_transfer_detected;
    if (diff < timeout) {
      timeout = timeout << 1;
    }
    if (i == self || t - site->detected[i] < timeout) {
      n++;
    }
  }
/* IFDBG(D_DETECT, FN; NDBG(maxnodes,d); NDBG(n,d);); */
#ifdef NODE_0_IS_ARBITRATOR
  return maxnodes > 0 &&
         (n > maxnodes / 2 ||
          (ARBITRATOR_HACK && (get_nodeno(site) == 0) && (2 == maxnodes)));
#else
  return maxnodes > 0 &&
         (n > maxnodes / 2 || (ARBITRATOR_HACK && (2 == maxnodes)));
#endif
}

static void send_my_view(site_def const *site);

static void update_global_count(site_def *site) {
  u_int i;
  u_int nodes = get_maxnodes(site);

  site->global_node_count = 0;
  for (i = 0; i < nodes && i < site->global_node_set.node_set_len; i++) {
    if (site->global_node_set.node_set_val[i]) site->global_node_count++;
  }
}

static void check_global_node_set(site_def *site, int *notify) {
  u_int i;
  u_int nodes = get_maxnodes(site);

  all_nodes_valid = 1;
  site->global_node_count = 0;
  for (i = 0; i < nodes && i < site->global_node_set.node_set_len; i++) {
    int detect = detect_node_timeout(site, i);
    if (!detect) {
      all_nodes_valid = 0;
    }
    IFDBG(
        D_DETECT, if (i == 0) {
          FN;
          NDBG(task_now(), f);
        });
    IFDBG(D_DETECT, FN; NDBG(i, d); NDBG(detect, d);
          NDBG(site->detected[i], f));
    if (site->global_node_set.node_set_val[i]) site->global_node_count++;
    if (site->global_node_set.node_set_val[i] != detect) {
      *notify = 1;
    }
    IFDBG(D_DETECT, FN; NDBG(i, u); NDBG(*notify, d));
  }
}

static node_no get_leader(site_def const *s) {
  if (s) {
    node_no leader = 0;
    int silent = DEFAULT_DETECTOR_LIVE_TIMEOUT;
    if (the_app_xcom_cfg) {
      silent = the_app_xcom_cfg->m_flp_timeout;
    }
    for (leader = 0; leader < get_maxnodes(s); leader++) {
      if (!may_be_dead(s->detected, leader, task_now(), silent,
                       s->servers[leader]->unreachable) &&
          is_set(s->global_node_set, leader))
        return leader;
    }
  }
  return 0;
}

int iamtheleader(site_def const *s) {
  if (!s)
    return 0;
  else
    return get_leader(s) == s->nodeno;
}

extern synode_no executed_msg;
extern synode_no max_synode;

static site_def *last_x_site = nullptr;

server_timewait_info server_timewait[NSERVERS];

void invalidate_detector_sites(site_def *site) {
  if (last_x_site == site) {
    last_x_site = nullptr;
  }
}

static void initiate_server_timeout() {
  u_int i;
  for (i = 0; i < NSERVERS; i++) {
    server_timewait[i].last_removed_time = task_now();
    memset(server_timewait[i].ip, 0, IP_MAX_SIZE);
    server_timewait[i].port = 0;
    server_timewait[i].occupied = 0;
    server_timewait[i].timeout = 0;
  }
}

void notify_detector_when_forced() { initiate_server_timeout(); }

static inline void save_server_timeout(server_timewait_info *server_info,
                                       const char *ip, xcom_port port,
                                       int timeout) {
  server_info->last_removed_time = task_now();
  server_info->occupied = 1;
  strcpy(server_info->ip, ip);
  server_info->port = port;
  server_info->timeout = timeout;
}

void notify_detector_when_needed(const char *ip, xcom_port port, int timeout) {
  u_int j;
  for (j = 0; j < NSERVERS; j++) {
    if (server_timewait[j].occupied) {
      double diff = task_now() - server_timewait[j].last_removed_time;
      if (diff > 60.0) {
        save_server_timeout(server_timewait + j, ip, port, timeout);
        break;
      } else {
        int retval = ((port == server_timewait[j].port) &&
                      strcmp(ip, server_timewait[j].ip) == 0);
        if (retval) {
          save_server_timeout(server_timewait + j, ip, port, timeout);
          break;
        } else {
          continue;
        }
      }
    } else {
      save_server_timeout(server_timewait + j, ip, port, timeout);
      break;
    }
  }
}

void notify_detector_when_removed(u_int n, node_address *names, int timeout) {
  u_int i;
  for (i = 0; i < n; i++) {
    node_address const *name = names + i;
    char ip[IP_MAX_SIZE];
    xcom_port port;
    int error = get_ip_and_port(name->address, ip, &port);
    if (!error) {
      notify_detector_when_needed(ip, port, timeout);
    }
  }
}

int ask_for_detector_if_added_ok(u_int n, node_address *names) {
  u_int i;

  if (n > 1) {
    G_WARNING("ask_for_detector_if_added_ok num:%u, not expected", n);
    return 1;
  }

  for (i = 0; i < n; i++) {
    node_address const *name = names + i;
    char ip[IP_MAX_SIZE];
    xcom_port port;
    int error = get_ip_and_port(name->address, ip, &port);
    if (!error) {
      u_int j;
      for (j = 0; j < NSERVERS; j++) {
        if (server_timewait[j].occupied) {
          int retval = ((port == server_timewait[j].port) &&
                        strcmp(ip, server_timewait[j].ip) == 0);
          if (retval) {
            double diff = task_now() - server_timewait[j].last_removed_time;
            if (diff <= server_timewait[j].timeout) {
              return 0;
            } else {
              return 1;
            }
          } else {
            continue;
          }
        } else {
          break;
        }
      }
    } else {
      G_WARNING("ask_for_detector_if_added_ok get ip and port error");
    }
  }

  return 1;
}

static void check_local_node_set(site_def *site, int *notify) {
  u_int i;
  u_int nodes = get_maxnodes(site);

  all_nodes_valid = 1;
  for (i = 0; i < nodes && i < site->local_node_set.node_set_len; i++) {
    int detect = detect_node_timeout(site, i);
    if (!detect) {
      all_nodes_valid = 0;
    }
    if (site->local_node_set.node_set_val[i] != detect) {
      site->local_node_set.node_set_val[i] = detect;
      *notify = 1;
    }
    IFDBG(D_DETECT, FN; NDBG(i, u); NDBG(*notify, d));
  }
}

/* Notify others about our current view */
int detector_task(task_arg arg [[maybe_unused]]) {
  DECL_ENV
  int notify;
  int local_notify;
  ulong basic_timeout;
  ENV_INIT
  END_ENV_INIT
  END_ENV;

  TASK_BEGIN
  last_x_site = nullptr;
  ep->notify = 1;
  ep->local_notify = 1;
  ep->basic_timeout = DEFAULT_DETECTOR_LIVE_TIMEOUT;
  initiate_server_timeout();
  G_INFO("enter detector_task");
  IFDBG(D_DETECT, FN;);
  while (!xcom_shutdown) {
    {
      site_def *x_site = get_executor_site_rw();

      IFDBG(D_DETECT, FN; SYCEXP(executed_msg); SYCEXP(max_synode));
      IFDBG(D_DETECT, FN; PTREXP(x_site); NDBG(get_nodeno(x_site), u));

      if (x_site && get_nodeno(x_site) != VOID_NODE_NO) {
        if (x_site != last_x_site) {
          reset_disjunct_servers(last_x_site, x_site);
        }
        update_detected(x_site, 0);
        if (x_site != last_x_site) {
          last_x_site = x_site;
          ep->notify = 1;
          ep->local_notify = 1;
          G_INFO("set local notify true when site is different");
        }

        if (the_app_xcom_cfg) {
          ep->basic_timeout = the_app_xcom_cfg->m_flp_timeout;
        }
        IFDBG(D_DETECT, FN; PTREXP(x_site); NDBG(get_nodeno(x_site), u));
        IFDBG(D_DETECT, FN;
              COPY_AND_FREE_GOUT(dbg_node_set(x_site->global_node_set)));
        IFDBG(D_DETECT, FN;
              COPY_AND_FREE_GOUT(dbg_node_set(x_site->local_node_set)));
        check_global_node_set(x_site, &ep->notify);
        update_global_count(x_site);
        IFDBG(D_DETECT, FN; NDBG(iamtheleader(x_site), d);
              NDBG(enough_live_nodes(x_site, ep->basic_timeout), d););
        /* Send xcom message if node has changed state */
        IFDBG(D_DETECT, FN; NDBG(ep->notify, d));
        if (ep->notify && iamtheleader(x_site) &&
            enough_live_nodes(x_site, ep->basic_timeout)) {
          const site_def *current_site_def = get_site_def();
          if (current_site_def) {
            server *my_server = current_site_def->servers[x_site->nodeno];
            if (my_server) {
              G_INFO(
                  "A configuration change was detected. Sending a Global View "
                  "Message to all nodes. My node identifier is %d and my "
                  "address "
                  "is %s:%d",
                  x_site->nodeno, my_server->srv, my_server->port);
            }
          }
          ep->notify = 0;
          G_INFO("call send_my_view in detector");
          send_my_view(x_site);
        } else {
          if (ep->notify) {
            if (iamtheleader(x_site)) {
              G_INFO("not enough live nodes for sending my view");
            }
          }
        }
      }

      if (x_site && get_nodeno(x_site) != VOID_NODE_NO) {
        IFDBG(D_DETECT, FN; PTREXP(x_site); NDBG(get_nodeno(x_site), u));
        IFDBG(D_DETECT, FN;
              COPY_AND_FREE_GOUT(dbg_node_set(x_site->global_node_set)));
        IFDBG(D_DETECT, FN;
              COPY_AND_FREE_GOUT(dbg_node_set(x_site->local_node_set)));
        update_global_count(x_site);
        check_local_node_set(x_site, &ep->local_notify);
        IFDBG(D_DETECT, FN; NDBG(ep->local_notify, d));
        if (ep->local_notify) {
          ep->local_notify = 0;
          G_INFO("call deliver_view_msg in detector");
          deliver_view_msg(x_site); /* To application */
        }
      }
    }
    TIMED_TASK_WAIT(&detector_wait, 1.0);
  }

  G_INFO("exit detector_task");

  FINALLY
  IFDBG(D_BUG, FN; STRLIT(" shutdown "));
  TASK_END;
}

node_set detector_node_set(site_def const *site) {
  node_set new_set;
  new_set.node_set_len = 0;
  new_set.node_set_val = nullptr;
  if (site) {
    u_int nodes = get_maxnodes(site);
    alloc_node_set(&new_set, nodes);
    {
      u_int i = 0;
      for (i = 0; i < nodes; i++) {
        new_set.node_set_val[i] = detect_node_timeout(site, i);
      }
    }
  }
  return new_set;
}

static void send_my_view(site_def const *site) {
  app_data_ptr a = new_app_data();
  pax_msg *msg = pax_msg_new(null_synode, site);
  IFDBG(D_DETECT, FN;);
  a->body.c_t = view_msg;
  a->body.app_u_u.present = detector_node_set(site);
  a->app_key = site->start;

  G_INFO("send_my_view calls xcom_send");
  xcom_send(a, msg);
}

void send_global_view() {
  site_def const *x_site = get_executor_site();
  if (iamtheleader(x_site)) {
    send_my_view(x_site);
  }
}

/* Alive task */

/*
   If a new configuration has been forced, the node's number assigned during
   the reconfiguration may be invalid. Specifically, this situation may happen
   when the network is down.

   This code is used to try to fix that by calling the xcom_find_node_index.
 */
static void validate_update_configuration(site_def const *site,
                                          synode_no alive_synode) {
  if (site && get_nodeno(site) == VOID_NODE_NO) {
    site_def *site_rw = find_site_def_rw(alive_synode);
    site_rw->nodeno = xcom_find_node_index(&site_rw->nodes);
  }
}

/* Send alive messages periodically */
#ifdef TASK_EVENT_TRACE
static unsigned int dump = 0;
#endif

int alive_task(task_arg arg [[maybe_unused]]) {
  DECL_ENV
  pax_msg *i_p;
  pax_msg *you_p;
  int silent;
  ENV_INIT
  END_ENV_INIT
  END_ENV;
  TASK_BEGIN

  ep->i_p = ep->you_p = nullptr;
  ep->silent = DEFAULT_DETECTOR_LIVE_TIMEOUT;

  while (!xcom_shutdown) {
    {
      double sec = task_now();
      synode_no alive_synode = get_current_message();
      site_def const *site = find_site_def(alive_synode);

      /*
        If a new configuration has been forced, the site's configuration may be
        invalid. Specifically, this function is called to verify if the site's
        node number is valid and fix it if this is not valid.
      */
      validate_update_configuration(site, alive_synode);

      if (site && get_nodeno(site) != VOID_NODE_NO) {
        /* Send alive if we have not been active for some time */
        if (server_active(site, get_nodeno(site)) < sec - 0.5) {
          replace_pax_msg(&ep->i_p, pax_msg_new(alive_synode, site));
          ep->i_p->op = i_am_alive_op;
          send_to_all_site(site, ep->i_p, "alive_task");
        }

        /* Ping nodes which seem absent */
        {
          node_no i;
          for (i = 0; i < get_maxnodes(site); i++) {
            if (i != get_nodeno(site)) {
              if (the_app_xcom_cfg) {
                ep->silent = the_app_xcom_cfg->m_flp_timeout;
              }
              double diff =
                  task_now() - site->servers[i]->large_transfer_detected;
              if (diff < ep->silent) {
                ep->silent = ep->silent << 1;
              }
              if (may_be_dead(site->detected, i, sec, ep->silent,
                              site->servers[i]->unreachable)) {
                replace_pax_msg(&ep->you_p, pax_msg_new(alive_synode, site));
                ep->you_p->op = are_you_alive_op;

                ep->you_p->a = new_app_data();
                ep->you_p->a->app_key.group_id = ep->you_p->a->group_id =
                    get_group_id(site);
                ep->you_p->a->body.c_t = xcom_boot_type;
                G_INFO(
                    "alive task set xcom_boot_type for node:%d, get "
                    "node no:%d",
                    i, get_nodeno(site));
                init_node_list(1, &site->nodes.node_list_val[i],
                               &ep->you_p->a->body.app_u_u.nodes);

                IFDBG(D_DETECT, FN; COPY_AND_FREE_GOUT(
                          dbg_list(&ep->you_p->a->body.app_u_u.nodes)););

                send_server_msg(site, i, ep->you_p);
              }
            }
          }
        }
      }
    }
    TASK_DELAY(1.0);
#ifdef TASK_EVENT_TRACE
    if (dump++ % 10 == 0) dump_task_events();
#endif
  }
  FINALLY
  IFDBG(D_BUG, FN; STRLIT(" shutdown "));
  replace_pax_msg(&ep->i_p, nullptr);
  replace_pax_msg(&ep->you_p, nullptr);
  TASK_END;
}
