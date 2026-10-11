//------------------------------------------------------------------------------
// UID sharding and alias expansion for xiofsagent.
//
// host:port,host:port is a static server list.
// host,port is an XRootD manager/alias, expanded at mount and every 5 min.
// A host that fails connect/handshake is down for 10 min, then probed
// (hard mounts keep probing; timeo= is the retry interval).
//
// Copyright (c) 2026 by the XRootD Collaboration
//------------------------------------------------------------------------------
#ifndef XIOFS_SHARD_HH
#define XIOFS_SHARD_HH

#include "XioUrl.hh"
#include "xiofs_uapi.h"

#include <cstdint>
#include <string>

bool shardInit(const XioFS::Url &url, std::string &err);
bool shardActive();
void shardSetHard(bool hard);
void shardFail(const std::string &host, int port);
bool shardPick(__u32 uid, std::string &host, int &port, std::string &err);
int shardPush(int ctl, const XioFS::Url &id, std::string &err);
void shardRefresh(const XioFS::Url &url, int ctl);

constexpr unsigned kShardRefreshSec = 300;
constexpr unsigned kShardCooldownSec = 600;

#endif
