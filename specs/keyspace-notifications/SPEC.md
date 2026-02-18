# Keyspace notifications

## Tasks

1. Understand the introduction, basics, and cluster specifics of keyspache notifications in Redis OSS
2. Take the requirements into account
3. Understand the implementation example based on the referenced PR
4. Implement a similar abstraction for redis-py
5. Create test cases to verify if the implementation works


## Introduction

Whereby it is easy to consume keyspace notifications via normal PubSub channels with a standalone Redis client, it is not straightforward to consume them in OSS cluster mode. This raises a need for an additional abstraction on top of PubSub's subscribe/psubscribe commands.

## Basics

Redis allows to notify clients whenever a key is modified. The mechanism that is leveraged is PubSub, which means that you can subscribe to keyspace notifications. There are two types of notifications/channel names:

- **Keyspace**: Have the prefix `__keyspace@0__:` and allow you to listen for operations performed on a specific key (e.g., `__keyspace@0__:mykey`). The notification then returns you the operation performed on the key.
- **Keyevent**: Have the prefix `__keyevent@0__:` and allow you to listen for specific operation events (`__keyevent@0__:del`). The notification then returns you the impacted key.


## Cluster specifics

- In regular PubSub messages are propagated horizontally between nodes, meaning that if client “A” connects to node “X” and issues PSUBSCRIBE foo*, and client “B” connects to a different node “Y” (a peer in the same cluster) and issues PUBLISH foo/bar some_payload, then client “A” will receive the message indirectly, i.e. B=>Y=>X=>A. This does not happen for keyspace and keyevent messages: the server issues events for keys that are manipulated on that node, and only clients subscribed directly to that node are notified.
- Effectively, this means that to effectively use any multi-key subscription (a pattern-based __keyspace, or any __keyevent), clients must connect to all the relevant nodes in that tier. This usually means “all primary nodes in the cluster”. Likewise, this must survive topology changes, i.e. if nodes are added, removed, failed-over, etc: the state must be maintained.
- Additionally, if we consider a single-key keyspace notification (such as __keyspace@0__:mysinglekey), we could use the same multi-node approach, but in a cluster we know that only the node (or nodes, if we consider replicas) that owns the key mysinglekey will issue such messages. To reduce overhead, we will usually prefer to only subscribe to the “correct” server.
- Redis OSS cluster doesn't support multiple logical databases in a sense of using `SELECT <db_index`. However, we might still want to leverage that with standalone client connections.

## Requirements

By using a cluster client, it should be possible to:

- Consume keyspace and key event notifications across the cluster via a new API for subscribing to `keyspace` and `keyevent` channels
- Support multiple logical 
- Take the differences in behavior between normally published messages and keyspace notifications into account - Keyspace notifications aren't propagated between nodes via the cluster bus
- Automatically react to topology changes (e.g. node added/removed, slot migration, ...) by resubscribing to the relevant channels

For consistency reasons, the new abstraction should also be implemented for a standalone client.

## Implementation example

Please take a look at https://github.com/StackExchange/StackExchange.Redis/pull/2995 which implements the same feature for StackExchange.Redis.

## Test cases

- Create a key on node 1 and verify that the notification about the creation of the key is received
- Updated a key on node 1 and verify that the keyspace notification is received
- Update a key on node 2 and verify that the key event notification is received
- Delete a key on node 3 and verify that the deletion notification is received
- Modify a bunch of keys across nodes 1, 2 and 3 that all match the sane pattern and check that all notifications are received
- Move a bunch of slots from node 1 to node 2 that impact some pre-defined keys and ensure that keyspace notifications and key event notifications are received correctly before and after the migration

