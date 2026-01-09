# Automatically Detect Master Changes on Redis Sentinel Failover

## Problem Brief

-Redis clients created through a Sentinel instance must always communicate with the current master node for a monitored service.

-When Sentinel reports that a monitored master has changed, all existing clients obtained from the same Sentinel instance must automatically update their connection target to the new master host and port. This update must occur without requiring the caller to recreate the client or rely on retry-based discovery.

-This behavior must apply consistently to both synchronous and asynchronous Redis clients.

-If multiple master changes occur in rapid succession, clients must reflect only the most recently reported master. Intermediate or stale master references must not persist.

-If a client’s master reference becomes outdated due to missed Sentinel notifications, the client must recover deterministically by re-discovering the current master via Sentinel.

-Existing retry behavior, discovery logic, and non-Sentinel Redis functionality must remain unchanged.

## Success Criteria

- Existing clients transparently follow the active master

- Consecutive master changes resolve to the most recent master only

- Stale client state is recoverable without external intervention

- All behavior is deterministic and objectively testable
