# High Availability (HA) Integration Guide

## Overview

The ansible-rulebook now supports High Availability (HA) mode using PostgreSQL-based leader election. In HA mode, multiple workers can run simultaneously, but only the leader will actively process events. If the leader fails, another worker automatically takes over.

## How It Works

### Architecture

```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│  Worker 1   │      │  Worker 2   │      │  Worker 3   │
│  (Leader)   │      │ (Follower)  │      │ (Follower)  │
└──────┬──────┘      └──────┬──────┘      └──────┬──────┘
       │                    │                    │
       │  Compete for lock  │                    │
       └────────────┬───────┴────────────────────┘
                    │
            ┌───────▼────────┐
            │   PostgreSQL   │
            │ Advisory Lock  │
            └────────────────┘
```

- **Leader**: Actively generates events and processes rules
- **Followers**: Dormant, waiting to become leader
- **Failover**: When leader dies, a follower becomes leader (~3-5 seconds)

### Leader Election

- Uses PostgreSQL advisory locks for distributed coordination
- Lock ID must be same across all workers
- Workers poll at regular intervals (configurable)
- Automatic failover on leader failure

## Configuration

### Command-Line Arguments

```bash
ansible-rulebook -r rulebook.yml -i inventory \
  --ha-postgres-dsn "postgresql://user:pass@host:5432/dbname" \
  --ha-uuid "550e8400-e29b-41d4-a716-446655440000" \
  --ha-poll-interval 3.0 \
  --ha-worker-id worker-1
```

#### Arguments:

- **`--ha-postgres-dsn`**: PostgreSQL connection string (required for HA)
  - Format: `postgresql://user:password@host:port/database`
  - Example: `postgresql://eda_user:eda_password@localhost:5432/eda_ha_db`
  - Env var: `EDA_HA_POSTGRES_DSN`

- **`--ha-uuid`**: UUID for HA coordination and state persistence (required for HA)
  - All workers in the same HA cluster must use the SAME UUID
  - UUID string (gets hashed internally to PostgreSQL advisory lock ID)
  - Example: `550e8400-e29b-41d4-a716-446655440000`
  - Env var: `EDA_HA_UUID`

- **`--ha-poll-interval`**: Polling interval in seconds (default: 5.0)
  - How often to check for leadership
  - Lower = faster failover, higher DB load
  - Recommended: 3.0 - 10.0 seconds
  - Env var: `EDA_HA_POLL_INTERVAL`

- **`--ha-worker-id`**: Worker identifier (default: auto-generated)
  - Used for logging and debugging
  - Must be unique per worker
  - Env var: `EDA_HA_WORKER_ID`

### Environment Variables

All arguments can be set via environment variables:

```bash
export EDA_HA_POSTGRES_DSN="postgresql://eda_user:eda_password@localhost:5432/eda_ha_db"
export EDA_HA_UUID="550e8400-e29b-41d4-a716-446655440000"
export EDA_HA_POLL_INTERVAL=5.0
export EDA_HA_WORKER_ID=worker-1

ansible-rulebook -r rulebook.yml -i inventory
```

## Writing HA-Aware Source Plugins

Source plugins automatically receive HA events when running in HA mode.

### Events Provided

Three `asyncio.Event` objects are passed in the `args` dict:

- **`leader_event`**: Set when this worker is leader, cleared when follower
- **`follower_event`**: Set when this worker is follower, cleared when leader
- **`shutdown_event`**: Set when shutting down (for graceful cleanup)

### Pattern: Leader-Only Source

```python
async def main(queue, args):
    """Source plugin that only generates events when leader."""

    # Extract HA events from args
    leader_event = args.get("leader_event")
    follower_event = args.get("follower_event")
    shutdown_event = args.get("shutdown_event")

    # Check if HA is enabled
    if not leader_event:
        # No HA - run normally
        await run_standalone(queue, args)
        return

    # HA mode - only work when leader
    while not shutdown_event.is_set():
        # Wait to become leader
        await leader_event.wait()
        logger.info("Became leader - starting work")

        # Do work while leader
        while leader_event.is_set() and not shutdown_event.is_set():
            # Generate events, poll external systems, etc.
            event = {"data": "..."}
            await queue.put(event)
            await asyncio.sleep(interval)

        logger.info("Lost leadership - stopping work")
```

### Complete Example

See `tests/sources/example_ha_source.py` for a complete, working example.

## Testing HA Mode

### Prerequisites

1. **PostgreSQL Database**:
   ```bash
   # Using Docker
   docker run -d \
     --name eda-postgres \
     -e POSTGRES_DB=eda_ha_db \
     -e POSTGRES_USER=eda_user \
     -e POSTGRES_PASSWORD=eda_password \
     -p 5432:5432 \
     postgres:15
   ```

2. **Install psycopg**:
   ```bash
   pip install psycopg
   ```

### Run Two Workers

**Terminal 1 - Worker 1**:
```bash
ansible-rulebook -r example_ha_rulebook.yml -i /dev/null \
  --ha-postgres-dsn "postgresql://eda_user:eda_password@localhost:5432/eda_ha_db" \
  --ha-uuid "550e8400-e29b-41d4-a716-446655440000" \
  --ha-poll-interval 3.0 \
  --ha-worker-id worker-1 \
  -S tests/sources -v
```

**Terminal 2 - Worker 2**:
```bash
ansible-rulebook -r example_ha_rulebook.yml -i /dev/null \
  --ha-postgres-dsn "postgresql://eda_user:eda_password@localhost:5432/eda_ha_db" \
  --ha-uuid "550e8400-e29b-41d4-a716-446655440000" \
  --ha-poll-interval 3.0 \
  --ha-worker-id worker-2 \
  -S tests/sources -v
```

Or simply use the provided scripts:
```bash
# Terminal 1
./worker1.sh

# Terminal 2
./worker2.sh
```

### Expected Behavior

1. **Startup**:
   - Worker 1 starts, becomes leader, generates events
   - Worker 2 starts, becomes follower, dormant

2. **Normal Operation**:
   - Only Worker 1 (leader) generates events
   - Worker 2 waits silently

3. **Failover**:
   - Kill Worker 1 (Ctrl+C)
   - Worker 2 detects loss within ~3-5 seconds
   - Worker 2 becomes leader, starts generating events

4. **Recovery**:
   - Restart Worker 1
   - Worker 1 becomes follower (Worker 2 still leader)
   - Both workers continue running

## Best Practices

### Database

1. **Use connection pooling** for production
2. **Monitor advisory locks**: `SELECT * FROM pg_locks WHERE locktype = 'advisory'`
3. **Set appropriate timeouts** on connections
4. **Use dedicated database** for HA (can be same server)

### Polling Interval

- **3-5 seconds**: Good balance (recommended)
- **< 3 seconds**: Faster failover, higher DB load
- **> 10 seconds**: Slower failover, lower DB load

### Worker IDs

- Use meaningful names: `worker-us-east-1a`, `worker-pod-xyz`
- Include hostname: `worker-$(hostname)`
- Include timestamp if dynamic: `worker-$(date +%s)`

### Error Handling

Source plugins should:
- Always check `shutdown_event.is_set()`
- Handle `asyncio.CancelledError` gracefully
- Release resources when losing leadership
- Log leadership transitions

### Monitoring

Monitor these metrics:
- Which worker is currently leader
- Leadership transitions (frequency indicates issues)
- Time to failover
- Database connection errors

## Troubleshooting

### Issue: Both workers generate events

**Cause**: Different UUIDs

**Fix**: Ensure all workers use the same `--ha-uuid`

### Issue: No worker becomes leader

**Cause**: Cannot connect to PostgreSQL

**Fix**: Check DSN, verify database is running, check network/firewall

### Issue: Slow failover

**Cause**: Poll interval too high

**Fix**: Decrease `--ha-poll-interval` (e.g., 3.0 seconds)

### Issue: Database connection errors

**Cause**: Invalid DSN or credentials

**Fix**: Test connection manually:
```python
import psycopg
conn = psycopg.connect("postgresql://eda_user:eda_password@localhost:5432/eda_ha_db")
conn.close()
```

### Issue: Source doesn't respect HA events

**Cause**: Source plugin not checking events

**Fix**: Update source plugin to check `leader_event` (see example)

## Migration from Non-HA

Existing source plugins work without modification:
- If HA args not provided, events are `None`
- Plugin can check `if leader_event:` to enable HA behavior
- Backward compatible - no breaking changes

## Advanced: Monitoring Leadership

### Check Current Leader in PostgreSQL

```sql
-- Show which connection holds the advisory lock
-- Note: The objid will be the hashed value of your UUID
SELECT
    pid,
    locktype,
    mode,
    granted,
    pg_blocking_pids(pid) as blocked_by
FROM pg_locks
WHERE locktype = 'advisory';
```

### Log Leadership Changes

The leader election logs transitions:
```
Worker worker-1: Successfully acquired leadership (ha_uuid=550e8400-e29b-41d4-a716-446655440000)
Worker worker-1: Lost leadership
Worker worker-2: Successfully acquired leadership (ha_uuid=550e8400-e29b-41d4-a716-446655440000)
```

## Example Files

- `tests/sources/example_ha_source.py` - Example HA-aware source plugin
- `example_ha_rulebook.yml` - Example rulebook using HA source
- `worker1.sh` and `worker2.sh` - Test scripts for running HA workers
- `test_customer_scripts.py` - Demo of multiple tasks with HA events

## Security Considerations

1. **Protect PostgreSQL credentials**: Use environment variables or secrets
2. **Use SSL**: Add `?sslmode=require` to DSN in production
3. **Restrict database access**: Worker only needs CONNECT privilege
4. **Audit advisory locks**: Monitor for unauthorized lock attempts

## Performance

- **Advisory locks are lightweight**: Minimal database overhead
- **Polling is efficient**: Single SELECT query per interval
- **No table needed**: Uses PostgreSQL internal locks
- **Automatic cleanup**: Locks released on disconnect

## Summary

✅ **Enable HA**: Add `--ha-postgres-dsn` argument
✅ **Same UUID**: All workers must use identical `--ha-uuid`
✅ **Unique worker ID**: Use different `--ha-worker-id` per worker
✅ **Update source plugins**: Check `leader_event` in args
✅ **Test failover**: Kill leader, verify follower takes over
