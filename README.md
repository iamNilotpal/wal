# Write-Ahead Log (WAL) Implementation in Go

This project is a simple and reliable implementation of a Write-Ahead Log (WAL)
in Go. A WAL helps systems like databases or distributed applications keep their
data safe, even after a crash. It works by saving changes to a log file before
applying them, so the system can recover to its last good state if something
goes wrong.

---

## Why Use WAL?

Write-ahead logs provide several key benefits:

1. **Durability**: All state changes are recorded on disk before being applied,
   ensuring persistence across crashes.
2. **High Write-Throughput**: Sequential writes optimize disk I/O for better
   performance.
3. **Data Integrity**: Append-only logging minimizes the risk of corrupting
   existing entries during crashes.

---

## Key Concepts in This Implementation

- **Segmentation**: Logs are divided into manageable segments to improve
  scalability and simplify log compaction. Older segments can be archived or
  cleaned up without affecting the active log.

- **Auto Recovery**: After a crash, the WAL automatically replays uncommitted
  log entries to restore the system to a consistent state, ensuring minimal
  downtime and data loss.

- **Checksums**: Each log entry includes a checksum to ensure integrity.
  Corrupted entries are detected and handled gracefully during recovery.
