# DECISIONS.md

1. **Indexes.**
   - `employees: { emp_code: 1 }` (unique): guarantees employee code uniqueness and O(1) lookups.
   - `employees: { department: 1, joined_on: 1 }`: supports department filtering and headcount eligibility checks.
   - `attendance_logs: { emp_code: 1, date: 1 }` (unique): guarantees one log per employee per day and eliminates race conditions.
   - `attendance_logs: { date: -1, emp_code: 1 }`: supports sorting and cursor pagination on list attendance.
   - `attendance_logs: { emp_code: 1, punch_in: -1 }`: enables instant lookup of open punch-ins during punch-out.
   - *Rejected:* Single index on `history.at`, as history is an embedded append-only audit trail and never queried by date range independently.

2. **Punch-in race.**
   Two concurrent requests calculate the same attendance date. Both issue `insert_one()`. The database compound unique index on `(emp_code, date)` ensures MongoDB atomically commits exactly one insert (returning 201). The second request encounters a `DuplicateKeyError`, which our exception handler intercepts to return HTTP 409 Conflict.

3. **Ties.**
   The leaderboard uses MongoDB's `$setWindowFields` with `$rank` (standard competition ranking: 1, 2, 2, 4). The `limit` filter (`$match: { rank: { $lte: limit } }`) executes after ranking. If two employees tie at the cutoff rank, both are included in the response (producing >`limit` rows), ordered by `emp_code` ascending.

4. **Headcount.**
   The pipeline originates in `employees` rather than `attendance_logs`, filtering `joined_on <= end_of_month`. It joins attendance logs via `$lookup`. Employees with zero logs produce an empty array, yet still increment the department's `$sum: 1` headcount.

5. **One thing you would change for 100x data.**
   Pre-aggregate attendance rollups (daily and monthly summaries) asynchronously via Change Streams or background workers, and shard `attendance_logs` by `{ emp_code: 1, date: 1 }` to prevent scanning millions of raw logs during analytical queries.
