# raft-rs

Not my first time implementing Raft. I wrote about [another
implementation](https://notes.eatonphil.com/2023-05-25-raft.html) in
Go I did. But you don't learn a concept well until you've implemented
it a few times. And I wanted some practice with Rust.

Goals:
- No dependencies beyond the standard library.
- Leader election.
- Log replication.

Non-goals (for now):
- Production use.
- Snapshots and log truncation.
- Cluster membership changes.

```console
$ cargo test
```
