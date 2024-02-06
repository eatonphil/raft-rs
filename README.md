# raft-rs

Not my first time implementing Raft. I wrote about [another
implementation](https://notes.eatonphil.com/2023-05-25-raft.html) in
Go I did. But you don't learn a concept well until you've implemented
it a few times. And I wanted some practice with Rust.

Achieved:
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

## References

- The Raft Paper: [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- Diego Ongaro's Thesis: [Consensus: Bridging Theory and Practice](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- Diego Ongaro's TLA+ spec: [TLA+ specification for the Raft consensus algorithm](https://github.com/ongardie/raft.tla)
