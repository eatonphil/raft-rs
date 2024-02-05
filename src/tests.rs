// SERVER TESTS
#[cfg(test)]
mod server_tests {
    use super::*;

    pub struct TmpDir {
        pub dir: std::path::PathBuf,
    }

    impl TmpDir {
        pub fn new() -> TmpDir {
            let mut counter: u32 = 0;
            loop {
                let dir = format!("test{}", counter);
                // Atomically try to create a new directory.
                if std::fs::create_dir(&dir).is_ok() {
                    return TmpDir { dir: dir.into() };
                }

                counter += 1;
            }
        }
    }

    // Delete the temp directory when it goes out of scope.
    impl Drop for TmpDir {
        fn drop(&mut self) {
            //std::fs::remove_dir_all(&self.dir).unwrap();
        }
    }

    #[test]
    fn test_update_and_restore() {
        let tmp = TmpDir::new();

        let mut durable = DurableState::new(&tmp.dir, 1);
        durable.restore();
        assert_eq!(durable.current_term, 0);
        assert_eq!(durable.voted_for, 0);
        assert_eq!(durable.next_log_index, 1);
        assert_eq!(
            durable.log_at_index(0),
            LogEntry {
                index: 0,
                term: 0,
                command: vec![],
                client_serial_id: 0,
                client_id: 0,
            }
        );

        durable.update(10234, 40592);
        assert_eq!(durable.current_term, 10234);
        assert_eq!(durable.voted_for, 40592);
        assert_eq!(durable.next_log_index, 1);
        assert_eq!(
            durable.log_at_index(0),
            LogEntry {
                index: 0,
                term: 0,
                command: vec![],
                client_serial_id: 0,
                client_id: 0,
            }
        );
        drop(durable);

        let mut durable = DurableState::new(&tmp.dir, 1);
        assert_eq!(durable.current_term, 0);
        assert_eq!(durable.voted_for, 0);
        assert_eq!(durable.next_log_index, 0);
        assert_eq!(
            durable.log_at_index(0),
            LogEntry {
                index: 0,
                term: 0,
                command: vec![],
                client_serial_id: 0,
                client_id: 0,
            }
        );

        durable.restore();
        assert_eq!(durable.current_term, 10234);
        assert_eq!(durable.voted_for, 40592);
        assert_eq!(durable.next_log_index, 1);
        assert_eq!(
            durable.log_at_index(0),
            LogEntry {
                index: 0,
                term: 0,
                command: vec![],
                client_serial_id: 0,
                client_id: 0,
            }
        );
    }

    #[test]
    fn test_log_append() {
        let tmp = TmpDir::new();

        let mut v = Vec::<LogEntry>::new();
        v.push(LogEntry {
            index: 1,
            term: 0,
            command: "abcdef123456".as_bytes().to_vec(),
            client_serial_id: 0,
            client_id: 0,
        });
        v.push(LogEntry {
            index: 2,
            term: 0,
            command: "foobar".as_bytes().to_vec(),
            client_serial_id: 0,
            client_id: 0,
        });

        // Write two entries and shut down.
        let mut durable = DurableState::new(&tmp.dir, 1);
        durable.restore();
        assert_eq!(durable.next_log_index, 1);
        durable.append(&mut v);
        assert_eq!(durable.next_log_index, 3);
        let prev_offset = durable.next_log_offset;
        drop(durable);

        // Start up and restore. Should have three entries.
        let mut durable = DurableState::new(&tmp.dir, 1);
        durable.restore();
        assert_eq!(prev_offset, durable.next_log_offset);
        assert_eq!(durable.next_log_index, 3);

        for (i, entry) in v.iter().enumerate() {
            assert_eq!(durable.log_at_index(1 + i as u64), *entry);
        }
        // Add in double the existing entries and shut down.
        let before_reverse = v.clone();
        v.reverse();
        let longcommand = b"a".repeat(10_000);
        let longcommand2 = b"a".repeat(PAGESIZE as usize);
        let longcommand3 = b"a".repeat(1 + PAGESIZE as usize);
        v.push(LogEntry {
            index: 3,
            command: longcommand.to_vec(),
            term: 0,
            client_serial_id: 0,
            client_id: 0,
        });
        v.push(LogEntry {
            index: 4,
            command: longcommand2.to_vec(),
            term: 0,
            client_serial_id: 0,
            client_id: 0,
        });
        v.push(LogEntry {
            index: 5,
            command: longcommand3.to_vec(),
            term: 0,
            client_serial_id: 0,
            client_id: 0,
        });
        durable.append(&mut v);

        let mut all = before_reverse;
        all.append(&mut v);
        for (i, entry) in all.iter().enumerate() {
            assert_eq!(durable.log_at_index(1 + i as u64), *entry);
        }

        drop(durable);

        // Start up and restore. Should now have 8 entries.
        let mut durable = DurableState::new(&tmp.dir, 1);
        durable.restore();
        assert_eq!(durable.next_log_index, 8);
        for (i, entry) in all.iter().enumerate() {
            assert_eq!(durable.log_at_index(1 + i as u64), *entry);
        }

        // Check in reverse as well.
        for (i, entry) in all.iter().rev().enumerate() {
            let real_index = all.len() - i;
            assert_eq!(durable.log_at_index(real_index as u64), *entry);
        }
    }

    #[test]
    fn test_rpc_message_encode_decode() {
        let tests = vec![
            RPCMessage {
                body: RPCBody::AppendEntriesRequest(AppendEntriesRequest {
                    request_id: 1948233,
                    term: 88,
                    leader_id: 2132,
                    prev_log_index: 1823,
                    prev_log_term: 193,
                    entries: vec![
                        LogEntry {
                            index: 0,
                            term: 88,
                            command: "hey there".into(),
                            client_serial_id: 102,
                            client_id: 1,
                        },
                        LogEntry {
                            index: 0,
                            term: 90,
                            command: "blub".into(),
                            client_serial_id: 19,
                            client_id: 1,
                        },
                    ],
                    leader_commit: 95,
                }),
                from: 9999,
            },
            RPCMessage {
                body: RPCBody::AppendEntriesRequest(AppendEntriesRequest {
                    request_id: 9234742,
                    term: 91,
                    leader_id: 2132,
                    prev_log_index: 1823,
                    prev_log_term: 193,
                    entries: vec![],
                    leader_commit: 95,
                }),
                from: 9999,
            },
            RPCMessage {
                body: RPCBody::AppendEntriesResponse(AppendEntriesResponse {
                    request_id: 123813,
                    term: 10,
                    success: true,
                    match_index: 0,
                }),
                from: 9999,
            },
            RPCMessage {
                body: RPCBody::AppendEntriesResponse(AppendEntriesResponse {
                    request_id: 983911002,
                    term: 10,
                    success: false,
                    match_index: 1230984,
                }),
                from: 9999,
            },
            RPCMessage {
                body: RPCBody::RequestVoteRequest(RequestVoteRequest {
                    request_id: 1241,
                    term: 1023,
                    candidate_id: 2132,
                    last_log_index: 1823,
                    last_log_term: 193,
                }),
                from: 9999,
            },
            RPCMessage {
                body: RPCBody::RequestVoteResponse(RequestVoteResponse {
                    request_id: 1912390,
                    term: 1023,
                    vote_granted: true,
                }),
                from: 9999,
            },
            RPCMessage {
                body: RPCBody::RequestVoteResponse(RequestVoteResponse {
                    request_id: 12309814,
                    term: 1023,
                    vote_granted: false,
                }),
                from: 9999,
            },
        ];

        for rpcmessage in tests.into_iter() {
            let mut file = Vec::new();
            let mut cursor = std::io::Cursor::new(&mut file);
            let bufwriter = BufWriter::new(&mut cursor);
            println!("Testing transformation of {:?}.", rpcmessage);
            rpcmessage.encode(rpcmessage.from, bufwriter);

            drop(cursor);

            let mut cursor = std::io::Cursor::new(&mut file);
            let bufreader = BufReader::new(&mut cursor);
            let result = RPCMessage::decode(bufreader);
            assert_eq!(result, Ok(rpcmessage));
        }
    }

    pub fn test_server(
        tmp: &TmpDir,
        port: u16,
        servers: usize,
        debug: bool,
    ) -> (Server<TestStateMachine>, mpsc::Receiver<Vec<u8>>) {
        let mut cluster = vec![];
        for i in 0..servers {
            cluster.push(ServerConfig {
                id: 1 + i as u128,
                address: format!("127.0.0.1:{}", port + i as u16).parse().unwrap(),
            })
        }
        let config = Config {
            server_id: 1,
            server_index: 0,
            cluster,

            election_frequency: Duration::from_secs(10),

            random_seed: [0; 4],
            logger_debug: debug,
        };

        let sm = TestStateMachine {};
        return Server::new(1, &tmp.dir, sm, config);
    }

    #[test]
    fn test_rpc_manager() {
        let tmpdir = &TmpDir::new();
        let debug = false;
        let (server, _) = test_server(&tmpdir, 20010, 2, debug);

        let server_id = server.config.cluster[0].id;
        let logger = Logger { server_id, debug };
        let mut rpcm = RPCManager::new(server_id, server.config.cluster.clone(), logger);
        rpcm.start();

        let msg = RPCMessage {
            body: RPCBody::RequestVoteRequest(RequestVoteRequest {
                request_id: 0,
                term: 1023,
                candidate_id: 2,
                last_log_index: 1823,
                last_log_term: 193,
            }),
            from: 1,
        };
        rpcm.send(0, Condition::Follower, server.config.server_id, &msg);
        let received = rpcm.stream_receiver.recv().unwrap();
        assert_eq!(msg, received);

        let mut stop = rpcm.stop_mutex.lock().unwrap();
        *stop = true;
    }

    pub struct TestStateMachine {}

    impl StateMachine for TestStateMachine {
        fn apply(&self, messages: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
            return messages;
        }
    }

    #[test]
    fn test_single_server_apply_end_to_end() {
        let tmpdir = &TmpDir::new();
        let debug = false;
        let (mut server, result_receiver) = test_server(&tmpdir, 20002, 1, debug);

        // First test apply doesn't work as not a leader.
        let apply_result = server.apply(vec![vec![]], vec![1]);
        // Use try_recv() not recv() since recv() would block so the
        // test would hang if the logic were ever wrong. try_recv() is
        // correct since the message *must* at this point be available
        // to read.
        assert_eq!(apply_result, ApplyResult::NotALeader);

        // Now after initializing (realizing there's only one server, so is leader), try again.
        server.init();

        let apply_result = server.apply(vec!["abc".as_bytes().to_vec()], vec![1]);
        assert_eq!(apply_result, ApplyResult::Ok);

        server.tick();

        // See above note about try_recv() vs recv().
        let result = result_receiver.try_recv().unwrap();
        assert_eq!(result, "abc".as_bytes().to_vec());
    }

    #[test]
    fn test_handle_request_vote_request() {
        let tmpdir = &TmpDir::new();
        let debug = false;
        let (mut server, _) = test_server(&tmpdir, 20003, 1, debug);
        server.init();

        let msg = RequestVoteRequest {
            request_id: 88,
            term: 1,
            candidate_id: 2,
            last_log_index: 2,
            last_log_term: 1,
        };
        let response = server.handle_message(RPCMessage {
            body: RPCBody::RequestVoteRequest(msg),
            from: 9999,
        });
        assert_eq!(
            response,
            Some((
                RPCMessage {
                    body: RPCBody::RequestVoteResponse(RequestVoteResponse {
                        request_id: 88,
                        term: 1,
                        vote_granted: true,
                    }),
                    from: server.config.server_id
                },
                9999,
            )),
        );
    }

    #[test]
    fn test_handle_request_vote_response() {
        let tmpdir = &TmpDir::new();
        let debug = false;
        let (mut server, _) = test_server(&tmpdir, 20004, 1, debug);
        server.init();

        let msg = RequestVoteResponse {
            request_id: 0,
            term: 1,
            vote_granted: false,
        };
        let response = server.handle_message(RPCMessage {
            body: RPCBody::RequestVoteResponse(msg),
            from: server.config.server_id,
        });
        assert_eq!(response, None);
    }

    #[test]
    fn test_handle_append_entries_request_all_new_data() {
        let tmpdir = &TmpDir::new();
        let debug = false;
        let (mut server, _) = test_server(&tmpdir, 20007, 1, debug);
        server.init();

        // Must be a follower to accept entries.
        let mut state = server.state.lock().unwrap();
        state.transition(Condition::Follower, 0, 0);
        drop(state);

        let e = LogEntry {
            index: 0,
            term: 0,
            command: "hey there".as_bytes().to_vec(),
            client_serial_id: 0,
            client_id: 1,
        };
        let msg = AppendEntriesRequest {
            request_id: 90,
            term: 0,
            leader_id: 2132,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![e.clone()],
            leader_commit: 95,
        };
        let response = server.handle_message(RPCMessage {
            body: RPCBody::AppendEntriesRequest(msg),
            from: 2132,
        });
        assert_eq!(
            response,
            Some((
                RPCMessage {
                    body: RPCBody::AppendEntriesResponse(AppendEntriesResponse {
                        request_id: 90,
                        term: 0,
                        success: true,
                        match_index: 1,
                    }),
                    from: server.config.server_id,
                },
                2132,
            ))
        );

        let mut state = server.state.lock().unwrap();
        assert_eq!(state.durable.log_at_index(1), e);
    }

    #[test]
    fn test_handle_append_entries_request_overwrite() {
        let tmpdir = &TmpDir::new();
        let debug = false;
        let (mut server, _) = test_server(&tmpdir, 20008, 1, debug);
        server.init();

        // Must be a follower to accept entries.
        let mut state = server.state.lock().unwrap();
        state.transition(Condition::Follower, 0, 0);
        drop(state);

        let mut entries = vec![LogEntry {
            index: 0,
            term: 0,
            command: "abc".as_bytes().to_vec(),
            client_serial_id: 0,
            client_id: 1,
        }];

        let mut state = server.state.lock().unwrap();
        state.durable.append(&mut entries);
        assert_eq!(state.durable.log_at_index(1), entries[0]);
        drop(state);

        let e = LogEntry {
            index: 0,
            // New term differing from what is stored although
            // inserted at index `1` should cause overwrite.
            term: 1,
            command: "hey there".as_bytes().to_vec(),
            client_serial_id: 0,
            client_id: 1,
        };
        let msg = AppendEntriesRequest {
            request_id: 100,
            term: 0,
            leader_id: 2132,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![e.clone()],
            leader_commit: 95,
        };
        let response = server.handle_message(RPCMessage {
            body: RPCBody::AppendEntriesRequest(msg),
            from: 2132,
        });
        assert_eq!(
            response,
            Some((
                RPCMessage {
                    body: RPCBody::AppendEntriesResponse(AppendEntriesResponse {
                        request_id: 100,
                        term: 0,
                        success: true,
                        match_index: 1,
                    }),
                    from: server.config.server_id,
                },
                2132,
            ))
        );

        let mut state = server.state.lock().unwrap();
        assert_eq!(state.durable.log_at_index(1), e);
    }

    #[test]
    fn test_handle_append_entries_request() {
        let tmpdir = &TmpDir::new();
        let debug = false;
        let (mut server, _) = test_server(&tmpdir, 20005, 1, debug);
        server.init();

        let msg = AppendEntriesRequest {
            request_id: 0,
            term: 91,
            leader_id: 2132,
            prev_log_index: 1823,
            prev_log_term: 193,
            entries: vec![],
            leader_commit: 95,
        };
        let response = server.handle_message(RPCMessage {
            body: RPCBody::AppendEntriesRequest(msg),
            from: 2132,
        });
        assert_eq!(
            response,
            Some((
                RPCMessage {
                    body: RPCBody::AppendEntriesResponse(AppendEntriesResponse {
                        request_id: 0,
                        term: 91,
                        success: false,
                        match_index: 0,
                    }),
                    from: server.config.server_id,
                },
                2132
            ))
        );
    }

    #[test]
    fn test_handle_append_entries_response() {
        let tmpdir = &TmpDir::new();
        let debug = false;
        let (mut server, _) = test_server(&tmpdir, 20006, 2, debug);
        server.init();

        let mut state = server.state.lock().unwrap();
        // Multiple servers in the cluster so all start as follower.
        assert_eq!(state.volatile.condition, Condition::Follower);
        // Set to leader.
        state.volatile.condition = Condition::Leader;
        assert_eq!(state.volatile.condition, Condition::Leader);
        assert_eq!(state.durable.current_term, 0);
        drop(state);

        let msg = AppendEntriesResponse {
            request_id: 0,
            // Newer term than server so the server will become a
            // follower and not process the request.
            term: 1,
            success: true,
            match_index: 12,
        };
        let response = server.handle_message(RPCMessage {
            body: RPCBody::AppendEntriesResponse(msg.clone()),
            from: 2,
        });
        assert_eq!(response, None);

        // Term has been updated, server is now follower.
        let mut state = server.state.lock().unwrap();
        assert_eq!(state.volatile.condition, Condition::Follower);
        assert_eq!(state.durable.current_term, 1);

        // Reset state to leader for next tests.
        state.volatile.condition = Condition::Leader;
        drop(state);

        // This time the existing `term: 1` is for the same term so no
        // transition to follower.
        let response = server.handle_message(RPCMessage {
            body: RPCBody::AppendEntriesResponse(msg.clone()),
            from: 2,
        });
        assert_eq!(response, None);

        // Since the response was marked as successful, the
        // `match_index` and `next_index` for this server should have
        // been updated according to `msg.match_index`.
        let state = server.state.lock().unwrap();
        assert_eq!(state.volatile.next_index[1], msg.match_index + 1);
        assert_eq!(state.volatile.match_index[1], msg.match_index);
        drop(state);

        // Let's do another check for `match_index` if the response is
        // marked as not successful.
        let msg = AppendEntriesResponse {
            request_id: 0,
            // Newer term than server so the server will become a
            // follower and not process the request.
            term: 1,
            success: false,
            match_index: 12,
        };
        let response = server.handle_message(RPCMessage {
            body: RPCBody::AppendEntriesResponse(msg.clone()),
            from: 2,
        });
        assert_eq!(response, None);

        let state = server.state.lock().unwrap();
        assert_eq!(state.volatile.next_index[1], msg.match_index);
    }
}

// CRC TESTS
#[cfg(test)]
mod crc32c_tests {
    use super::*;

    #[test]
    fn test_crc32c() {
        let input = vec![
            ("", 0),
            (
                "sadkjflksadfjsdklfjsdlkfjasdflaksdjfalskdfjasldkfjasdlfasdf",
                0xDE647747,
            ),
            ("What a great little message.", 0x165AD1D7),
            ("f;lkjasdf;lkasdfasd", 0x4EA35847),
        ];
        for (input, output) in input.iter() {
            assert_eq!(crc32c(input.as_bytes()), *output);

            // Test streaming (*multiple* calls to update()) too.
            let mut c = CRC32C::new();
            for &byte in input.as_bytes().iter() {
                c.update(&[byte]);
            }
            assert_eq!(c.sum(), *output);
        }
    }
}

// RANDOM TESTS
#[cfg(test)]
mod random_tests {
    use super::*;

    #[test]
    fn test_random() {
        let mut r = Random::new([0; 4]);
        let _ = r.generate_f64();
        let _ = r.generate_bool();
        let _ = r.generate_u32();

        let p = r.generate_percent();
        assert!(p >= 0.0);
        assert!(p <= 1.0);
    }
}

// END TO END TESTS
#[cfg(test)]
mod e2e_tests {
    use super::*;

    fn assert_leader_election_duration_state(
        server: &Server<server_tests::TestStateMachine>,
        leader_elected: &mut u128,
        post_election_ticks: &mut usize,
    ) {
        let state = server.state.lock().unwrap();
        // If it's a leader, it should be the same leader as before.
        if state.volatile.condition == Condition::Leader {
            if *leader_elected == 0 {
                *leader_elected = server.config.server_id;
            } else {
                // Once one is elected it shouldn't change.
                assert_eq!(*leader_elected, server.config.server_id);
                *post_election_ticks -= 1;
            }
        }

        // And the other way around. If it was a leader, it should still be a leader.
        if *leader_elected == server.config.server_id {
            assert_eq!(state.volatile.condition, Condition::Leader);
        }
    }

    fn assert_leader_election_final_state(
        servers: &Vec<Server<server_tests::TestStateMachine>>,
        leader_elected: u128,
    ) {
        // A leader should have been elected.
        assert_ne!(leader_elected, 0);

        for server in servers.iter() {
            let state = server.state.lock().unwrap();
            if server.config.server_id == leader_elected {
                // Leader should be a leader.
                assert_eq!(state.volatile.condition, Condition::Leader);
            } else {
                // All other servers should not be a leader.
                assert_ne!(state.volatile.condition, Condition::Leader);
            }
        }
    }

    fn get_seed() -> [u64; 4] {
        let mut seed = Random::seed();
        let seed_to_string = |s: [u64; 4]| -> String {
            let mut string = String::new();
            for chunk in s.iter() {
                for byte in chunk.to_le_bytes().iter() {
                    string = format!("{}{:02X?}", string, byte);
                }
            }
            assert_eq!(string.len(), 8 * 4 * 2);
            return string;
        };
        if let Ok(s) = std::env::var("RAFT_SEED") {
            assert_eq!(s.len(), 8 * 4 * 2);
            let mut i = 0;
            while i < s.len() {
                let mut bytes = [0; 8];
                let mut j = 0;
                while j < bytes.len() * 2 {
                    bytes[j / 2] = u8::from_str_radix(&s[i + j..i + j + 2], 16).unwrap();
                    j += 2;
                }

                seed[i / 16] = u64::from_le_bytes(bytes);
                i += 16;
            }

            assert_eq!(seed_to_string(seed), s);
        }

        println!("RAFT_SEED={}", seed_to_string(seed));

        seed
    }

    fn test_cluster(
        tmpdir: &server_tests::TmpDir,
        port: u16,
        debug: bool,
    ) -> (
        Vec<Server<server_tests::TestStateMachine>>,
        Vec<mpsc::Receiver<Vec<u8>>>,
        Duration,
    ) {
        let random_seed = get_seed();
        let tick_freq = Duration::from_millis(1);

        let mut cluster = vec![];
        const SERVERS: u8 = 3;
        for i in 0..SERVERS {
            cluster.push(ServerConfig {
                id: 1 + i as u128,
                address: format!("127.0.0.1:{}", port + i as u16).parse().unwrap(),
            })
        }

        let mut servers = vec![];
        let mut results_receivers = vec![];
        let mut per_server_random_seed_generator = Random::new(random_seed);
        for i in 0..SERVERS {
            let config = Config {
                server_id: 1 + i as u128,
                server_index: i as usize,
                cluster: cluster.clone(),

                election_frequency: 500 * tick_freq,

                random_seed: per_server_random_seed_generator.generate_seed(),

                logger_debug: debug,
            };

            let sm = server_tests::TestStateMachine {};
            let (server, results_receiver) = Server::new(1, &tmpdir.dir, sm, config);
            servers.push(server);
            results_receivers.push(results_receiver);
            servers[i as usize].init();
        }

        return (servers, results_receivers, tick_freq);
    }

    fn assert_leader_converge(
        servers: &mut Vec<Server<server_tests::TestStateMachine>>,
        tick_freq: Duration,
        skip_id: u128,
    ) -> u128 {
        let mut post_election_ticks = 50;
        let mut leader_elected = 0;
        let mut max_ticks = 500;
        while leader_elected == 0 || post_election_ticks > 0 {
            max_ticks -= 1;
            if max_ticks == 0 {
                panic!("Ran too long without leader election. Something is wrong.");
            }

            for server in servers.iter_mut() {
                if server.config.server_id == skip_id {
                    continue;
                }

                server.tick();

                assert_leader_election_duration_state(
                    &server,
                    &mut leader_elected,
                    &mut post_election_ticks,
                );
            }
            assert!(tick_freq > Duration::from_millis(0));
            std::thread::sleep(tick_freq);
        }

        assert_leader_election_final_state(&servers, leader_elected);
        return leader_elected;
    }

    #[test]
    fn test_converge_leader_no_entries() {
        let tmpdir = server_tests::TmpDir::new();
        let debug = false;
        let (mut servers, _, tick_freq) = test_cluster(&tmpdir, 20030, debug);

        let old_leader = assert_leader_converge(&mut servers, tick_freq, 0);

        println!("\n\n----- EPOCH -----\n\n");

        // Now what happens if the old leader stops doing anything?
        let mut leader_elected = 0;
        let mut post_election_ticks = 20;
        while leader_elected == 0 {
            for server in servers.iter_mut() {
                if server.config.server_id == old_leader {
                    let mut state = server.state.lock().unwrap();
                    if state.volatile.condition == Condition::Leader {
                        state.transition(Condition::Follower, 0, 0);
                    }
                    continue;
                }

                server.tick();

                assert_leader_election_duration_state(
                    &server,
                    &mut leader_elected,
                    &mut post_election_ticks,
                );
            }

            std::thread::sleep(tick_freq);
        }

        assert_leader_election_final_state(&servers, leader_elected);

        println!("\n\n----- EPOCH -----\n\n");

        // And if all are back up do we converge again?

        _ = assert_leader_converge(&mut servers, tick_freq, 0);
    }

    fn wait_for_all_applied(
        servers: &mut Vec<Server<server_tests::TestStateMachine>>,
        tick_freq: Duration,
        skip_id: u128,
        waiting_for: &Vec<u8>,
    ) {
        let mut applied = vec![false; servers.len()];
        let mut applied_at = vec![0; servers.len()];
        for _ in 0..100 {
            for (i, server) in servers.iter_mut().enumerate() {
                if server.config.server_id == skip_id {
                    continue;
                }

                server.tick();

                let mut state = server.state.lock().unwrap();
                let mut checked = state.volatile.commit_index + 1;
                while checked > 0 {
                    checked -= 1;
                    println!("Checking index: {checked}.");

                    let log = state.durable.log_at_index(checked);
                    let exists_in_log = log.command == *waiting_for;
                    if exists_in_log {
                        println!("Exists for {i} in log at entry: {checked}.");
                        // It should not exist twice in a different location.
                        if applied[i] {
                            assert_eq!(applied_at[i], checked);
                        }
                        applied[i] = true;
                        applied_at[i] = checked;
                    } else {
                        // There shouldn't be any other non-empty data.
                        assert_eq!(log.command.len(), 0);
                    }
                }
            }

            // End the check as soon as we can so tests don't take
            // unnecessarily long.
            let mut all_applied = true;
            for (i, server) in servers.iter().enumerate() {
                if server.config.server_id == skip_id {
                    continue;
                }

                if !applied[i] {
                    all_applied = false;
                    break;
                }
            }
            if all_applied {
                break;
            }

            std::thread::sleep(tick_freq);
        }

        for i in 0..applied.len() {
            if servers[i].config.server_id == skip_id {
                continue;
            }

            assert!(applied[i]);
        }
    }

    fn test_apply_skip_id(skip_id: u128, port: u16) {
        let tmpdir = server_tests::TmpDir::new();
        let debug = std::env::var("RAFT_DEBUG").unwrap_or("".into()) == "true";
        let (mut servers, results_receivers, tick_freq) = test_cluster(&tmpdir, port, debug);

        for server in servers.iter_mut() {
            if server.config.server_id == skip_id as u128 {
                server.stop();
            }
        }

        let leader_id = assert_leader_converge(&mut servers, tick_freq, skip_id);
        assert_ne!(leader_id, skip_id);

        let msg = "abc".as_bytes().to_vec();
        let cmds = vec![msg.clone()];
        let cmd_ids = vec![1];
        let (apply_result, result_receiver) = 'apply_result: {
            for (i, server) in servers.iter_mut().enumerate() {
                if server.config.server_id == leader_id {
                    break 'apply_result (server.apply(cmds, cmd_ids), &results_receivers[i]);
                }
            }

            unreachable!("Invalid leader.");
        };

        // Message should be applied in cluster within 20 ticks.
        for _ in 0..20 {
            if let ApplyResult::Ok = apply_result {
                if let Ok(received) = result_receiver.try_recv() {
                    assert_eq!(received, msg.clone());
                    break;
                }
            } else {
                panic!("Expected ok result.");
            }

            for server in servers.iter_mut() {
                if server.config.server_id == skip_id {
                    continue;
                }

                server.tick();
            }

            std::thread::sleep(tick_freq);
        }

        // Within another 100 ticks, all (but skipped) servers
        // should have applied the same message. (Only 2/3 are
        // required for committing, remember).  Actually this case
        // isn't meaningful unless we expanded the cluster size to
        // 5 because then a quorum would be 3.
        wait_for_all_applied(&mut servers, tick_freq, skip_id, &msg);

        println!("\n\nBringing skipped server back.\n\n");

        drop(servers);
        let (mut servers, senders, tick_freq) = test_cluster(&tmpdir, port, debug);

        _ = assert_leader_converge(&mut servers, tick_freq, skip_id);

        // And within another 100 ticks where we DONT SKIP skip_id,
        // ALL servers in the cluster should have the message.
        // That is, a downed server should catch up with message
        // it missed when it does come back up.
        let skip_id = 0; // 0 is so that none are skipped since 0 isn't a valid id.
        wait_for_all_applied(&mut servers, tick_freq, skip_id, &msg);

        // Explicitly keep this around so the cluster sending results doesn't panic.
        drop(senders);
    }

    #[test]
    fn test_apply_none_down() {
        let port = 20033;
        // Skipping server 0 does nothing since 0 is not a valid
        // server id.
        let skip_id = 0;
        test_apply_skip_id(skip_id, port);
    }

    #[test]
    fn test_apply_one_down() {
        let port = 20036;
        // Skipping `1` checks to make sure application
        // still happens even with 2/3 servers up.
        let skip_id = 1;
        test_apply_skip_id(skip_id, port);
    }

    #[test]
    fn test_bulk() {
        let port = 20039;
        let tmpdir = server_tests::TmpDir::new();
        let debug = false;
        let (mut servers, mut result_receivers, tick_freq) = test_cluster(&tmpdir, port, debug);

        let mut input_senders = vec![];
        let mut output_receivers = vec![];

        const BATCHES: usize = 10;
        const BATCH_SIZE: usize = 10;
        const INNER_BATCH: usize = 10;

        while servers.len() > 0 {
            let (input_sender, input_receiver): (
                mpsc::Sender<(Vec<Vec<u8>>, Vec<u128>)>,
                mpsc::Receiver<(Vec<Vec<u8>>, Vec<u128>)>,
            ) = mpsc::channel();
            input_senders.push(input_sender);

            let (output_sender, output_receiver): (
                mpsc::Sender<ApplyResult>,
                mpsc::Receiver<ApplyResult>,
            ) = mpsc::channel();
            output_receivers.push(output_receiver);

            let mut server = servers.pop().unwrap();

            std::thread::spawn(move || {
                loop {
                    for (msgs, ids) in input_receiver.try_iter() {
                        // println!("Server received message: {:?}, {:?}.", msgs, ids);
                        // Gracefully shut down when we receive an
                        // empty message, so that we can gracefully
                        // shut down when we're done in this test.
                        if msgs.len() == 0 {
                            println!("Shutting server down.");
                            server.stop();
                            drop(server);
                            return;
                        }

                        let result = server.apply(msgs, ids);
                        output_sender.send(result).unwrap();
                    }

                    server.tick();

                    std::thread::sleep(tick_freq);
                }
            });
        }

        println!("Started servers.");

        // 1 Million batches of 10 preallocate before inserting into
        // cluster so that we don't measure allocation time.
        let mut batches = vec![];
        let mut msg: u64 = 0;
        for _ in 0..BATCHES {
            let mut batch = vec![vec![]; BATCH_SIZE];
            for j in 0..BATCH_SIZE {
                batch[j] = vec![];
                for _ in 0..INNER_BATCH {
                    batch[j].extend(msg.to_le_bytes().to_vec());
                    msg += 1;
                }
            }
            batches.push(batch);
        }

        println!("Created batches.");

        // Insert batches.
        let t1 = Instant::now();
        let mut client_serial_id: u128 = 1;
        let mut ids = vec![0; BATCH_SIZE];
        for batch in batches.iter() {
            for i in 0..ids.len() {
                ids[i] = client_serial_id + i as u128;
            }
            // Need to keep submitting each individual batch until it
            // is handled by someone who is a leader.
            'batch: loop {
                for input_sender in input_senders.iter() {
                    // TODO: Could we do this to not need the clone.
                    input_sender.send((batch.clone(), ids.clone())).unwrap();
                }

                for receiver in output_receivers.iter() {
                    'inner: loop {
                        if let Ok(result) = receiver.try_recv() {
                            match result {
                                ApplyResult::NotALeader => {
                                    break 'inner;
                                }
                                // Otherwise keep checking until we hear it's ok.
                                ApplyResult::Ok => {
                                    client_serial_id += BATCH_SIZE as u128;
                                    // println!("Submitted: {}.", client_serial_id - 1);
                                    break 'batch;
                                }
                            }
                        }
                    }
                }
            }
        }

        println!("Submitted batches.");

        // Wait for completion.
        let (sender, receiver) = mpsc::channel();
        while result_receivers.len() > 0 {
            let receiver = result_receivers.pop().unwrap();
            let sender_clone = sender.clone();
            std::thread::spawn(move || loop {
                if let Ok(_) = receiver.recv() {
                    sender_clone.send(1).unwrap();
                    continue;
                }

                return;
            });
        }

        let mut seen = 0;
        while seen < BATCHES * BATCH_SIZE {
            if let Ok(_) = receiver.recv() {
                seen += INNER_BATCH;
            }
        }

        let t = (Instant::now() - t1).as_secs_f64();
        println!(
            "All batches (total entries: {}) complete in {}s. Throughput: {}/s.",
            BATCHES * BATCH_SIZE * INNER_BATCH,
            t,
            (BATCHES as f64 * BATCH_SIZE as f64 * INNER_BATCH as f64) / t,
        );

        if let Ok(skip_check) = std::env::var("SKIP_CHECK") {
            if skip_check == "1" {
                return;
            }
        }

        // Give them time to all apply logs.
        std::thread::sleep(Duration::from_millis(10000));

        // Now shut down all servers.
        for sender in input_senders.iter() {
            sender.send((vec![], vec![])).unwrap();
        }

        // Each thread ticks for 1ms so give ours 2s to wait.
        std::thread::sleep(Duration::from_millis(2000));

        // Now check that batches are in all servers and in the
        // correct order and with nothing else.
        let (servers, _, _) = test_cluster(&tmpdir, port, debug);
        for server in servers.iter() {
            let mut state = server.state.lock().unwrap();
            let mut match_index: u64 = 0;
            let mut checked_index = 0;

            println!("Checking for {}.", server.config.server_id);
            assert_eq!(
                state.durable.debug_client_entry_count(),
                BATCH_SIZE as u64 * BATCHES as u64
            );

            while match_index < BATCH_SIZE as u64 * BATCHES as u64 * INNER_BATCH as u64 {
                let mut expected_msg = vec![];
                for i in 0..INNER_BATCH {
                    expected_msg.extend((i as u64 + match_index).to_le_bytes().to_vec());
                }
                let e = state.durable.log_at_index(checked_index);

                // It must only EITHER be 1) the one we expect or 2) an empty command.
                if e.command == expected_msg {
                    match_index += INNER_BATCH as u64;
                } else {
                    assert_eq!(e.command.len(), 0);
                }

                checked_index += 1;
            }

            // All remaining entries must be empty messages.
            while checked_index < state.durable.next_log_index - 1 {
                let e = state.durable.log_at_index(checked_index);
                assert_eq!(e.command.len(), 0);
                checked_index += 1;
            }
        }
    }
}
