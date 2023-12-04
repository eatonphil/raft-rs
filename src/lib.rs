use std::convert::TryInto;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::io::{BufRead, BufReader, BufWriter};
use std::os::unix::prelude::FileExt;
use std::time::Duration;

const PAGESIZE: u64 = 4096;

pub enum ApplyResult {
    NotALeader,
    Ok(Vec<Vec<u8>>),
}

pub trait StateMachine {
    fn apply(&self, messages: Vec<Vec<u8>>) -> Vec<Vec<u8>>;
}

pub struct Config {
    id: u32,
    address: std::net::SocketAddr,
}

struct LogEntry {
    // In-memory channel for sending results back to a user of the
    // library. Will always be `None` when a log is read back from
    // disk.
    response_sender: Option<std::sync::mpsc::Sender<Vec<u8>>>,

    // Actual data.
    command: Vec<u8>,
    term: u64,
}

//        ON DISK FORMAT
//
// | Byte Range     | Value          |
// |----------------|----------------|
// |        0 - 4   | Magic Number   |
// |        4 - 8   | Format Version |
// |        8 - 16  | Term           |
// |       16 - 17  | Did Vote       |
// |       17 - 21  | Voted For      |
// |       21 - 29  | Log Length     |
// |       29 - 37  | Next Log Entry |
// |       37 - 41  | Checksum       |
// | PAGESIZE - EOF | Log Entries    |
//
//           ON DISK LOG ENTRY FORMAT
//
// | Byte Range                      | Value          |
// |---------------------------------|----------------|
// |  0 - 4                          | Checksum       |
// |  4 - 12                         | Term           |
// | 12 - 20                         | Command Length |
// | 20 - (20 + $Command Length)     | Command        |
//
// After Command, the file will be padding until the next boundary
// divisible by 4k.

struct DurableState {
    // Backing file.
    file: std::fs::File,
    next_log_entry: u64, // Offset of next free space.

    // Actual data.
    current_term: u64,
    voted_for: Option<u32>,
    log: Vec<LogEntry>,
}

impl DurableState {
    fn new(data_directory: &std::path::Path, id: u32) -> DurableState {
        let mut filename = data_directory.to_path_buf();
        filename.push(format!("server_{}.data", id));
        let file = std::fs::File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(filename)
            .expect("Could not open data file.");
        DurableState {
            file,
            next_log_entry: PAGESIZE,
            current_term: 0,
            voted_for: None,
            log: Vec::<LogEntry>::new(),
        }
    }

    fn restore(&mut self) {
        if let Ok(m) = self.file.metadata() {
            if m.len() == 0 {
                return;
            }
        } else {
            return;
        }

        let mut metadata: [u8; PAGESIZE as usize] = [0; PAGESIZE as usize];
        self.file
            .read_exact_at(&mut metadata[0..], 0)
            .expect("Could not read server metadata.");

        // Magic number check.
        assert_eq!(metadata[0..4], 0xFABEF15E_u32.to_le_bytes());

        // Version number check.
        assert_eq!(metadata[4..8], 1_u32.to_le_bytes());

        self.current_term = u64::from_le_bytes(metadata[8..16].try_into().unwrap());
        let did_vote = metadata[16] == 1;
        if did_vote {
            self.voted_for = Some(u32::from_le_bytes(metadata[17..21].try_into().unwrap()));
        }

        let log_length = u64::from_le_bytes(metadata[21..29].try_into().unwrap()) as usize;
        self.log = Vec::with_capacity(log_length);
        if log_length == 0 {
            return;
        }

        self.next_log_entry = u64::from_le_bytes(metadata[29..37].try_into().unwrap());

        let checksum = u32::from_le_bytes(metadata[37..41].try_into().unwrap());
        if checksum != crc32c(&metadata[0..37]) {
            panic!("Bad checksum for data file.");
        }

        self.file.seek(std::io::SeekFrom::Start(PAGESIZE)).unwrap();
        let mut reader = BufReader::new(&self.file);
        while self.log.len() < log_length {
            let mut log_entry = LogEntry {
                response_sender: None,
                term: 0,
                command: Vec::new(),
            };

            let mut metadata: [u8; 20] = [0; 20];
            reader.read_exact(&mut metadata[0..]).unwrap();
            log_entry.term = u64::from_le_bytes(metadata[4..12].try_into().unwrap());
            let command_length = u64::from_le_bytes(metadata[12..20].try_into().unwrap());
            log_entry.command.resize(command_length as usize, b'0');

            let stored_checksum = u32::from_le_bytes(metadata[0..4].try_into().unwrap());
            let mut actual_checksum = CRC32C::new();
            actual_checksum.update(&metadata[4..]);
            reader.read_exact(&mut log_entry.command).unwrap();

            actual_checksum.update(&log_entry.command);
            if stored_checksum != actual_checksum.sum() {
                panic!("Bad checksum for data file.");
            }

            // Read (and drop) until the next page boundary.
            let rest_before_page_boundary = PAGESIZE - ((20 + command_length) % PAGESIZE);
            reader.consume(rest_before_page_boundary as usize);

            self.log.push(log_entry);
        }
    }

    // Durably add logs to disk.
    fn append(&mut self, commands: &[&[u8]]) -> Vec<std::sync::mpsc::Receiver<Vec<u8>>> {
        // Why is this here? Why does it fail when set to the end of
        // restore()?
        self.file
            .seek(std::io::SeekFrom::Start(self.next_log_entry))
            .unwrap();

        let mut buffer: [u8; PAGESIZE as usize];
        let mut receivers =
            Vec::<std::sync::mpsc::Receiver<Vec<u8>>>::with_capacity(commands.len());

        // Write out all logs.
        for command in commands.iter() {
            let (sender, receiver): (
                std::sync::mpsc::Sender<Vec<u8>>,
                std::sync::mpsc::Receiver<Vec<u8>>,
            ) = std::sync::mpsc::channel();

            receivers.push(receiver);

            let entry = LogEntry {
                term: self.current_term,
                // TODO: Do we need this clone here?
                command: command.to_vec(),

                response_sender: Some(sender),
            };

            buffer = [0; PAGESIZE as usize];
            let command_length = entry.command.len() as u64;

            buffer[4..12].copy_from_slice(&entry.term.to_le_bytes());

            buffer[12..20].copy_from_slice(&command_length.to_le_bytes());

            let mut checksum = CRC32C::new();
            checksum.update(&buffer[4..20]);
            checksum.update(&entry.command);
            buffer[0..4].copy_from_slice(&checksum.sum().to_le_bytes());

            let command_first_page = if command_length <= PAGESIZE - 20 {
                command_length
            } else {
                PAGESIZE - 20
            } as usize;
            buffer[20..20 + command_first_page]
                .copy_from_slice(&entry.command[0..command_first_page]);
            self.file.write_all(&buffer).unwrap();

            let mut pages = 1;
            let mut written = command_first_page;
            while written < entry.command.len() {
                let to_write = if entry.command.len() - written > PAGESIZE as usize {
                    PAGESIZE as usize
                } else {
                    entry.command.len() - written
                };
                buffer.copy_from_slice(&entry.command[written..to_write]);
                self.file.write_all(&buffer).unwrap();
                written += PAGESIZE as usize;
                pages += 1;
            }

            self.next_log_entry += pages * PAGESIZE;

            self.log.push(entry);
        }

        // Write log length metadata.
        self.update(self.current_term, self.voted_for);
        receivers
    }

    // Durably save non-log data.
    fn update(&mut self, term: u64, voted_for: Option<u32>) {
        self.current_term = term;
        self.voted_for = voted_for;

        let mut metadata: [u8; PAGESIZE as usize] = [0; PAGESIZE as usize];
        // Magic number.
        metadata[0..4].copy_from_slice(&0xFABEF15E_u32.to_le_bytes());
        // Version.
        metadata[4..8].copy_from_slice(&1_u32.to_le_bytes());

        metadata[8..16].copy_from_slice(&term.to_le_bytes());

        if let Some(v) = voted_for {
            metadata[16] = 1;
            metadata[17..21].copy_from_slice(&v.to_le_bytes());
        } else {
            metadata[16] = 0;
        }

        let log_length = self.log.len() as u64;
        metadata[21..29].copy_from_slice(&log_length.to_le_bytes());

        metadata[29..37].copy_from_slice(&self.next_log_entry.to_le_bytes());

        let checksum = crc32c(&metadata[0..37]);
        metadata[37..41].copy_from_slice(&checksum.to_le_bytes());

        self.file.write_all_at(&metadata, 0).unwrap();

        // fsync.
        self.file.sync_all().unwrap();
    }
}

#[derive(Copy, Clone, PartialEq)]
enum Condition {
    Leader,
    Follower,
    Candidate,
}

struct VolatileState {
    condition: Condition,

    commit_index: usize,
    last_applied: usize,

    // Leader-only state.
    next_index: Vec<usize>,
    match_index: Vec<usize>,
}

impl VolatileState {
    fn new(cluster_size: usize) -> VolatileState {
        VolatileState {
            condition: Condition::Follower,
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::with_capacity(cluster_size),
            match_index: Vec::with_capacity(cluster_size),
        }
    }

    fn reset(&mut self) {
        let count = self.next_index.len();
        for i in 0..count {
            self.next_index[i] = 0;
            self.match_index[i] = 0;
        }
    }
}

struct State {
    tcp_done: bool,

    durable: DurableState,
    volatile: VolatileState,
}

pub struct Server<SM: StateMachine> {
    cluster: Vec<Config>,
    cluster_index: usize, // For this Server.
    sm: SM,

    state: std::sync::Mutex<State>,
}

//             REQUEST WIRE PROTOCOL
//
// | Byte Range | Value                                         |
// |------------|-----------------------------------------------|
// |  0         | Ok                                            |
// |  1         | Request Type                                  |
// |  6 - 14    | Term                                          |
// | 14 - 18    | Leader Id / Candidate Id                      |
// | 18 - 26    | Prev Log Index / Last Log Index               |
// | 26 - 34    | Prev Log Term / Last Log Term                 |
// | 34 - 42    | (Request Vote) Checksum / Leader Commit Index |
// | 42 - 50    | Entries Length                                |
// | 50 - 54    | (Append Entries) Checksum                     |
// | 54 - EOM   | Entries                                       |
//
//             ENTRIES WIRE PROTOCOL
//
// See: ON DISK LOG ENTRY FORMAT.
//
//             RESPONSE WIRE PROTOCOL
//
// | Byte Range   | Value                  |
// |--------------|------------------------|
// |  0           | Ok                     |
// |  1           | Response Type          |
// |  8 - 16      | Term                   |
// |  16          | Success / Vote Granted |
// |  17 - 21     | Checksum               |

struct RequestVoteRequest {
    term: u64,
    candidate_id: u32,
    last_log_index: usize,
    last_log_term: u64,
}

impl RequestVoteRequest {
    fn decode<T: std::io::Read>(
        mut reader: BufReader<T>,
        metadata: [u8; 10],
        term: u64,
    ) -> Option<RPCBody> {
        let mut buffer: [u8; 38] = [0; 38];
        buffer[0..metadata.len()].copy_from_slice(&metadata);
        reader.read_exact(&mut buffer[metadata.len()..]).unwrap();

        let checksum = u32::from_le_bytes(buffer[34..38].try_into().unwrap());
        if checksum != crc32c(&buffer[0..34]) {
            return None;
        }

        let candidate_id = u32::from_le_bytes(buffer[14..18].try_into().unwrap());
        let last_log_index = u64::from_le_bytes(buffer[18..26].try_into().unwrap());
        let last_log_term = u64::from_le_bytes(buffer[26..34].try_into().unwrap());

        Some(RPCBody::RequestVoteRequest(RequestVoteRequest {
            term,
            candidate_id,
            last_log_index: last_log_index as usize,
            last_log_term,
        }))
    }

    fn encode<T: std::io::Write>(&self, _writer: BufWriter<T>) {
        // TODO;
    }
}

struct RequestVoteResponse {
    term: u64,
    vote_granted: bool,
}

impl RequestVoteResponse {
    fn decode<T: std::io::Read>(
        mut reader: BufReader<T>,
        metadata: [u8; 10],
        term: u64,
    ) -> Option<RPCBody> {
        let mut buffer: [u8; 21] = [0; 21];
        buffer[0..metadata.len()].copy_from_slice(&metadata);
        reader.read_exact(&mut buffer[metadata.len()..]).unwrap();

        let checksum = u32::from_le_bytes(buffer[17..21].try_into().unwrap());
        if checksum != crc32c(&buffer[0..17]) {
            return None;
        }

        Some(RPCBody::RequestVoteResponse(RequestVoteResponse {
            term,
            vote_granted: buffer[16] == 1,
        }))
    }

    fn encode<T: std::io::Write>(&self, _writer: BufWriter<T>) {
        // TODO;
    }
}

struct AppendEntriesRequest {
    term: u64,
    leader_id: u32,
    prev_log_index: usize,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: usize,
}

impl AppendEntriesRequest {
    fn decode<T: std::io::Read>(
        mut reader: BufReader<T>,
        metadata: [u8; 10],
        term: u64,
    ) -> Option<RPCBody> {
        let mut buffer: [u8; 54] = [0; 54];
        buffer[0..metadata.len()].copy_from_slice(&metadata);
        reader.read_exact(&mut buffer[metadata.len()..]).unwrap();

        let checksum = u32::from_le_bytes(buffer[50..54].try_into().unwrap());
        if checksum != crc32c(&buffer[0..50]) {
            return None;
        }

        let leader_id = u32::from_le_bytes(buffer[14..18].try_into().unwrap());
        let prev_log_index = u64::from_le_bytes(buffer[18..26].try_into().unwrap());
        let prev_log_term = u64::from_le_bytes(buffer[26..34].try_into().unwrap());
        let leader_commit = u64::from_le_bytes(buffer[34..42].try_into().unwrap());
        let entries_length = u64::from_le_bytes(buffer[42..50].try_into().unwrap());
        let entries = Vec::<LogEntry>::with_capacity(entries_length as usize);

        Some(RPCBody::AppendEntriesRequest(AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index: prev_log_index as usize,
            prev_log_term,
            leader_commit: leader_commit as usize,
            entries,
        }))
    }

    fn encode<T: std::io::Write>(&self, _writer: BufWriter<T>) {
        // TODO;
    }
}

struct AppendEntriesResponse {
    term: u64,
    success: bool,
}

impl AppendEntriesResponse {
    fn decode<T: std::io::Read>(
        mut reader: BufReader<T>,
        metadata: [u8; 10],
        term: u64,
    ) -> Option<RPCBody> {
        let mut buffer: [u8; 21] = [0; 21];
        buffer[0..metadata.len()].copy_from_slice(&metadata);
        reader.read_exact(&mut buffer[metadata.len()..]).unwrap();

        let checksum = u32::from_le_bytes(buffer[17..21].try_into().unwrap());
        if checksum != crc32c(&buffer[0..17]) {
            return None;
        }

        Some(RPCBody::AppendEntriesResponse(AppendEntriesResponse {
            term,
            success: buffer[16] == 1,
        }))
    }

    fn encode<T: std::io::Write>(&self, _writer: BufWriter<T>) {
        // TODO;
    }
}

enum RPCBodyKind {
    RequestVoteRequest = 0,
    RequestVoteResponse = 1,
    AppendEntriesRequest = 2,
    AppendEntriesResponse = 3,
}

enum RPCBody {
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
}

struct RPCMessage {
    ok: bool,
    term: u64,
    body: RPCBody,
}

impl RPCMessage {
    fn decode<T: std::io::Read>(mut reader: BufReader<T>) -> Option<RPCMessage> {
        let mut metadata: [u8; 10] = [0; 10];
        if let Err(e) = reader.read_exact(&mut metadata) {
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut
            {
                return None;
            }

            panic!("Could not read request: {:#?}.", e);
        }

        // Drop any request or response that doesn't have the Ok byte set.
        if metadata[0] != 1 {
            return None;
        }

        let message_type = metadata[1];
        let term = u64::from_le_bytes(metadata[2..10].try_into().unwrap());
        let body = if message_type == RPCBodyKind::RequestVoteRequest as u8 {
            RequestVoteRequest::decode(reader, metadata, term)
        } else if message_type == RPCBodyKind::RequestVoteResponse as u8 {
            RequestVoteResponse::decode(reader, metadata, term)
        } else if message_type == RPCBodyKind::AppendEntriesRequest as u8 {
            AppendEntriesRequest::decode(reader, metadata, term)
        } else if message_type == RPCBodyKind::AppendEntriesResponse as u8 {
            AppendEntriesResponse::decode(reader, metadata, term)
        } else {
            panic!("Unknown request type: {}.", message_type);
        };

        let goodbody = body?;

        Some(RPCMessage {
            ok: true,
            term,
            body: goodbody,
        })
    }

    fn encode<T: std::io::Write>(_writer: BufWriter<T>, _term: u64, _body: RPCBody) {
        // TODO: fill in.
    }
}

impl<SM: StateMachine> Server<SM> {
    pub fn apply(&mut self, commands: &[&[u8]]) -> ApplyResult {
        // Append commands to local durable state if leader.
        let mut state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Leader {
            return ApplyResult::NotALeader;
        }

        let receivers = state.durable.append(commands);
        drop(state);

        // Wait for messages to be applied.
        let to_add = commands.len();
        let mut results = Vec::<Vec<u8>>::with_capacity(to_add);
        for r in receivers {
            for result in r.try_iter() {
                // TODO: Is this clone() necessary?
                results.push(result.clone());
            }
        }
        // TODO: Handle taking too long.

        assert!(results.len() == to_add);
        ApplyResult::Ok(results)
    }

    fn handle_request_vote_request(
        &mut self,
        stream: BufWriter<std::net::TcpStream>,
        _request: RequestVoteRequest,
    ) {
        // TODO: fill in.

        let state = self.state.lock().unwrap();
        RequestVoteResponse {
            term: state.durable.current_term,
            vote_granted: false,
        }
        .encode(stream);
    }

    fn handle_request_vote_response(
        &mut self,
        _stream: BufWriter<std::net::TcpStream>,
        _response: RequestVoteResponse,
    ) {
        // TODO: fill in.
    }

    fn handle_append_entries_request(
        &mut self,
        stream: BufWriter<std::net::TcpStream>,
        request: AppendEntriesRequest,
    ) {
        let state = self.state.lock().unwrap();
        if request.term < state.durable.current_term {
            AppendEntriesResponse {
                term: state.durable.current_term,
                success: false,
            }
            .encode(stream);
            return;
        }

        // TODO: fill in.

        AppendEntriesResponse {
            term: state.durable.current_term,
            success: true,
        }
        .encode(stream);
    }

    fn handle_append_entries_response(
        &mut self,
        _stream: BufWriter<std::net::TcpStream>,
        _response: AppendEntriesResponse,
    ) {
        // TODO: fill in.
    }

    fn handle_request(&mut self, stream: std::net::TcpStream) {
        stream
            .set_read_timeout(Some(Duration::from_millis(2000)))
            .unwrap();
        let reader = BufReader::new(stream.try_clone().unwrap());

        let message = if let Some(message) = RPCMessage::decode(reader) {
            message
        } else {
            return;
        };

        let mut state = self.state.lock().unwrap();
        if message.term > state.durable.current_term {
            let voted_for = state.durable.voted_for;
            state.durable.update(message.term, voted_for);
            state.volatile.condition = Condition::Follower;
        }

        drop(state); // Release the lock on self.state.

        let bufwriter = BufWriter::new(stream);
        match message.body {
            RPCBody::RequestVoteRequest(r) => self.handle_request_vote_request(bufwriter, r),
            RPCBody::RequestVoteResponse(r) => self.handle_request_vote_response(bufwriter, r),
            RPCBody::AppendEntriesRequest(r) => self.handle_append_entries_request(bufwriter, r),
            RPCBody::AppendEntriesResponse(r) => self.handle_append_entries_response(bufwriter, r),
        };
    }

    fn leader_maybe_new_quorum(&mut self) {
        // If there exists an N such that N > commitIndex, a majority
        // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // set commitIndex = N (§5.3, §5.4).
        let quorum_needed = self.cluster.len() / 2;
        let mut state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Leader {
            return;
        }

        let log_length = state.durable.log.len();
        for i in log_length..0 {
            let mut quorum = quorum_needed;

            if i <= state.volatile.commit_index {
                break;
            }

            for (i, &match_index) in state.volatile.match_index.iter().enumerate() {
                // self always counts as part of quorum, so skip it in
                // the count. quorum_needed already takes self into
                // consideration (`len() / 2` not `len() / 2 + 1`).
                if i == self.cluster_index {
                    continue;
                }

                if match_index >= i && state.durable.log[i].term == state.durable.current_term {
                    quorum -= 1;
                }
            }

            if quorum == 0 {
                state.volatile.commit_index = i;
            }
        }
    }

    fn leader_send_heartbeat(&mut self) {
        // Upon election: send initial empty AppendEntries RPCs
        // (heartbeat) to each server; repeat during idle periods to
        // prevent election timeouts (§5.2)
        let state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Leader {
            return;
        }

        _ = 1; // Make clippy happy.
    }

    fn follower_maybe_become_candidate(&mut self) {
        // If election timeout elapses without receiving AppendEntries
        // RPC from current leader or granting vote to candidate:
        // convert to candidate
        let state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Follower {
            return;
        }

        _ = 1; // Make clippy happy.
    }

    fn candidate_become_leader(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.volatile.reset();
    }

    fn candidate_request_votes(&mut self) {
        let state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Candidate {
            return;
        }

        drop(state);

        if false {
            self.candidate_become_leader();
        }
    }

    fn apply_entries(&mut self) {
        let mut state = self.state.lock().unwrap();
        let mut to_apply = Vec::<Vec<u8>>::new();
        let starting_index = state.volatile.last_applied + 1;
        while state.volatile.last_applied <= state.volatile.commit_index {
            state.volatile.last_applied += 1;
            to_apply.push(
                state.durable.log[state.volatile.last_applied]
                    .command
                    // TODO: Is this clone necessary?
                    .clone(),
            );
        }

        if !to_apply.is_empty() {
            let results = self.sm.apply(to_apply);
            for (i, result) in results.into_iter().enumerate() {
                if let Some(sender) = &state.durable.log[starting_index + i].response_sender {
                    // TODO: Is this clone necessary?
                    sender.send(result.clone()).unwrap();
                }
            }
        }
    }

    pub fn start(&mut self) {
        self.restore();

        let (tx, rx): (
            std::sync::mpsc::Sender<std::net::TcpStream>,
            std::sync::mpsc::Receiver<std::net::TcpStream>,
        ) = std::sync::mpsc::channel();

        let thread_tx = tx.clone();
        let stop = std::sync::Arc::new(std::sync::Mutex::new(false));
        let thread_stop = stop.clone();

        let address = self.cluster[self.cluster_index].address;

        std::thread::spawn(move || {
            let listener = std::net::TcpListener::bind(address)
                .expect("Could not bind to configured address.");

            for stream in listener.incoming() {
                let stop = thread_stop.lock().unwrap();
                if *stop {
                    break;
                }

                if let Ok(s) = stream {
                    thread_tx.send(s).unwrap();
                }
            }
        });

        loop {
            let state = self.state.lock().unwrap();
            if state.tcp_done {
                let mut stop = stop.lock().unwrap();
                *stop = true;
                break;
            }
            drop(state);

            // Leader operations.
            self.leader_send_heartbeat();
            self.leader_maybe_new_quorum();

            // Follower operations.
            self.follower_maybe_become_candidate();

            // Candidate operations.
            self.candidate_request_votes();

            // All condition operations.
            self.apply_entries();

            for s in rx.try_iter() {
                self.handle_request(s);
            }
        }
    }

    pub fn stop(&mut self) {
        let mut state = self.state.lock().unwrap();
        // Prevent server from accepting any more log entries.
        state.volatile.condition = Condition::Follower;
        state.tcp_done = true;
    }

    pub fn restore(&self) {
        let mut state = self.state.lock().unwrap();
        state.durable.restore();
    }

    pub fn new(
        id: u32,
        data_directory: &std::path::Path,
        sm: SM,
        cluster: Vec<Config>,
    ) -> Server<SM> {
        let cluster_size = cluster.len();
        let cluster_index = cluster
            .iter()
            .position(|c| c.id == id)
            .expect("Server ID must point to valid ID within cluster config.");
        Server {
            cluster,
            cluster_index,
            sm,

            state: std::sync::Mutex::new(State {
                durable: DurableState::new(data_directory, id),
                volatile: VolatileState::new(cluster_size),

                tcp_done: false,
            }),
        }
    }
}

struct CRC32C {
    result: u32,
}

const CRC32C_TABLE: [u32; 256] = [
    0x00000000, 0xF26B8303, 0xE13B70F7, 0x1350F3F4, 0xC79A971F, 0x35F1141C, 0x26A1E7E8, 0xD4CA64EB,
    0x8AD958CF, 0x78B2DBCC, 0x6BE22838, 0x9989AB3B, 0x4D43CFD0, 0xBF284CD3, 0xAC78BF27, 0x5E133C24,
    0x105EC76F, 0xE235446C, 0xF165B798, 0x030E349B, 0xD7C45070, 0x25AFD373, 0x36FF2087, 0xC494A384,
    0x9A879FA0, 0x68EC1CA3, 0x7BBCEF57, 0x89D76C54, 0x5D1D08BF, 0xAF768BBC, 0xBC267848, 0x4E4DFB4B,
    0x20BD8EDE, 0xD2D60DDD, 0xC186FE29, 0x33ED7D2A, 0xE72719C1, 0x154C9AC2, 0x061C6936, 0xF477EA35,
    0xAA64D611, 0x580F5512, 0x4B5FA6E6, 0xB93425E5, 0x6DFE410E, 0x9F95C20D, 0x8CC531F9, 0x7EAEB2FA,
    0x30E349B1, 0xC288CAB2, 0xD1D83946, 0x23B3BA45, 0xF779DEAE, 0x05125DAD, 0x1642AE59, 0xE4292D5A,
    0xBA3A117E, 0x4851927D, 0x5B016189, 0xA96AE28A, 0x7DA08661, 0x8FCB0562, 0x9C9BF696, 0x6EF07595,
    0x417B1DBC, 0xB3109EBF, 0xA0406D4B, 0x522BEE48, 0x86E18AA3, 0x748A09A0, 0x67DAFA54, 0x95B17957,
    0xCBA24573, 0x39C9C670, 0x2A993584, 0xD8F2B687, 0x0C38D26C, 0xFE53516F, 0xED03A29B, 0x1F682198,
    0x5125DAD3, 0xA34E59D0, 0xB01EAA24, 0x42752927, 0x96BF4DCC, 0x64D4CECF, 0x77843D3B, 0x85EFBE38,
    0xDBFC821C, 0x2997011F, 0x3AC7F2EB, 0xC8AC71E8, 0x1C661503, 0xEE0D9600, 0xFD5D65F4, 0x0F36E6F7,
    0x61C69362, 0x93AD1061, 0x80FDE395, 0x72966096, 0xA65C047D, 0x5437877E, 0x4767748A, 0xB50CF789,
    0xEB1FCBAD, 0x197448AE, 0x0A24BB5A, 0xF84F3859, 0x2C855CB2, 0xDEEEDFB1, 0xCDBE2C45, 0x3FD5AF46,
    0x7198540D, 0x83F3D70E, 0x90A324FA, 0x62C8A7F9, 0xB602C312, 0x44694011, 0x5739B3E5, 0xA55230E6,
    0xFB410CC2, 0x092A8FC1, 0x1A7A7C35, 0xE811FF36, 0x3CDB9BDD, 0xCEB018DE, 0xDDE0EB2A, 0x2F8B6829,
    0x82F63B78, 0x709DB87B, 0x63CD4B8F, 0x91A6C88C, 0x456CAC67, 0xB7072F64, 0xA457DC90, 0x563C5F93,
    0x082F63B7, 0xFA44E0B4, 0xE9141340, 0x1B7F9043, 0xCFB5F4A8, 0x3DDE77AB, 0x2E8E845F, 0xDCE5075C,
    0x92A8FC17, 0x60C37F14, 0x73938CE0, 0x81F80FE3, 0x55326B08, 0xA759E80B, 0xB4091BFF, 0x466298FC,
    0x1871A4D8, 0xEA1A27DB, 0xF94AD42F, 0x0B21572C, 0xDFEB33C7, 0x2D80B0C4, 0x3ED04330, 0xCCBBC033,
    0xA24BB5A6, 0x502036A5, 0x4370C551, 0xB11B4652, 0x65D122B9, 0x97BAA1BA, 0x84EA524E, 0x7681D14D,
    0x2892ED69, 0xDAF96E6A, 0xC9A99D9E, 0x3BC21E9D, 0xEF087A76, 0x1D63F975, 0x0E330A81, 0xFC588982,
    0xB21572C9, 0x407EF1CA, 0x532E023E, 0xA145813D, 0x758FE5D6, 0x87E466D5, 0x94B49521, 0x66DF1622,
    0x38CC2A06, 0xCAA7A905, 0xD9F75AF1, 0x2B9CD9F2, 0xFF56BD19, 0x0D3D3E1A, 0x1E6DCDEE, 0xEC064EED,
    0xC38D26C4, 0x31E6A5C7, 0x22B65633, 0xD0DDD530, 0x0417B1DB, 0xF67C32D8, 0xE52CC12C, 0x1747422F,
    0x49547E0B, 0xBB3FFD08, 0xA86F0EFC, 0x5A048DFF, 0x8ECEE914, 0x7CA56A17, 0x6FF599E3, 0x9D9E1AE0,
    0xD3D3E1AB, 0x21B862A8, 0x32E8915C, 0xC083125F, 0x144976B4, 0xE622F5B7, 0xF5720643, 0x07198540,
    0x590AB964, 0xAB613A67, 0xB831C993, 0x4A5A4A90, 0x9E902E7B, 0x6CFBAD78, 0x7FAB5E8C, 0x8DC0DD8F,
    0xE330A81A, 0x115B2B19, 0x020BD8ED, 0xF0605BEE, 0x24AA3F05, 0xD6C1BC06, 0xC5914FF2, 0x37FACCF1,
    0x69E9F0D5, 0x9B8273D6, 0x88D28022, 0x7AB90321, 0xAE7367CA, 0x5C18E4C9, 0x4F48173D, 0xBD23943E,
    0xF36E6F75, 0x0105EC76, 0x12551F82, 0xE03E9C81, 0x34F4F86A, 0xC69F7B69, 0xD5CF889D, 0x27A40B9E,
    0x79B737BA, 0x8BDCB4B9, 0x988C474D, 0x6AE7C44E, 0xBE2DA0A5, 0x4C4623A6, 0x5F16D052, 0xAD7D5351,
];

impl CRC32C {
    fn new() -> CRC32C {
        CRC32C { result: !0 }
    }

    fn update(&mut self, input: &[u8]) {
        for &byte in input.iter() {
            self.result =
                CRC32C_TABLE[((self.result ^ byte as u32) & 0xFF) as usize] ^ (self.result >> 8);
        }
    }

    fn sum(&self) -> u32 {
        return self.result ^ !0;
    }
}

fn crc32c(input: &[u8]) -> u32 {
    let mut c = CRC32C::new();
    c.update(input);
    c.sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TmpDir {
        dir: std::path::PathBuf,
    }

    impl TmpDir {
        fn new() -> TmpDir {
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
            std::fs::remove_dir_all(&self.dir).unwrap();
        }
    }

    #[test]
    fn test_save_and_restore() {
        let tmp = TmpDir::new();

        let mut durable = DurableState::new(&tmp.dir, 1);
        durable.restore();
        assert_eq!(durable.current_term, 0);
        assert_eq!(durable.voted_for, None);
        assert_eq!(durable.log.len(), 0);

        durable.update(10234, Some(40592));
        assert_eq!(durable.current_term, 10234);
        assert_eq!(durable.voted_for, Some(40592));
        assert_eq!(durable.log.len(), 0);
        drop(durable);

        let mut durable = DurableState::new(&tmp.dir, 1);
        assert_eq!(durable.current_term, 0);
        assert_eq!(durable.voted_for, None);
        assert_eq!(durable.log.len(), 0);

        durable.restore();
        assert_eq!(durable.current_term, 10234);
        assert_eq!(durable.voted_for, Some(40592));
        assert_eq!(durable.log.len(), 0);
    }

    #[test]
    fn test_log_append() {
        let tmp = TmpDir::new();

        let mut v = Vec::new();
        v.push(b"abcdef123456"[0..].into());
        v.push(b"foobar"[0..].into());

        // Write two entries and shut down.
        let mut durable = DurableState::new(&tmp.dir, 1);
        durable.restore();
        durable.append(&v);
        drop(durable);

        // Start up and restore. Should have two entries.
        let mut durable = DurableState::new(&tmp.dir, 1);
        durable.restore();
        assert_eq!(durable.log.len(), 2);
        assert_eq!(durable.log[0].term, 0);
        assert_eq!(durable.log[0].command, b"abcdef123456"[0..]);
        assert_eq!(durable.log[1].term, 0);
        assert_eq!(durable.log[1].command, b"foobar"[0..]);

        // Add in double the existing entries and shut down.
        v.reverse();
        durable.append(&v);
        drop(durable);

        // Start up and restore. Should now have four entries.
        let mut durable = DurableState::new(&tmp.dir, 1);
        durable.restore();
        assert_eq!(durable.log.len(), 4);
        assert_eq!(durable.log[0].term, 0);
        assert_eq!(durable.log[0].command, b"abcdef123456"[0..]);
        assert_eq!(durable.log[1].term, 0);
        assert_eq!(durable.log[1].command, b"foobar"[0..]);
        assert_eq!(durable.log[2].term, 0);
        assert_eq!(durable.log[2].command, b"foobar"[0..]);
        assert_eq!(durable.log[3].term, 0);
        assert_eq!(durable.log[3].command, b"abcdef123456"[0..]);
    }

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
        for (input, output) in input.into_iter() {
            println!("For {:?}.{} == {}", input, crc32c(input.as_bytes()), output);
            assert_eq!(crc32c(input.as_bytes()), output);
        }
    }
}
