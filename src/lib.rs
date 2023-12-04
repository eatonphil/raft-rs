use std::convert::TryInto;
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::io::{BufRead, BufReader, BufWriter};
use std::os::unix::prelude::FileExt;
use std::time::Duration;

const PAGESIZE: u64 = 4096;

const POLYNOMIAL: u32 = 0xEDB88320;
struct CRC32 {
    result: u32,
}

impl CRC32 {
    fn new() -> CRC32 {
        CRC32 { result: 0xFFFFFFFF }
    }

    fn update(&mut self, input: &[u8]) {
        for byte in input.iter() {
            self.result ^= *byte as u32;

            for _ in 0..8 {
                self.result = (self.result >> 1) ^ ((self.result & 1) * POLYNOMIAL);
            }
        }
    }

    fn sum(&self) -> u32 {
        !self.result
    }
}

fn crc32(input: &[u8]) -> u32 {
    let mut c = CRC32::new();
    c.update(input);
    c.sum()
}

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
        if checksum != crc32(&metadata[0..37]) {
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
            let mut actual_checksum = CRC32::new();
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

            let mut checksum = CRC32::new();
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

        let checksum = crc32(&metadata[0..37]);
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
        if checksum != crc32(&buffer[0..34]) {
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
        if checksum != crc32(&buffer[0..17]) {
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
        if checksum != crc32(&buffer[0..50]) {
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
        if checksum != crc32(&buffer[0..17]) {
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
    fn test_crc32() {
        let input = vec![
            ("", 0),
            (
                "sadkjflksadfjsdklfjsdlkfjasdflaksdjfalskdfjasldkfjasdlfasdf",
                0x633DFF42,
            ),
            ("What a great little message.", 0xAEABCE75),
            ("f;lkjasdf;lkasdfasd", 0xF570C312),
        ];
        for (input, output) in input.into_iter() {
            assert_eq!(crc32(input.as_bytes()), output);
        }
    }
}
