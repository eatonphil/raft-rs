// References:
// [0] In Search of an Understandable Consensus Algorithm (Extended Version) -- https://raft.github.io/raft.pdf

use std::convert::TryInto;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, Write};
use std::net::SocketAddr;
use std::os::unix::prelude::FileExt;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

const PAGESIZE: u64 = 4096;

#[derive(Debug, PartialEq)]
pub enum ApplyResult {
    NotALeader,
    Ok(Vec<u8>),
}

pub trait StateMachine {
    fn apply(&self, messages: Vec<Vec<u8>>) -> Vec<Vec<u8>>;
}

//        ON DISK FORMAT
//
// | Byte Range     | Value          |
// |----------------|----------------|
// |        0 - 4   | Magic Number   |
// |        4 - 8   | Format Version |
// |        8 - 16  | Term           |
// |       16 - 17  | Did Vote       |
// |       17 - 33  | Voted For      |
// |       33 - 41  | Log Length     |
// |       41 - 45  | Checksum       |
// | PAGESIZE - EOF | Log Entries    |
//
//           ON DISK LOG ENTRY FORMAT
//
// | Byte Range                  | Value           |
// |-----------------------------|-----------------|
// |  0 - 4                      | Checksum        |
// |  4 - 12                     | Term            |
// | 12 - 20                     | Command Length  |
// | 20 - (20 + $Command Length) | Command         |

#[derive(Debug)]
struct LogEntry {
    // In-memory channel for sending results back to a user of the
    // library. Will always be `None` when a log is read back from
    // disk.
    result_sender: Option<mpsc::Sender<ApplyResult>>,

    // Actual data.
    command: Vec<u8>,
    term: u64,
}

impl PartialEq for LogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.command == other.command && self.term == other.term
    }
}

impl LogEntry {
    fn encode(&self, buffer: &mut [u8; PAGESIZE as usize], mut writer: impl std::io::Write) {
        *buffer = [0; PAGESIZE as usize];
        let command_length = self.command.len() as u64;

        buffer[4..12].copy_from_slice(&self.term.to_le_bytes());

        buffer[12..20].copy_from_slice(&command_length.to_le_bytes());

        let mut checksum = CRC32C::new();
        checksum.update(&buffer[4..20]);
        checksum.update(&self.command);
        buffer[0..4].copy_from_slice(&checksum.sum().to_le_bytes());

        let command_first_page = if command_length <= PAGESIZE - 20 {
            command_length
        } else {
            PAGESIZE - 20
        } as usize;
        buffer[20..20 + command_first_page].copy_from_slice(&self.command[0..command_first_page]);
        writer.write_all(buffer).unwrap();

        let mut written = command_first_page;
        while written < self.command.len() {
            let to_write = if self.command.len() - written > PAGESIZE as usize {
                PAGESIZE as usize
            } else {
                self.command.len() - written
            };
            buffer[0..to_write].copy_from_slice(&self.command[written..written + to_write]);
            writer.write_all(buffer).unwrap();
            written += PAGESIZE as usize;
        }
    }

    fn decode(reader: &mut BufReader<impl std::io::Read>) -> Option<LogEntry> {
        let mut metadata: [u8; 20] = [0; 20];
        reader.read_exact(&mut metadata).unwrap();
        let term = u64::from_le_bytes(metadata[4..12].try_into().unwrap());
        let command_length = u64::from_le_bytes(metadata[12..20].try_into().unwrap());

        let mut command = vec![b'0'; command_length as usize];

        let stored_checksum = u32::from_le_bytes(metadata[0..4].try_into().unwrap());
        let mut actual_checksum = CRC32C::new();
        actual_checksum.update(&metadata[4..]);

        assert_eq!(command.len(), command_length as usize);
        reader.read_exact(&mut command).unwrap();
        actual_checksum.update(&command);

        if stored_checksum != actual_checksum.sum() {
            return None;
        }

        // Read (and drop) until the next page boundary.
        let rest_before_page_boundary = PAGESIZE - ((20 + command_length) % PAGESIZE);
        reader.consume(rest_before_page_boundary as usize);

        Some(LogEntry {
            term,
            command,
            result_sender: None,
        })
    }
}

struct DurableState {
    // Backing file.
    file: std::fs::File,

    // Actual data.
    current_term: u64,
    voted_for: Option<u128>,
    log: Vec<LogEntry>,
}

impl DurableState {
    fn new(data_directory: &std::path::Path, id: u128) -> DurableState {
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
            current_term: 0,
            voted_for: None,
            log: vec![],
        }
    }

    fn restore(&mut self) {
        // Ensure we're in the right place to start reading or
        // appending logs. Metadata is always in the first page so we
        // can pread/pwrite it.
        self.file.seek(std::io::SeekFrom::Start(PAGESIZE)).unwrap();

        // If there's nothing to restore, calling append with the
        // required 0th empty log entry will be sufficient to get
        // state into the right place.
        if let Ok(m) = self.file.metadata() {
            if m.len() == 0 {
                self.append(&[vec![]], None);
                return;
            }
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
            self.voted_for = Some(u128::from_le_bytes(metadata[17..33].try_into().unwrap()));
        }

        let checksum = u32::from_le_bytes(metadata[41..45].try_into().unwrap());
        if checksum != crc32c(&metadata[0..41]) {
            panic!("Bad checksum for data file.");
        }

        let log_length = u64::from_le_bytes(metadata[33..41].try_into().unwrap()) as usize;
        self.log = Vec::with_capacity(log_length);

        let reader = &mut BufReader::new(&self.file);
        while self.log.len() < log_length {
            if let Some(e) = LogEntry::decode(reader) {
                self.log.push(e);
            } else {
                panic!("Could not read log from data file.");
            }
        }
    }

    // Durably add logs to disk.
    fn append(
        &mut self,
        commands: &[Vec<u8>],
        result_sender: Option<mpsc::Sender<ApplyResult>>,
    ) {
        let mut buffer: [u8; PAGESIZE as usize] = [0; PAGESIZE as usize];

        // Write out all new logs.
        for command in commands.iter() {
            let entry = LogEntry {
                term: self.current_term,
                // TODO: Do we need this clone here?
                command: command.to_vec(),

                result_sender: result_sender.clone(),
            };

            entry.encode(&mut buffer, &self.file);

            self.log.push(entry);
        }

        // Write log length metadata.
        self.update(self.current_term, self.voted_for);
    }

    // Durably save non-log data.
    fn update(&mut self, term: u64, voted_for: Option<u128>) {
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
            metadata[17..33].copy_from_slice(&v.to_le_bytes());
        } else {
            metadata[16] = 0;
        }

        let log_length = self.log.len() as u64;
        metadata[33..41].copy_from_slice(&log_length.to_le_bytes());

        let checksum = crc32c(&metadata[0..41]);
        metadata[41..45].copy_from_slice(&checksum.to_le_bytes());

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
    last_valid_message: Instant,

    // Leader-only state.
    next_index: Vec<usize>,
    match_index: Vec<usize>,

    // Candidate-only state.
    votes: usize,
}

impl VolatileState {
    fn new(cluster_size: usize) -> VolatileState {
        VolatileState {
            condition: Condition::Follower,
            commit_index: 0,
            last_applied: 0,
            next_index: vec![0; cluster_size],
            match_index: vec![0; cluster_size],
            votes: 0,
            last_valid_message: Instant::now(),
        }
    }

    fn reset(&mut self) {
        let count = self.next_index.len();
        for i in 0..count {
            self.next_index[i] = 0;
            self.match_index[i] = 0;
        }
        self.votes = 0;
    }
}

struct State {
    stopped: bool,
    durable: DurableState,
    volatile: VolatileState,
}

//             REQUEST WIRE PROTOCOL
//
// | Byte Range | Value                                         |
// |------------|-----------------------------------------------|
// |  0 - 16    | Sender Id                                     |
// | 16         | Request Type                                  |
// | 17 - 25    | Term                                          |
// | 25 - 41    | Leader Id / Candidate Id                      |
// | 41 - 49    | Prev Log Index / Last Log Index               |
// | 49 - 57    | Prev Log Term / Last Log Term                 |
// | 57 - 65    | (Request Vote) Checksum / Leader Commit Index |
// | 65 - 73    | Entries Length                                |
// | 73 - 77    | (Append Entries) Checksum                     |
// | 77 - EOM   | Entries                                       |
//
//             ENTRIES WIRE PROTOCOL
//
// See: ON DISK LOG ENTRY FORMAT.
//
//             RESPONSE WIRE PROTOCOL
//
// | Byte Range   | Value                  |
// |--------------|------------------------|
// |  0 - 16      | Sender Id              |
// |  16          | Response Type          |
// |  17 - 25     | Term                   |
// |  25          | Success / Vote Granted |
// |  26 - 30     | Checksum               |

struct RPCMessageEncoder<T: std::io::Write> {
    sender_id: u128,
    writer: BufWriter<T>,
    written: Vec<u8>,
}

impl<T: std::io::Write> RPCMessageEncoder<T> {
    fn new(sender_id: u128, writer: BufWriter<T>) -> RPCMessageEncoder<T> {
        RPCMessageEncoder {
            writer,
            written: vec![],
            sender_id,
        }
    }

    fn metadata(&mut self, kind: u8, term: u64) {
        assert_eq!(self.written.len(), 0);

        self.written
            .extend_from_slice(&self.sender_id.to_le_bytes());

        self.written.push(kind);

        self.written.extend_from_slice(&term.to_le_bytes());

        self.writer.write_all(&self.written).unwrap();
    }

    fn data(&mut self, data: &[u8]) {
        let offset = self.written.len();
        self.written.extend_from_slice(data);
        self.writer.write_all(&self.written[offset..]).unwrap();
    }

    fn done(&mut self) {
        let checksum = crc32c(&self.written);

        self.writer.write_all(&checksum.to_le_bytes()).unwrap();
        self.writer.flush().unwrap();
    }
}

#[derive(Debug, PartialEq)]
struct RequestVoteRequest {
    term: u64,
    candidate_id: u128,
    last_log_index: usize,
    last_log_term: u64,
}

impl RequestVoteRequest {
    fn decode<T: std::io::Read>(
        mut reader: BufReader<T>,
        metadata: [u8; 25],
        term: u64,
    ) -> Option<RPCBody> {
        let mut buffer: [u8; 61] = [0; 61];
        buffer[0..metadata.len()].copy_from_slice(&metadata);
        reader.read_exact(&mut buffer[metadata.len()..]).unwrap();

        let checksum = u32::from_le_bytes(buffer[57..61].try_into().unwrap());
        if checksum != crc32c(&buffer[0..57]) {
            return None;
        }

        let candidate_id = u128::from_le_bytes(buffer[25..41].try_into().unwrap());
        let last_log_index = u64::from_le_bytes(buffer[41..49].try_into().unwrap());
        let last_log_term = u64::from_le_bytes(buffer[49..57].try_into().unwrap());

        Some(RPCBody::RequestVoteRequest(RequestVoteRequest {
            term,
            candidate_id,
            last_log_index: last_log_index as usize,
            last_log_term,
        }))
    }

    fn encode<T: std::io::Write>(&self, encoder: &mut RPCMessageEncoder<T>) {
        encoder.metadata(RPCBodyKind::RequestVoteRequest as u8, self.term);
        encoder.data(&self.candidate_id.to_le_bytes());
        encoder.data(&(self.last_log_index as u64).to_le_bytes());
        encoder.data(&self.last_log_term.to_le_bytes());
        encoder.done();
    }
}

#[derive(Debug, PartialEq)]
struct RequestVoteResponse {
    term: u64,
    vote_granted: bool,
}

impl RequestVoteResponse {
    fn decode<T: std::io::Read>(
        mut reader: BufReader<T>,
        metadata: [u8; 25],
        term: u64,
    ) -> Option<RPCBody> {
        let mut buffer: [u8; 30] = [0; 30];
        buffer[0..metadata.len()].copy_from_slice(&metadata);
        reader.read_exact(&mut buffer[metadata.len()..]).unwrap();

        let checksum = u32::from_le_bytes(buffer[26..30].try_into().unwrap());
        if checksum != crc32c(&buffer[0..26]) {
            return None;
        }

        Some(RPCBody::RequestVoteResponse(RequestVoteResponse {
            term,
            vote_granted: buffer[25] == 1,
        }))
    }

    fn encode<T: std::io::Write>(&self, encoder: &mut RPCMessageEncoder<T>) {
        encoder.metadata(RPCBodyKind::RequestVoteResponse as u8, self.term);
        encoder.data(&[self.vote_granted as u8]);
        encoder.done();
    }
}

#[derive(Debug, PartialEq)]
struct AppendEntriesRequest {
    term: u64,
    leader_id: u128,
    prev_log_index: usize,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: usize,
}

impl AppendEntriesRequest {
    fn decode<T: std::io::Read>(
        mut reader: BufReader<T>,
        metadata: [u8; 25],
        term: u64,
    ) -> Option<RPCBody> {
        let mut buffer: [u8; 77] = [0; 77];
        buffer[0..metadata.len()].copy_from_slice(&metadata);
        reader.read_exact(&mut buffer[metadata.len()..]).unwrap();

        let checksum = u32::from_le_bytes(buffer[73..77].try_into().unwrap());
        if checksum != crc32c(&buffer[0..73]) {
            return None;
        }

        let leader_id = u128::from_le_bytes(buffer[25..41].try_into().unwrap());
        let prev_log_index = u64::from_le_bytes(buffer[41..49].try_into().unwrap());
        let prev_log_term = u64::from_le_bytes(buffer[49..57].try_into().unwrap());
        let leader_commit = u64::from_le_bytes(buffer[57..65].try_into().unwrap());
        let entries_length = u64::from_le_bytes(buffer[65..73].try_into().unwrap()) as usize;
        let mut entries = Vec::<LogEntry>::with_capacity(entries_length);

        while entries.len() < entries_length {
            if let Some(e) = LogEntry::decode(&mut reader) {
                entries.push(e);
            } else {
                return None;
            }
        }

        Some(RPCBody::AppendEntriesRequest(AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index: prev_log_index as usize,
            prev_log_term,
            leader_commit: leader_commit as usize,
            entries,
        }))
    }

    fn encode<T: std::io::Write>(&self, encoder: &mut RPCMessageEncoder<T>) {
        encoder.metadata(RPCBodyKind::AppendEntriesRequest as u8, self.term);
        encoder.data(&self.leader_id.to_le_bytes());
        encoder.data(&(self.prev_log_index as u64).to_le_bytes());
        encoder.data(&self.prev_log_term.to_le_bytes());
        encoder.data(&(self.leader_commit as u64).to_le_bytes());
        encoder.data(&(self.entries.len() as u64).to_le_bytes());
        encoder.done();

        let mut buffer: [u8; PAGESIZE as usize] = [0; PAGESIZE as usize];
        for entry in self.entries.iter() {
            entry.encode(&mut buffer, &mut encoder.writer);
        }
    }
}

#[derive(Debug, PartialEq)]
struct AppendEntriesResponse {
    term: u64,
    success: bool,
}

impl AppendEntriesResponse {
    fn decode<T: std::io::Read>(
        mut reader: BufReader<T>,
        metadata: [u8; 25],
        term: u64,
    ) -> Option<RPCBody> {
        let mut buffer: [u8; 30] = [0; 30];
        buffer[0..metadata.len()].copy_from_slice(&metadata);
        reader.read_exact(&mut buffer[metadata.len()..]).unwrap();

        let checksum = u32::from_le_bytes(buffer[26..30].try_into().unwrap());
        if checksum != crc32c(&buffer[0..26]) {
            return None;
        }

        Some(RPCBody::AppendEntriesResponse(AppendEntriesResponse {
            term,
            success: buffer[25] == 1,
        }))
    }

    fn encode<T: std::io::Write>(&self, encoder: &mut RPCMessageEncoder<T>) {
        encoder.metadata(RPCBodyKind::AppendEntriesResponse as u8, self.term);
        encoder.data(&[self.success as u8]);
        encoder.done();
    }
}

enum RPCBodyKind {
    RequestVoteRequest = 0,
    RequestVoteResponse = 1,
    AppendEntriesRequest = 2,
    AppendEntriesResponse = 3,
}

#[derive(Debug, PartialEq)]
enum RPCBody {
    RequestVoteRequest(RequestVoteRequest),
    RequestVoteResponse(RequestVoteResponse),
    AppendEntriesRequest(AppendEntriesRequest),
    AppendEntriesResponse(AppendEntriesResponse),
}

#[derive(Debug, PartialEq)]
struct RPCMessage {
    from: u128,
    term: u64,
    body: RPCBody,
}

impl RPCMessage {
    fn new(term: u64, body: RPCBody) -> RPCMessage {
        RPCMessage {
            from: 0,
            term,
            body,
        }
    }

    fn decode<T: std::io::Read>(mut reader: BufReader<T>) -> Option<RPCMessage> {
        let mut metadata: [u8; 25] = [0; 25];
        if let Err(e) = reader.read_exact(&mut metadata) {
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut
            {
                return None;
            }

            panic!("Could not read request: {:#?}.", e);
        }

        let server_id = u128::from_le_bytes(metadata[0..16].try_into().unwrap());

        let message_type = metadata[16];
        let term = u64::from_le_bytes(metadata[17..25].try_into().unwrap());
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

        Some(RPCMessage {
            from: server_id,
            term,
            body: body?,
        })
    }

    fn encode<T: std::io::Write>(&self, sender_id: u128, writer: BufWriter<T>) {
        let encoder = &mut RPCMessageEncoder::new(sender_id, writer);
        match &self.body {
            RPCBody::RequestVoteRequest(rvr) => rvr.encode(encoder),
            RPCBody::RequestVoteResponse(rvr) => rvr.encode(encoder),
            RPCBody::AppendEntriesRequest(aer) => aer.encode(encoder),
            RPCBody::AppendEntriesResponse(aer) => aer.encode(encoder),
        };
    }
}

struct RPCManager {
    cluster: Vec<ServerConfig>,
    server_id: u128,
    stream_sender: mpsc::Sender<RPCMessage>,
    stream_receiver: mpsc::Receiver<RPCMessage>,
    stop_mutex: Arc<Mutex<bool>>,
}

impl RPCManager {
    fn new(server_id: u128, cluster: Vec<ServerConfig>) -> RPCManager {
        let (stream_sender, stream_receiver): (
            mpsc::Sender<RPCMessage>,
            mpsc::Receiver<RPCMessage>,
        ) = mpsc::channel();
        RPCManager {
            cluster,
            server_id,
            stream_sender,
            stream_receiver,
            stop_mutex: Arc::new(Mutex::new(false)),
        }
    }

    fn address_from_id(&self, id: u128) -> SocketAddr {
        for server in self.cluster.iter() {
            if server.id == id {
                return server.address;
            }
        }

        panic!("Bad Server Id for configuration.")
    }

    fn start(&mut self) {
        let address = self.address_from_id(self.server_id);
        let listener =
            std::net::TcpListener::bind(address).expect("Could not bind to configured address.");

        let thread_stop = self.stop_mutex.clone();
        let thread_stream_sender = self.stream_sender.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let stop = thread_stop.lock().unwrap();
                if *stop {
                    break;
                }

                let bufreader = BufReader::new(stream);
                if let Some(msg) = RPCMessage::decode(bufreader) {
                    thread_stream_sender.send(msg).unwrap();
                }
            }
        });
    }

    fn send(&mut self, to_server_id: u128, message: &RPCMessage) {
        let address = self.address_from_id(to_server_id);
        let thread_stream_sender = self.stream_sender.clone();
        let server_id = self.server_id;

        let stream = std::net::TcpStream::connect(address).unwrap();
        let bufwriter = BufWriter::new(stream.try_clone().unwrap());
        message.encode(server_id, bufwriter);

        std::thread::spawn(move || {
            let bufreader = BufReader::new(stream);
            if let Some(response) = RPCMessage::decode(bufreader) {
                thread_stream_sender.send(response).unwrap();
            }
        });
    }
}

#[derive(Copy, Clone)]
pub struct ServerConfig {
    id: u128,
    address: SocketAddr,
}

pub struct Config {
    // Cluster configuration.
    server_index: usize,
    server_id: u128,
    cluster: Vec<ServerConfig>,

    // Timing configuration.
    heartbeat_frequency: Duration,
    heartbeat_timeout: Duration,
}

pub struct Server<SM: StateMachine> {
    config: Config,

    sm: SM,
    rpc_manager: RPCManager,

    state: Mutex<State>,
}

impl<SM: StateMachine> Server<SM> {
    pub fn apply(&mut self, commands: Vec<Vec<u8>>) -> mpsc::Receiver<ApplyResult> {
        let (result_sender, result_receiver): (
            mpsc::Sender<ApplyResult>,
            mpsc::Receiver<ApplyResult>,
        ) = mpsc::channel();

        // Append commands to local durable state if leader.
        let mut state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Leader {
            result_sender.send(ApplyResult::NotALeader).unwrap();
            return result_receiver;
        }

        state.durable.append(&commands, Some(result_sender));
        result_receiver
    }

    fn handle_request_vote_request(
        &mut self,
        request: RequestVoteRequest,
    ) -> (u64, Option<RPCBody>) {
        let mut state = self.state.lock().unwrap();
        let term = state.durable.current_term;
        let false_request = RPCBody::RequestVoteResponse(RequestVoteResponse {
            term,
            vote_granted: false,
        });

        if request.term < term {
            return (term, Some(false_request));
        }

        // Reset the heartbeat timer.
        state.volatile.last_valid_message = Instant::now();

        let canvote = if let Some(id) = state.durable.voted_for {
            id == request.candidate_id
        } else {
            true
        };
        if !canvote {
            return (term, Some(false_request));
        }

        // "2. If votedFor is null or candidateId, and candidate’s log
        // is at least as up-to-date as receiver’s log, grant vote
        // (§5.2, §5.4)." - Reference [0] Page 4
        //
        // "Raft determines which of two logs is more up-to-date
        // by comparing the index and term of the last entries in the
        // logs. If the logs have last entries with different terms, then
        // the log with the later term is more up-to-date. If the logs
        // end with the same term, then whichever log is longer is
        // more up-to-date." - Reference [0] Page 8

        // TODO: Fill in.
        let candidate_more_up_to_date = false;

        if candidate_more_up_to_date {
            state.durable.update(term, Some(request.candidate_id));

            // Reset heartbeat timer.
            state.volatile.last_valid_message = Instant::now();
        }

        let msg = RPCBody::RequestVoteResponse(RequestVoteResponse {
            term,
            vote_granted: candidate_more_up_to_date,
        });
        (term, Some(msg))
    }

    fn handle_request_vote_response(
        &mut self,
        response: RequestVoteResponse,
    ) -> (u64, Option<RPCBody>) {
        let quorum = self.config.cluster.len() / 2;
        assert!(quorum > 0 || (self.config.cluster.len() == 1 && quorum == 0));

        if response.vote_granted {
            let mut state = self.state.lock().unwrap();
            state.volatile.votes += 1;
            // This will not handle the case where a single
            // server-cluster needs to become the leader.
            if state.volatile.votes == quorum {
                drop(state);
                self.candidate_become_leader();
            }
        }

        (0, None)
    }

    fn handle_append_entries_request(
        &mut self,
        request: AppendEntriesRequest,
    ) -> (u64, Option<RPCBody>) {
        let mut state = self.state.lock().unwrap();
        let term = state.durable.current_term;
        if request.term < term {
            return (
                term,
                Some(RPCBody::AppendEntriesResponse(AppendEntriesResponse {
                    term,
                    success: false,
                })),
            );
        }

        // Reset heartbeat timer.
        state.volatile.last_valid_message = Instant::now();

        // TODO: fill in.

        (
            term,
            Some(RPCBody::AppendEntriesResponse(AppendEntriesResponse {
                term,
                success: true,
            })),
        )
    }

    fn handle_append_entries_response(
        &mut self,
        _response: AppendEntriesResponse,
    ) -> (u64, Option<RPCBody>) {
        // TODO: fill in.
        (0, None)
    }

    fn rpc_handle_message(&mut self, message: RPCMessage) {
        let mut state = self.state.lock().unwrap();
        if message.term > state.durable.current_term {
            let voted_for = state.durable.voted_for;
            state.durable.update(message.term, voted_for);
            state.volatile.condition = Condition::Follower;
        }
        drop(state);

        let (term, rsp) = match message.body {
            RPCBody::RequestVoteRequest(r) => self.handle_request_vote_request(r),
            RPCBody::RequestVoteResponse(r) => self.handle_request_vote_response(r),
            RPCBody::AppendEntriesRequest(r) => self.handle_append_entries_request(r),
            RPCBody::AppendEntriesResponse(r) => self.handle_append_entries_response(r),
        };
        if let Some(body) = rsp {
            let msg = &RPCMessage::new(term, body);
            self.rpc_manager.send(message.from, msg);
        }
    }

    fn leader_maybe_new_quorum(&mut self) {
        // "If there exists an N such that N > commitIndex, a majority
        // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // set commitIndex = N (§5.3, §5.4)." - Reference [0] Page 4
        let quorum_needed = self.config.cluster.len() / 2;
        let mut state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Leader {
            return;
        }

        let log_length = state.durable.log.len();

        'outer: for i in (0..log_length).rev() {
            let mut quorum = quorum_needed;

            if i <= state.volatile.commit_index && self.config.cluster.len() > 1 {
                break;
            }

            assert!(quorum > 0 || (self.config.cluster.len() == 1 && quorum == 0));
            for (server_index, &match_index) in state.volatile.match_index.iter().enumerate() {
                // Here so that in the case of there being a single
                // server in the cluster, we still (trivially) achieve
                // new quorums.
                if quorum == 0 {
                    state.volatile.commit_index = i;
                    break 'outer;
                }

                // self always counts as part of quorum, so skip it in
                // the count. quorum_needed already takes self into
                // consideration (`len() / 2` not `len() / 2 + 1`).
                if self.config.server_index == server_index {
                    continue;
                }

                if match_index >= i && state.durable.log[i].term == state.durable.current_term {
                    quorum -= 1;
                }
            }
        }
    }

    fn leader_send_heartbeat(&mut self) {
        // "Upon election: send initial empty AppendEntries RPCs
        // (heartbeat) to each server; repeat during idle periods to
        // prevent election timeouts (§5.2)." - Reference [0] Page 4
        let state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Leader {
            return;
        }

        _ = 1; // Make clippy happy.
    }

    fn follower_maybe_become_candidate(&mut self) {
        // "If election timeout elapses without receiving AppendEntries
        // RPC from current leader or granting vote to candidate:
        // convert to candidate." - Reference [0] Page 4
        let state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Follower {
            return;
        }

        if state.volatile.last_valid_message.elapsed() > self.config.heartbeat_timeout {
            drop(state);
            self.candidate_request_votes();
        }
    }

    fn candidate_become_leader(&mut self) {
        let mut state = self.state.lock().unwrap();
        state.volatile.reset();
        state.volatile.condition = Condition::Leader;
        drop(state);
        self.leader_send_heartbeat();
    }

    fn candidate_request_votes(&mut self) {
        let mut state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Candidate {
            return;
        }

        let term = state.durable.current_term + 1;
        let my_server_id = self.config.server_id;
        state.durable.update(term, Some(my_server_id));

        // Trivial case. In a single-server cluster, the server is the
        // leader.
        if self.config.cluster.len() == 1 {
            drop(state);
            self.candidate_become_leader();
            return;
        }

        let msg = &RPCMessage::new(
            term,
            RPCBody::RequestVoteRequest(RequestVoteRequest {
                term,
                candidate_id: self.config.server_id,
                last_log_index: state.durable.log.len() - 1,
                last_log_term: state.durable.log[state.durable.log.len() - 1].term,
            }),
        );
        for server in self.config.cluster.iter() {
            // Skip self.
            if server.id == my_server_id {
                continue;
            }

            self.rpc_manager.send(server.id, msg);
        }
    }

    fn apply_entries(&mut self) {
        let mut state = self.state.lock().unwrap();
        let mut to_apply = Vec::<Vec<u8>>::new();
        let starting_index = state.volatile.last_applied + 1;
        while state.volatile.last_applied < state.volatile.commit_index {
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
                if let Some(sender) = &state.durable.log[starting_index + i].result_sender {
                    sender.send(ApplyResult::Ok(result)).unwrap();
                }
            }
        }
    }

    pub fn init(&mut self) {
        self.restore();

        self.rpc_manager.start();

        if self.config.cluster.len() == 1 {
            let mut state = self.state.lock().unwrap();
            state.volatile.condition = Condition::Leader;
        }
    }

    pub fn stop(&mut self) {
        let mut state = self.state.lock().unwrap();
        // Prevent server from accepting any more log entries.
        state.volatile.condition = Condition::Follower;
        state.stopped = true;

        // Stop RPCManager.
        let mut stop = self.rpc_manager.stop_mutex.lock().unwrap();
        *stop = true;
    }

    pub fn tick(&mut self) {
        let state = self.state.lock().unwrap();
        if state.stopped {
            return;
        }
        drop(state);

        // Leader operations.
        self.leader_send_heartbeat();
        self.leader_maybe_new_quorum();

        // Follower operations.
        self.follower_maybe_become_candidate();

        // All condition operations.
        self.apply_entries();

        if let Ok(msg) = self.rpc_manager.stream_receiver.try_recv() {
            self.rpc_handle_message(msg);
        }
    }

    pub fn restore(&self) {
        let mut state = self.state.lock().unwrap();
        state.durable.restore();
    }

    pub fn new(data_directory: &std::path::Path, sm: SM, config: Config) -> Server<SM> {
        let cluster_size = config.cluster.len();
        let id = config.server_id;
        let rpc_manager = RPCManager::new(config.server_id, config.cluster.clone());
        Server {
            rpc_manager,
            config,
            sm,

            state: Mutex::new(State {
                durable: DurableState::new(data_directory, id),
                volatile: VolatileState::new(cluster_size),
                stopped: false,
            }),
        }
    }
}

// CRC32C is a port of (what seems to be) FreeBSD's public-domain
// implementation:
// https://web.mit.edu/freebsd/head/sys/libkern/crc32.c.

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
        self.result ^ !0
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
    fn test_update_and_restore() {
        let tmp = TmpDir::new();

        let mut durable = DurableState::new(&tmp.dir, 1);
        durable.restore();
        assert_eq!(durable.current_term, 0);
        assert_eq!(durable.voted_for, None);
        assert_eq!(durable.log.len(), 1);
        assert_eq!(durable.log[0].term, 0);
        assert_eq!(durable.log[0].command, vec![]);

        durable.update(10234, Some(40592));
        assert_eq!(durable.current_term, 10234);
        assert_eq!(durable.voted_for, Some(40592));
        assert_eq!(durable.log.len(), 1);
        assert_eq!(durable.log[0].term, 0);
        assert_eq!(durable.log[0].command, vec![]);
        drop(durable);

        let mut durable = DurableState::new(&tmp.dir, 1);
        assert_eq!(durable.current_term, 0);
        assert_eq!(durable.voted_for, None);
        assert_eq!(durable.log.len(), 0);

        durable.restore();
        assert_eq!(durable.current_term, 10234);
        assert_eq!(durable.voted_for, Some(40592));
        assert_eq!(durable.log.len(), 1);
        assert_eq!(durable.log[0].term, 0);
        assert_eq!(durable.log[0].command, vec![]);
    }

    #[test]
    fn test_log_append() {
        let tmp = TmpDir::new();

        let mut v = Vec::<Vec<u8>>::new();
        v.push("abcdef123456".as_bytes().to_vec());
        v.push("foobar".as_bytes().to_vec());

        // Write two entries and shut down.
        let mut durable = DurableState::new(&tmp.dir, 1);
        durable.restore();
        durable.append(&v, None);
        drop(durable);

        // Start up and restore. Should have two entries.
        let mut durable = DurableState::new(&tmp.dir, 1);
        durable.restore();
        assert_eq!(durable.log.len(), 3);
        assert_eq!(durable.log[1].term, 0);
        assert_eq!(durable.log[1].command, b"abcdef123456"[0..]);
        assert_eq!(durable.log[2].term, 0);
        assert_eq!(durable.log[2].command, b"foobar"[0..]);

        // Add in double the existing entries and shut down.
        v.reverse();
        let longcommand = "a".repeat(10_000);
        let longcommand2 = "a".repeat(PAGESIZE as usize);
        let longcommand3 = "a".repeat(1 + PAGESIZE as usize);
        v.push(longcommand.as_bytes().to_vec());
        v.push(longcommand2.as_bytes().to_vec());
        v.push(longcommand3.as_bytes().to_vec());
        durable.append(&v, None);
        drop(durable);

        // Start up and restore. Should now have four entries.
        let mut durable = DurableState::new(&tmp.dir, 1);
        durable.restore();
        assert_eq!(durable.log.len(), 8);
        assert_eq!(durable.log[1].term, 0);
        assert_eq!(durable.log[1].command, b"abcdef123456"[0..]);
        assert_eq!(durable.log[2].term, 0);
        assert_eq!(durable.log[2].command, b"foobar"[0..]);
        assert_eq!(durable.log[3].term, 0);
        assert_eq!(durable.log[3].command, b"foobar"[0..]);
        assert_eq!(durable.log[4].term, 0);
        assert_eq!(durable.log[4].command, b"abcdef123456"[0..]);
        assert_eq!(durable.log[5].term, 0);
        assert_eq!(durable.log[5].command, longcommand.as_bytes());
        assert_eq!(durable.log[6].term, 0);
        assert_eq!(durable.log[6].command, longcommand2.as_bytes());
        assert_eq!(durable.log[7].term, 0);
        assert_eq!(durable.log[7].command, longcommand3.as_bytes());
    }

    #[test]
    fn test_rpc_message_encode_decode() {
        let tests = vec![
            RPCMessage::new(
                88,
                RPCBody::AppendEntriesRequest(AppendEntriesRequest {
                    term: 88,
                    leader_id: 2132,
                    prev_log_index: 1823,
                    prev_log_term: 193,
                    entries: vec![
                        LogEntry {
                            term: 88,
                            command: "hey there".into(),
                            result_sender: None,
                        },
                        LogEntry {
                            term: 90,
                            command: "blub".into(),
                            result_sender: None,
                        },
                    ],
                    leader_commit: 95,
                }),
            ),
            RPCMessage::new(
                91,
                RPCBody::AppendEntriesRequest(AppendEntriesRequest {
                    term: 91,
                    leader_id: 2132,
                    prev_log_index: 1823,
                    prev_log_term: 193,
                    entries: vec![],
                    leader_commit: 95,
                }),
            ),
            RPCMessage::new(
                10,
                RPCBody::AppendEntriesResponse(AppendEntriesResponse {
                    term: 10,
                    success: true,
                }),
            ),
            RPCMessage::new(
                10,
                RPCBody::AppendEntriesResponse(AppendEntriesResponse {
                    term: 10,
                    success: false,
                }),
            ),
            RPCMessage::new(
                1023,
                RPCBody::RequestVoteRequest(RequestVoteRequest {
                    term: 1023,
                    candidate_id: 2132,
                    last_log_index: 1823,
                    last_log_term: 193,
                }),
            ),
            RPCMessage::new(
                1023,
                RPCBody::RequestVoteResponse(RequestVoteResponse {
                    term: 1023,
                    vote_granted: true,
                }),
            ),
            RPCMessage::new(
                1023,
                RPCBody::RequestVoteResponse(RequestVoteResponse {
                    term: 1023,
                    vote_granted: false,
                }),
            ),
        ];

        for rpcmessage in tests.into_iter() {
            let mut file = Vec::new();
            let mut cursor = std::io::Cursor::new(&mut file);
            let bufwriter = BufWriter::new(&mut cursor);
            rpcmessage.encode(0, bufwriter);

            drop(cursor);

            let mut cursor = std::io::Cursor::new(&mut file);
            let bufreader = BufReader::new(&mut cursor);
            let result = RPCMessage::decode(bufreader);
            assert_eq!(result, Some(rpcmessage));
        }
    }

    #[test]
    fn test_rpc_manager() {
        let config = vec![ServerConfig {
            id: 0,
            address: "127.0.0.1:20001".parse().unwrap(),
        }];
        let mut rpcm = RPCManager::new(0, config);
        rpcm.start();

        let msg = RPCMessage::new(
            1023,
            RPCBody::RequestVoteRequest(RequestVoteRequest {
                term: 1023,
                candidate_id: 2132,
                last_log_index: 1823,
                last_log_term: 193,
            }),
        );
        rpcm.send(0, &msg);
        let received = rpcm.stream_receiver.recv().unwrap();
        assert_eq!(msg, received);
        let mut stop = rpcm.stop_mutex.lock().unwrap();
        *stop = true;
    }

    struct TestStateMachine {}

    impl StateMachine for TestStateMachine {
        fn apply(&self, messages: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
            return messages;
        }
    }

    #[test]
    fn test_single_server_apply_end_to_end() {
        let tmp = TmpDir::new();
        let config = Config {
            server_id: 0,
            server_index: 0,
            cluster: vec![ServerConfig {
                id: 0,
                address: "127.0.0.1:20002".parse().unwrap(),
            }],

            heartbeat_frequency: Duration::from_secs(5),
            heartbeat_timeout: Duration::from_secs(10),
        };

        let sm = TestStateMachine {};
        let mut server = Server::new(&tmp.dir, sm, config);

        // First test apply doesn't work as not a leader.
        let result_receiver = server.apply(vec![vec![]]);
        assert_eq!(result_receiver.recv().unwrap(), ApplyResult::NotALeader);

        // Now after initializing (realizing there's only one server, so is leader), try again.
        server.init();

        let result_receiver = server.apply(vec!["abc".as_bytes().to_vec()]);
        server.tick();
        let result = result_receiver.try_recv().unwrap();
        assert_eq!(result, ApplyResult::Ok("abc".as_bytes().to_vec()));

        // Clean up.
        server.stop();
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
