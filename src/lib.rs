use std::io::Read;
use std::io::Seek;
use std::convert::TryInto;
use std::os::unix::prelude::FileExt;

pub type Command = Vec<u8>;

pub enum ApplyResult {
    NotALeader,
    Ok(Vec<Command>)
}

pub trait StateMachine {
    fn apply(self, messages: Vec<Command>) -> ApplyResult;
}

pub struct Config {
    id: u32,
    address: std::net::IpAddr,
}

struct LogEntry {
    command: Command,
    term: u64,
}

struct DurableState {
    // Backing file.
    file: std::fs::File,

    // Actual data.
    current_term: u64,
    voted_for: Option<u32>,
    log: Vec<LogEntry>,
}

impl DurableState {
    fn new(data_directory: &str, id: u32) -> DurableState {
	let file = std::fs::File::options()
	    .create(true)
	    .read(true)
	    .write(true)
	    .open(format!("{}/server_{}.data", data_directory, id))
	    .expect("Could not open data file.");
	return DurableState{
	    file: file,
	    current_term: 0,
	    voted_for: None,
	    log: Vec::<LogEntry>::new(),
	};
    }
    
    //        ON DISK FORMAT
    //
    // | Byte Range | Value       |
    // |------------|-------------|
    // | 0 - 8      | Term        |
    // | 8 - 9      | Did Vote    |
    // | 9 - 13     | Voted For   |
    // | 13 - 21    | Log Length  |
    // | 4096 - EOF | Log Entries |
    //
    //           ON DISK LOG ENTRY FORMAT
    //
    // | Byte Range                  | Value          |
    // |-----------------------------|----------------|
    // | 0 - 8                       | Term           |
    // | 8 - 16                      | Command Length |
    // | 16 - (16 + $Command Length) | Command        |
    //
    // After Command, the file will be padding until the next boundary
    // divisible by 4k.

    fn restore(&mut self) {
	if let Ok(m) = self.file.metadata() {
	    if m.len() == 0 {
		return;
	    }
	} else {
	    return;
	}
	
	let mut metadata: [u8; 4096] = [0; 4096];
	self.file.read_exact_at(&mut metadata[0..], 0).expect("Could not read server metadata.");

	self.current_term = u64::from_le_bytes(metadata[0..8].try_into().unwrap());
	let did_vote = metadata[8] == 1;
	if did_vote {
	    self.voted_for = Some(u32::from_le_bytes(metadata[9..13].try_into().unwrap()));
	}

	let log_length = u64::from_le_bytes(metadata[13..21].try_into().unwrap()) as usize;
	self.log = Vec::with_capacity(log_length);
	if log_length == 0 {
	    return;
	}

	self.file.seek(std::io::SeekFrom::Start(4096)).unwrap();
	let mut reader = std::io::BufReader::new(&self.file);
	while self.log.len() < log_length {
	    let mut log_entry = LogEntry{
		term: 0,
		command: Vec::new(),
	    };

	    let mut metadata: [u8; 16] = [0; 16];
	    reader.read_exact(&mut metadata[0..]).unwrap();
	    log_entry.term = u64::from_le_bytes(metadata[0..8].try_into().unwrap());
	    let command_length = u64::from_le_bytes(metadata[8..16].try_into().unwrap()) as usize;
	    log_entry.command.resize(command_length, b'0');

	    reader.read_exact(&mut log_entry.command[0..]).unwrap();
	    
	    self.log.push(log_entry);
	}
    }

    // Durably add logs to disk.
   fn append(&mut self, term: u64, commands: Vec<Command>) {
       let n = commands.len();

       // Write out all logs.
       for i in 0..n {
	   self.log.push(LogEntry{
	       term: term,
	       // TODO: Do we need this clone here?
	       command: commands[i].clone(),
	   });
       }

       // Write log length metadata.
       self.update(self.current_term, self.voted_for);
    }

    // Durably save non-log data.
    fn update(&mut self, term: u64, voted_for: Option<u32>) {
	self.current_term = term;
	self.voted_for = voted_for;

	let mut metadata: [u8; 4096] = [0; 4096];
	metadata[0..8].copy_from_slice(&term.to_le_bytes());

	if let Some(v) = voted_for {
	    metadata[8] = 1;
	    metadata[9..13].copy_from_slice(&v.to_le_bytes());
	} else {
	    metadata[8] = 0;
	}

	let log_length = self.log.len() as u64;
	metadata[13..21].copy_from_slice(&log_length.to_le_bytes());

	self.file.write_all_at(&metadata, 0).unwrap();

	// fsync.
	self.file.sync_all().unwrap();
    }
}

enum Condition {
    Leader,
    Follower,
    Candidate,
}

struct VolatileState {
    condition: Condition,
    term: u64,

    commit_index: usize,
    last_applied: usize,

    // Leader-only state.
    next_index: Vec<usize>,
    match_index: Vec<usize>,
}

impl VolatileState {
    fn new(cluster_size: usize) -> VolatileState {
	return VolatileState{
	    term: 0,
	    condition: Condition::Follower,
	    commit_index: 0,
	    last_applied: 0,
	    next_index: Vec::with_capacity(cluster_size),
	    match_index: Vec::with_capacity(cluster_size),
	};
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

    durable_state: DurableState,
    volatile_state: VolatileState,
}

pub struct Server<SM: StateMachine + std::marker::Send> {
    cluster: Vec<Config>,
    sm: SM,

    state: std::sync::Mutex<State>,
}

impl<SM: StateMachine + std::marker::Send> Server<SM> {
    pub fn apply(&mut self, commands: Vec<Command>) -> ApplyResult {
	// Append commands to local durable state if leader.
	let mut state = self.state.lock().unwrap();
	if !matches!(state.volatile_state.condition, Condition::Leader) {
	    return ApplyResult::NotALeader;
	}

	let prev_length = state.durable_state.log.len();
	let to_add = commands.len();
	let term = state.volatile_state.term;
	state.durable_state.append(term, commands);
	drop(state);

	// Wait for messages to be applied. Probably a better way.
	loop {
	    std::thread::sleep(std::time::Duration::from_millis(10));
	    let state = self.state.lock().unwrap();
	    if state.durable_state.log.len() >= prev_length + to_add {
		break;
	    }
	}
	// TODO: Handle taking too long.

	// Return results of messages.
	let state = self.state.lock().unwrap();
	let mut results = Vec::<Command>::with_capacity(to_add);
	for log_entry in state.durable_state.log[prev_length..to_add].iter() {
	    results.push(log_entry.command.clone());
	}
	assert!(results.len() == to_add);
	return ApplyResult::Ok(results);
    }

    fn handle_request(&mut self, connection: std::net::TcpStream) {

    }

    // pub fn start(&mut self) {
    // 	 std::thread::spawn(move || {
    // 	    let listener = std::net::TcpListener::bind("127.0.0.1:80")
    // 		.expect("Could not bind to port.");

    // 	    for stream in listener.incoming() {
    // 		let state = self.state.lock().unwrap();
    // 		if state.tcp_done {
    // 		    break;
    // 		}
    // 		drop(state);

    // 		if let std::io::Result::Ok(s) = stream {
    // 		    self.handle_request(s);
    // 		}
    // 	    }
    // 	});
    // }

    pub fn stop(&mut self) {
	let mut state = self.state.lock().unwrap();
	state.tcp_done = true;
	drop(state);
    }

    pub fn restore(&self) {
	let mut state = self.state.lock().unwrap();
	state.durable_state.restore();
    }

    pub fn new(id: u32, data_directory: &str, sm: SM, cluster: Vec<Config>) -> Server<SM> {
	let cluster_size = cluster.len();
	return Server{
	    cluster: cluster,
	    sm: sm,

	    state: std::sync::Mutex::new(State{
		durable_state: DurableState::new(data_directory, id),
		volatile_state: VolatileState::new(cluster_size),

		tcp_done: false,
	    }),
	};
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init() -> String {
	let dir = "test";
	let _ = std::fs::remove_dir_all(dir); // Ok if this fails.
	std::fs::create_dir_all(dir).unwrap();
	return dir.to_owned();
    }

    #[test]
    fn test_save_and_restore() {
	let dir = init();

	let mut durable_state = DurableState::new(dir.as_str(), 1);
	durable_state.restore();
	assert_eq!(durable_state.current_term, 0);
	assert_eq!(durable_state.voted_for, None);
	assert_eq!(durable_state.log.len(), 0);

	durable_state.update(10234, Some(40592));
	assert_eq!(durable_state.current_term, 10234);
	assert_eq!(durable_state.voted_for, Some(40592));
	assert_eq!(durable_state.log.len(), 0);
	drop(durable_state);

	let mut durable_state = DurableState::new(dir.as_str(), 1);
	assert_eq!(durable_state.current_term, 0);
	assert_eq!(durable_state.voted_for, None);
	assert_eq!(durable_state.log.len(), 0);

	durable_state.restore();
	assert_eq!(durable_state.current_term, 10234);
	assert_eq!(durable_state.voted_for, Some(40592));
	assert_eq!(durable_state.log.len(), 0);
    }
}
