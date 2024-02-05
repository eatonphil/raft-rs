// References:
// [0] In Search of an Understandable Consensus Algorithm (Extended Version) -- https://raft.github.io/raft.pdf

use std::convert::{TryFrom, TryInto};
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::SocketAddr;
use std::os::unix::prelude::FileExt;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

const PAGESIZE: u64 = 512;

#[derive(Debug, PartialEq)]
pub enum ApplyResult {
    NotALeader,
    Ok,
}

pub trait StateMachine {
    fn apply(&self, messages: Vec<Vec<u8>>) -> Vec<Vec<u8>>;
}

struct PageCache {
    // Backing file.
    file: std::fs::File,

    // Page cache. Maps file offset to page.
    page_cache: std::collections::HashMap<u64, [u8; PAGESIZE as usize]>,
    page_cache_size: usize,

    // For buffering actual writes to disk.
    buffer: Vec<u8>,
    buffer_write_at: Option<u64>,
    buffer_write_at_offset: u64,
}

impl PageCache {
    fn new(file: std::fs::File, page_cache_size: usize) -> PageCache {
        let mut page_cache = std::collections::HashMap::new();
        // Allocate the space up front! The page cache should never
        // allocate after this. This is a big deal.
        page_cache.reserve(page_cache_size + 1);

        PageCache {
            file,
            page_cache_size,
            page_cache,

            buffer: vec![],
            buffer_write_at: None,
            buffer_write_at_offset: 0,
        }
    }

    fn insert_or_replace_in_cache(&mut self, offset: u64, page: [u8; PAGESIZE as usize]) {
        if self.page_cache_size == 0 {
            return;
        }

        // If it's already in the cache, just overwrite it.
        if let Some(existing) = self.page_cache.get(&offset) {
            if page != *existing {
                self.page_cache.insert(offset, page);
            }
            return;
        }

        // TODO: Come up with a better cache policy.
        if self.page_cache.len() == self.page_cache_size {
            self.page_cache.clear();
        }

        // Otherwise insert and evict something if we're out of space.
        self.page_cache.insert(offset, page);
    }

    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.page_cache.len()
    }

    fn read(&mut self, offset: u64, buf_into: &mut [u8; PAGESIZE as usize]) {
        // For now, must to read() while a `write()` is ongoing. See
        // the comment in `self.write()`.
        assert_eq!(self.buffer_write_at, None);

        assert_eq!(buf_into.len(), PAGESIZE as usize);
        if let Some(page) = self.page_cache.get(&offset) {
            buf_into.copy_from_slice(page);
            return;
        }

        self.file.read_exact_at(&mut buf_into[0..], offset).unwrap();
        self.insert_or_replace_in_cache(offset, *buf_into);
    }

    fn write(&mut self, offset: u64, page: [u8; PAGESIZE as usize]) {
        if self.buffer_write_at.is_none() {
            self.buffer_write_at = Some(offset);
            self.buffer_write_at_offset = offset;
        } else {
            // Make sure we're always doing sequential writes in
            // between self.flush() call.
            assert_eq!(self.buffer_write_at_offset, offset - PAGESIZE);
            self.buffer_write_at_offset = offset;
        }

        assert_ne!(self.buffer_write_at, None);

        // TODO: It is potentially unsafe if we are doing reads
        // inbetween writes. That isn't possible in the current
        // code. The case to worry about would be `self.write()`
        // before `self.sync()` where the pagecache gets filled up and
        // this particular page isn't in the pagecache and hasn't yet
        // been written to disk. The only correct thing to do would be
        // for `self.read()` to also check `self.buffer` before
        // reading from disk.
        self.buffer.extend(page);

        self.insert_or_replace_in_cache(offset, page);
    }

    fn sync(&mut self) {
        self.file
            .write_all_at(&self.buffer, self.buffer_write_at.unwrap())
            .unwrap();
        self.buffer.clear();
        self.buffer_write_at = None;
        self.buffer_write_at_offset = 0;
        self.file.sync_all().unwrap();
    }
}

#[cfg(test)]
mod pagecache_tests {
    use super::*;

    #[test]
    fn test_pagecache() {
        let tests = [0, 1, 100];
        for cache_size in tests {
            let tmp = server_tests::TmpDir::new();
            let mut filename = tmp.dir.to_path_buf();
            filename.push("test.dat");
            let file = std::fs::File::options()
                .create(true)
                .read(true)
                .write(true)
                .open(filename.clone())
                .expect("Could not open data file.");

            let first_page = [b'a'; PAGESIZE as usize];
            let third_page = [b'c'; PAGESIZE as usize];
            let mut p = PageCache::new(file, cache_size);
            p.write(0, first_page);
            assert!(p.len() <= cache_size);
            if cache_size > 0 {
                assert!(p.len() > 0);
            }
            p.sync();

            p.write(PAGESIZE * 2, third_page);
            assert!(p.len() <= cache_size);
            if cache_size > 0 {
                assert!(p.len() > 0);
            }
            p.sync();

            drop(p);

            let mut file = std::fs::File::options()
                .read(true)
                .open(filename)
                .expect("Could not open data file.");
            let mut all_pages = [0; 3 * PAGESIZE as usize];
            file.read_exact(&mut all_pages).unwrap();

            let second_page = [0; PAGESIZE as usize];
            assert_eq!(all_pages[0..PAGESIZE as usize], first_page);
            assert_eq!(
                all_pages[PAGESIZE as usize..2 * PAGESIZE as usize],
                second_page
            );
            assert_eq!(
                all_pages[2 * PAGESIZE as usize..3 * PAGESIZE as usize],
                third_page
            );

            let mut p = PageCache::new(file, cache_size);
            let mut page = [0; PAGESIZE as usize];
            p.read(0, &mut page);
            assert!(p.len() <= cache_size);
            if cache_size > 0 {
                assert!(p.len() > 0);
            }
            assert_eq!(page, first_page);
            p.read(PAGESIZE, &mut page);
            assert!(p.len() <= cache_size);
            if cache_size > 0 {
                assert!(p.len() > 0);
            }
            assert_eq!(page, second_page);
            p.read(PAGESIZE * 2, &mut page);
            assert!(p.len() <= cache_size);
            if cache_size > 0 {
                assert!(p.len() > 0);
            }
            assert_eq!(page, third_page);
        }
    }
}

struct PageCacheIO<'this> {
    offset: u64,
    pagecache: &'this mut PageCache,
}

impl<'this> Read for &mut PageCacheIO<'this> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        assert_eq!(buf.len(), PAGESIZE as usize);
        let fixed_buf = <&mut [u8; PAGESIZE as usize]>::try_from(buf).unwrap();
        self.pagecache.read(self.offset, fixed_buf);
        self.offset += PAGESIZE;
        Ok(PAGESIZE as usize)
    }
}

impl<'this> Write for PageCacheIO<'this> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        assert_eq!(buf.len(), PAGESIZE as usize);
        let fixed_buf = <&[u8; PAGESIZE as usize]>::try_from(buf).unwrap();
        self.pagecache.write(self.offset, *fixed_buf);
        self.offset += PAGESIZE;
        Ok(PAGESIZE as usize)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.pagecache.sync();
        Ok(())
    }
}

//        ON DISK FORMAT
//
// | Byte Range     | Value          |
// |----------------|----------------|
// |        0 - 4   | Magic Number   |
// |        4 - 8   | Format Version |
// |        8 - 16  | Term           |
// |       16 - 32  | Voted For      |
// |       32 - 40  | Log Length     |
// |       40 - 44  | Checksum       |
// | PAGESIZE - EOF | Log Entries    |
//
//           ON DISK LOG ENTRY FORMAT
//
// | Byte Range                   | Value              |
// |------------------------------|--------------------|
// |  0                           | Entry Start Marker |
// |  1 - 5                       | Checksum           |
// |  5 - 13                      | Log Index          |
// | 13 - 21                      | Term               |
// | 21 - 37                      | Client Serial Id   |
// | 37 - 53                      | Client Id          |
// | 53 - 61                      | Command Length     |
// | 61 - (61 + $Command Length$) | Command            |
//
// $Entry Start$ is `1` when the page is the start of an entry, not an
// overflow page.

#[derive(Debug, Clone)]
struct LogEntry {
    // Actual data.
    command: Vec<u8>,
    index: u64,
    term: u64,
    client_serial_id: u128,
    client_id: u128,
}

impl PartialEq for LogEntry {
    fn eq(&self, other: &Self) -> bool {
        self.command == other.command && self.term == other.term
    }
}

impl LogEntry {
    fn command_first_page(command_length: usize) -> usize {
        let page_minus_metadata = (PAGESIZE - 61) as usize;
        if command_length <= page_minus_metadata {
            command_length
        } else {
            page_minus_metadata
        }
    }

    fn store_metadata(&self, buffer: &mut [u8; PAGESIZE as usize]) -> usize {
        *buffer = [0; PAGESIZE as usize];
        let command_length = self.command.len();

        buffer[0] = 1; // Entry start marker.
        buffer[5..13].copy_from_slice(&self.term.to_le_bytes());
        buffer[13..21].copy_from_slice(&self.index.to_le_bytes());
        buffer[21..37].copy_from_slice(&self.client_serial_id.to_le_bytes());
        buffer[37..53].copy_from_slice(&self.client_id.to_le_bytes());
        buffer[53..61].copy_from_slice(&command_length.to_le_bytes());

        let mut checksum = CRC32C::new();
        checksum.update(&buffer[5..61]);
        checksum.update(&self.command);
        buffer[1..5].copy_from_slice(&checksum.sum().to_le_bytes());

        let command_first_page = LogEntry::command_first_page(command_length);
        buffer[61..61 + command_first_page].copy_from_slice(&self.command[0..command_first_page]);
        command_length - command_first_page
    }

    fn store_overflow(&self, buffer: &mut [u8; PAGESIZE as usize], offset: usize) -> usize {
        let to_write = self.command.len() - offset;
        let filled = if to_write > PAGESIZE as usize - 1 {
            // -1 for the overflow marker.
            PAGESIZE as usize - 1
        } else {
            to_write
        };
        buffer[0] = 0; // Overflow marker.
        buffer[1..1 + filled].copy_from_slice(&self.command[offset..offset + filled]);
        filled
    }

    fn encode(&self, buffer: &mut [u8; PAGESIZE as usize], mut writer: impl std::io::Write) -> u64 {
        let to_write = self.store_metadata(buffer);
        writer.write_all(buffer).unwrap();
        let mut pages = 1;

        let mut written = self.command.len() - to_write;

        while written < self.command.len() {
            let filled = self.store_overflow(buffer, written);
            writer.write_all(buffer).unwrap();
            written += filled;
            pages += 1;
        }

        pages
    }

    fn recover_metadata(page: &[u8; PAGESIZE as usize]) -> (LogEntry, u32, usize) {
        assert_eq!(page[0], 1); // Start of entry marker.
        let term = u64::from_le_bytes(page[5..13].try_into().unwrap());
        let index = u64::from_le_bytes(page[13..21].try_into().unwrap());
        let client_serial_id = u128::from_le_bytes(page[21..37].try_into().unwrap());
        let client_id = u128::from_le_bytes(page[37..53].try_into().unwrap());
        let command_length = u64::from_le_bytes(page[53..61].try_into().unwrap()) as usize;
        let stored_checksum = u32::from_le_bytes(page[1..5].try_into().unwrap());

        // recover_metadata() will only decode the first page's worth of
        // the command. Call recover_overflow() to decode any
        // additional pages.
        let command_first_page = LogEntry::command_first_page(command_length);
        let mut command = vec![0; command_length];
        command[0..command_first_page].copy_from_slice(&page[61..61 + command_first_page]);

        (
            LogEntry {
                index,
                term,
                command,
                client_serial_id,
                client_id,
            },
            stored_checksum,
            command_first_page,
        )
    }

    fn recover_overflow(
        page: &[u8; PAGESIZE as usize],
        command: &mut [u8],
        command_read: usize,
    ) -> usize {
        let to_read = command.len() - command_read;

        // Entry start marker is false for overflow page.
        assert_eq!(page[0], 0);

        let fill = if to_read > PAGESIZE as usize - 1 {
            // -1 for the entry start marker.
            PAGESIZE as usize - 1
        } else {
            to_read
        };
        command[command_read..command_read + fill].copy_from_slice(&page[1..1 + fill]);
        fill
    }

    fn decode(mut reader: impl std::io::Read) -> LogEntry {
        let mut page = [0; PAGESIZE as usize];
        // Since entries are always encoded into complete PAGESIZE
        // bytes, for network or for disk, it should always be
        // reasonable to block on an entire PAGESIZE of bytes, for
        // network or for disk.
        reader.read_exact(&mut page).unwrap();

        let (mut entry, stored_checksum, command_read) = LogEntry::recover_metadata(&page);
        let mut actual_checksum = CRC32C::new();
        actual_checksum.update(&page[5..61]);

        let mut read = command_read;
        while read < entry.command.len() {
            reader.read_exact(&mut page).unwrap();
            let filled = LogEntry::recover_overflow(&page, &mut entry.command, read);
            read += filled;
        }

        actual_checksum.update(&entry.command);
        assert_eq!(stored_checksum, actual_checksum.sum());
        entry
    }

    fn decode_from_pagecache(pagecache: &mut PageCache, offset: u64) -> (LogEntry, u64) {
        let mut reader = PageCacheIO { offset, pagecache };
        let entry = LogEntry::decode(&mut reader);
        let offset = reader.offset;

        (entry, offset)
    }
}

struct DurableState {
    // In-memory data.
    last_log_term: u64,
    next_log_index: u64,
    next_log_offset: u64,
    pagecache: PageCache,

    // On-disk data.
    current_term: u64,
    voted_for: u128, // Zero is the None value. User must not be a valid server id.
}

impl DurableState {
    fn new(data_directory: &std::path::Path, id: u128, page_cache_size: usize) -> DurableState {
        let mut filename = data_directory.to_path_buf();
        filename.push(format!("server_{}.data", id));
        let file = std::fs::File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(filename)
            .expect("Could not open data file.");
        DurableState {
            last_log_term: 0,
            next_log_index: 0,
            next_log_offset: PAGESIZE,
            pagecache: PageCache::new(file, page_cache_size),

            current_term: 0,
            voted_for: 0,
        }
    }

    fn restore(&mut self) {
        // If there's nothing to restore, calling append with the
        // required 0th empty log entry will be sufficient to get
        // state into the right place.
        if let Ok(m) = self.pagecache.file.metadata() {
            if m.len() == 0 {
                self.append(&mut [LogEntry {
                    index: 0,
                    term: 0,
                    command: vec![],
                    client_serial_id: 0,
                    client_id: 0,
                }]);
                return;
            }
        }

        let mut metadata: [u8; PAGESIZE as usize] = [0; PAGESIZE as usize];
        self.pagecache.read(0, &mut metadata);

        // Magic number check.
        assert_eq!(metadata[0..4], 0xFABEF15E_u32.to_le_bytes());

        // Version number check.
        assert_eq!(metadata[4..8], 1_u32.to_le_bytes());

        self.current_term = u64::from_le_bytes(metadata[8..16].try_into().unwrap());
        self.voted_for = u128::from_le_bytes(metadata[16..32].try_into().unwrap());

        let checksum = u32::from_le_bytes(metadata[40..44].try_into().unwrap());
        if checksum != crc32c(&metadata[0..40]) {
            panic!("Bad checksum for data file.");
        }

        let log_length = u64::from_le_bytes(metadata[32..40].try_into().unwrap()) as usize;

        let mut scanned = 0;
        while scanned < log_length {
            self.next_log_index += 1;

            let (e, new_offset) =
                LogEntry::decode_from_pagecache(&mut self.pagecache, self.next_log_offset);
            self.last_log_term = e.term;
            self.next_log_offset = new_offset;
            scanned += 1;
        }
    }

    #[allow(dead_code)]
    fn debug_client_entry_count(&mut self) -> u64 {
        let mut count = 0;
        for i in 0..self.next_log_index {
            let e = self.log_at_index(i);
            if !e.command.is_empty() {
                count += 1;
            }
        }

        count
    }

    fn append(&mut self, entries: &mut [LogEntry]) {
        self.append_from_index(entries, self.next_log_index);
    }

    // Durably add logs to disk.
    fn append_from_index(&mut self, entries: &mut [LogEntry], from_index: u64) {
        let mut buffer: [u8; PAGESIZE as usize] = [0; PAGESIZE as usize];

        self.next_log_offset = self.offset_from_index(from_index);
        // This is extremely important. Sometimes the log must be
        // truncated. This is what does the truncation. Existing
        // messages are not necessarily overwritten. But metadata for
        // what the current last log index is always correct.
        self.next_log_index = from_index;

        let mut writer = PageCacheIO {
            offset: self.next_log_offset,
            pagecache: &mut self.pagecache,
        };
        if !entries.is_empty() {
            // Write out all new logs.
            for entry in entries.iter_mut() {
                entry.index = self.next_log_index;
                //println!("Entry at {} has index: {}.", self.next_log_offset, entry.index);
                self.next_log_index += 1;

                assert!(self.next_log_offset >= PAGESIZE);

                let pages = entry.encode(&mut buffer, &mut writer);
                self.next_log_offset += pages * PAGESIZE;

                self.last_log_term = entry.term;
            }

            writer.flush().unwrap();
        }

        // Write log length metadata.
        self.update(self.current_term, self.voted_for);
    }

    // Durably save non-log data.
    fn update(&mut self, term: u64, voted_for: u128) {
        self.current_term = term;
        self.voted_for = voted_for;

        let mut metadata: [u8; PAGESIZE as usize] = [0; PAGESIZE as usize];
        // Magic number.
        metadata[0..4].copy_from_slice(&0xFABEF15E_u32.to_le_bytes());
        // Version.
        metadata[4..8].copy_from_slice(&1_u32.to_le_bytes());

        metadata[8..16].copy_from_slice(&term.to_le_bytes());

        metadata[16..32].copy_from_slice(&voted_for.to_le_bytes());

        let log_length = self.next_log_index;
        metadata[32..40].copy_from_slice(&log_length.to_le_bytes());

        let checksum = crc32c(&metadata[0..40]);
        metadata[40..44].copy_from_slice(&checksum.to_le_bytes());

        self.pagecache.write(0, metadata);
        self.pagecache.sync();
    }

    fn offset_from_index(&mut self, index: u64) -> u64 {
        if index == self.next_log_index {
            return self.next_log_offset;
        }

        //println!("Looking for {index}.");

        assert!(index < self.next_log_index);
        let mut page: [u8; PAGESIZE as usize] = [0; PAGESIZE as usize];

        // Rather than linear search backwards, we store the index in
        // the page itself and then do a binary search on disk.
        let mut l = PAGESIZE;
        let mut r = self.next_log_offset - PAGESIZE;
        //println!("l is {l}, r is {r}");
        while l <= r {
            let mut m = l + (r - l) / 2;
            // Round up to the nearest page.
            m += m % PAGESIZE;
            //println!("M is {m}.");
            assert_eq!(m % PAGESIZE, 0);

            // Look for a start of entry page.
            self.pagecache.read(m, &mut page);
            while page[0] != 1 {
                m -= PAGESIZE;
                self.pagecache.read(m, &mut page);
            }

            // TODO: Bad idea to hardcode the offset.
            let current_index = u64::from_le_bytes(page[13..21].try_into().unwrap());
            //println!("Index found at {m} is: {current_index}. Looking for {index}.");
            if current_index == index {
                return m;
            }

            if current_index < index {
                // Read until the next entry, set m to the next entry.
                page[0] = 0;
                m += PAGESIZE;
                self.pagecache.read(m, &mut page);
                while page[0] != 1 {
                    m += PAGESIZE;
                    self.pagecache.read(m, &mut page);
                }

                l = m;
            } else {
                r = m - PAGESIZE;
            }

            //println!("l is {l}, r is {r}. index is {index}");
        }

        unreachable!(
            "Could not find index {index} with log length: {}.",
            self.next_log_index
        );
    }

    fn log_at_index(&mut self, i: u64) -> LogEntry {
        let offset = self.offset_from_index(i);
        let (entry, _) = LogEntry::decode_from_pagecache(&mut self.pagecache, offset);
        entry
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
enum Condition {
    Leader,
    Follower,
    Candidate,
}

impl std::fmt::Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

struct VolatileState {
    condition: Condition,

    commit_index: u64,
    last_applied: u64,

    // Timeouts
    election_frequency: Duration, // Read-only.
    election_timeout: Instant,    // Randomly set based on election_frequency.
    rand: Random,

    // Leader-only state.
    next_index: Vec<u64>,
    match_index: Vec<u64>,

    // Candidate-only state.
    votes: usize,
}

impl VolatileState {
    fn new(cluster_size: usize, election_frequency: Duration, rand: Random) -> VolatileState {
        let jitter = election_frequency.as_secs_f64() / 3.0;
        VolatileState {
            condition: Condition::Follower,
            commit_index: 0,
            last_applied: 0,
            next_index: vec![0; cluster_size],
            match_index: vec![0; cluster_size],
            votes: 0,

            election_frequency,
            election_timeout: Instant::now() + Duration::from_secs_f64(jitter),
            rand,
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
    logger: Logger,
    durable: DurableState,
    volatile: VolatileState,

    // Non-Raft state.
    stopped: bool,
}

impl State {
    fn log<S: AsRef<str> + std::fmt::Display>(&self, msg: S) {
        self.logger.log(
            self.durable.current_term,
            self.durable.next_log_index,
            self.volatile.condition,
            msg,
        );
    }

    fn next_request_id(&mut self) -> u64 {
        self.volatile.rand.generate_u64()
    }

    fn reset_election_timeout(&mut self) {
        let random_percent = self.volatile.rand.generate_percent();
        let positive = self.volatile.rand.generate_bool();
        let jitter = random_percent as f64 * (self.volatile.election_frequency.as_secs_f64() / 2.0);

        let mut new_timeout = self.volatile.election_frequency;
        // Duration apparently isn't allowed to be negative.
        if positive {
            new_timeout += Duration::from_secs_f64(jitter);
        } else {
            new_timeout -= Duration::from_secs_f64(jitter);
        }

        self.volatile.election_timeout = Instant::now() + new_timeout;

        self.log(format!(
            "Resetting election timeout: {}ms.",
            new_timeout.as_millis()
        ));
    }

    fn transition(&mut self, condition: Condition, term_increase: u64, voted_for: u128) {
        assert_ne!(self.volatile.condition, condition);
        self.log(format!("Became {}.", condition));
        self.volatile.condition = condition;
        // Reset vote.
        self.durable
            .update(self.durable.current_term + term_increase, voted_for);
    }
}

//             REQUEST WIRE PROTOCOL
//
// | Byte Range | Value                                         |
// |------------|-----------------------------------------------|
// |  0 - 8     | Request Id                                    |
// |  8 - 24    | Sender Id                                     |
// | 24         | Request Type                                  |
// | 25 - 33    | Term                                          |
// | 33 - 49    | Leader Id / Candidate Id                      |
// | 49 - 57    | Prev Log Index / Last Log Index               |
// | 57 - 65    | Prev Log Term / Last Log Term                 |
// | 65 - 73    | (Request Vote) Checksum / Leader Commit Index |
// | 73         | Entries Length                                |
// | 74 - 78    | (Append Entries) Checksum                     |
// | 78 - EOM   | Entries                                       |
//
//             ENTRIES WIRE PROTOCOL
//
// See: ON DISK LOG ENTRY FORMAT.
//
//             RESPONSE WIRE PROTOCOL
//
// | Byte Range | Value                                 |
// |------------|---------------------------------------|
// |  0 - 8     | Request Id                            |
// |  8 - 24    | Sender Id                             |
// | 24         | Response Type                         |
// | 25 - 33    | Term                                  |
// | 33         | Success / Vote Granted                |
// | 34 - 42    | (Request Vote) Checksum / Match Index |
// | 42 - 46    | (Append Entries) Checksum             |

struct RPCMessageEncoder<T: std::io::Write> {
    request_id: u64, // Not part of Raft. Used only for debugging.
    sender_id: u128,
    writer: BufWriter<T>,
    written: Vec<u8>,
}

impl<T: std::io::Write> RPCMessageEncoder<T> {
    fn new(request_id: u64, sender_id: u128, writer: BufWriter<T>) -> RPCMessageEncoder<T> {
        RPCMessageEncoder {
            request_id,
            writer,
            written: vec![],
            sender_id,
        }
    }

    fn metadata(&mut self, kind: u8, term: u64) {
        assert_eq!(self.written.len(), 0);

        self.written
            .extend_from_slice(&self.request_id.to_le_bytes());

        self.written
            .extend_from_slice(&self.sender_id.to_le_bytes());

        self.written.push(kind);

        self.written.extend_from_slice(&term.to_le_bytes());
        assert_eq!(self.written.len(), 33);

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
    request_id: u64, // Not part of Raft. Used only for debugging.
    term: u64,
    candidate_id: u128,
    last_log_index: u64,
    last_log_term: u64,
}

impl RequestVoteRequest {
    fn decode<T: std::io::Read>(
        mut reader: BufReader<T>,
        metadata: [u8; 33],
        request_id: u64,
        term: u64,
    ) -> Result<RPCBody, String> {
        let mut buffer: [u8; 69] = [0; 69];
        buffer[0..metadata.len()].copy_from_slice(&metadata);
        reader.read_exact(&mut buffer[metadata.len()..]).unwrap();

        let checksum = u32::from_le_bytes(buffer[65..69].try_into().unwrap());
        if checksum != crc32c(&buffer[0..65]) {
            return Err("Bad checksum.".into());
        }

        let candidate_id = u128::from_le_bytes(buffer[33..49].try_into().unwrap());
        let last_log_index = u64::from_le_bytes(buffer[49..57].try_into().unwrap());
        let last_log_term = u64::from_le_bytes(buffer[57..65].try_into().unwrap());

        Ok(RPCBody::RequestVoteRequest(RequestVoteRequest {
            request_id,
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }))
    }

    fn encode<T: std::io::Write>(&self, encoder: &mut RPCMessageEncoder<T>) {
        encoder.metadata(RPCBodyKind::RequestVoteRequest as u8, self.term);
        encoder.data(&self.candidate_id.to_le_bytes());
        encoder.data(&self.last_log_index.to_le_bytes());
        encoder.data(&self.last_log_term.to_le_bytes());
        encoder.done();
    }
}

#[derive(Debug, PartialEq)]
struct RequestVoteResponse {
    request_id: u64, // Not part of Raft. Used only for debugging.
    term: u64,
    vote_granted: bool,
}

impl RequestVoteResponse {
    fn decode<T: std::io::Read>(
        mut reader: BufReader<T>,
        metadata: [u8; 33],
        request_id: u64,
        term: u64,
    ) -> Result<RPCBody, String> {
        let mut buffer: [u8; 38] = [0; 38];
        buffer[0..metadata.len()].copy_from_slice(&metadata);
        reader.read_exact(&mut buffer[metadata.len()..]).unwrap();

        let checksum = u32::from_le_bytes(buffer[34..38].try_into().unwrap());
        if checksum != crc32c(&buffer[0..34]) {
            return Err("Bad checksum.".into());
        }

        Ok(RPCBody::RequestVoteResponse(RequestVoteResponse {
            request_id,
            term,
            vote_granted: buffer[33] == 1,
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
    request_id: u64, // Not part of Raft. Used only for debugging.
    term: u64,
    leader_id: u128,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: u64,
}

impl AppendEntriesRequest {
    fn decode<T: std::io::Read>(
        mut reader: BufReader<T>,
        metadata: [u8; 33],
        request_id: u64,
        term: u64,
    ) -> Result<RPCBody, String> {
        let mut buffer: [u8; 78] = [0; 78];
        buffer[0..metadata.len()].copy_from_slice(&metadata);
        reader.read_exact(&mut buffer[metadata.len()..]).unwrap();

        let checksum = u32::from_le_bytes(buffer[74..78].try_into().unwrap());
        if checksum != crc32c(&buffer[0..74]) {
            return Err("Bad checksum.".into());
        }

        let leader_id = u128::from_le_bytes(buffer[33..49].try_into().unwrap());
        let prev_log_index = u64::from_le_bytes(buffer[49..57].try_into().unwrap());
        let prev_log_term = u64::from_le_bytes(buffer[57..65].try_into().unwrap());
        let leader_commit = u64::from_le_bytes(buffer[65..73].try_into().unwrap());
        let entries_length = buffer[73] as usize;
        let mut entries = Vec::<LogEntry>::with_capacity(entries_length);

        while entries.len() < entries_length {
            let e = LogEntry::decode(&mut reader);
            entries.push(e);
        }

        Ok(RPCBody::AppendEntriesRequest(AppendEntriesRequest {
            request_id,
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            leader_commit,
            entries,
        }))
    }

    fn encode<T: std::io::Write>(&self, encoder: &mut RPCMessageEncoder<T>) {
        encoder.metadata(RPCBodyKind::AppendEntriesRequest as u8, self.term);
        encoder.data(&self.leader_id.to_le_bytes());
        encoder.data(&(self.prev_log_index).to_le_bytes());
        encoder.data(&self.prev_log_term.to_le_bytes());
        encoder.data(&(self.leader_commit).to_le_bytes());
        assert!(self.entries.len() <= 0xFF);
        encoder.data(&(self.entries.len() as u8).to_le_bytes());
        encoder.done();

        let mut buffer: [u8; PAGESIZE as usize] = [0; PAGESIZE as usize];
        for entry in self.entries.iter() {
            entry.encode(&mut buffer, &mut encoder.writer);
        }
        encoder.writer.flush().unwrap();
    }
}

#[derive(Debug, PartialEq, Clone)]
struct AppendEntriesResponse {
    request_id: u64,
    term: u64,
    success: bool,

    // When `success == true`, `match_index` is the value the leader
    // should set this server's `match_index` to. `next_index` should
    // be set to `match_index + 1`.
    //
    // When `success == false`, `match_index` is the value the leader
    // should set this server's `next_index` to.
    //
    // This isn't something the Raft paper proscribes though it is
    // used in the TLA+ spec. It seems necessary if you are supposed
    // to allow multiple in-flight requests per follower.
    match_index: u64,
}

impl AppendEntriesResponse {
    fn decode<T: std::io::Read>(
        mut reader: BufReader<T>,
        metadata: [u8; 33],
        request_id: u64,
        term: u64,
    ) -> Result<RPCBody, String> {
        let mut buffer: [u8; 46] = [0; 46];
        buffer[0..metadata.len()].copy_from_slice(&metadata);
        reader.read_exact(&mut buffer[metadata.len()..]).unwrap();

        let match_index = u64::from_le_bytes(buffer[34..42].try_into().unwrap());

        let checksum = u32::from_le_bytes(buffer[42..46].try_into().unwrap());
        if checksum != crc32c(&buffer[0..42]) {
            return Err("Bad checksum.".into());
        }

        Ok(RPCBody::AppendEntriesResponse(AppendEntriesResponse {
            request_id,
            term,
            success: buffer[33] == 1,
            match_index,
        }))
    }

    fn encode<T: std::io::Write>(&self, encoder: &mut RPCMessageEncoder<T>) {
        encoder.metadata(RPCBodyKind::AppendEntriesResponse as u8, self.term);
        encoder.data(&[self.success as u8]);
        encoder.data(&self.match_index.to_le_bytes());
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

impl RPCBody {
    fn term(&self) -> u64 {
        match self {
            RPCBody::RequestVoteRequest(r) => r.term,
            RPCBody::RequestVoteResponse(r) => r.term,
            RPCBody::AppendEntriesRequest(r) => r.term,
            RPCBody::AppendEntriesResponse(r) => r.term,
        }
    }

    fn request_id(&self) -> u64 {
        match self {
            RPCBody::RequestVoteRequest(r) => r.request_id,
            RPCBody::RequestVoteResponse(r) => r.request_id,
            RPCBody::AppendEntriesRequest(r) => r.request_id,
            RPCBody::AppendEntriesResponse(r) => r.request_id,
        }
    }
}

#[derive(Debug, PartialEq)]
struct RPCMessage {
    from: u128,
    body: RPCBody,
}

impl RPCMessage {
    fn term(&self) -> u64 {
        self.body.term()
    }

    fn request_id(&self) -> u64 {
        self.body.request_id()
    }

    fn decode<T: std::io::Read>(mut reader: BufReader<T>) -> Result<RPCMessage, String> {
        let mut metadata: [u8; 33] = [0; 33];
        if reader.read_exact(&mut metadata).is_err() {
            return Err("Could not read metadata.".into());
        }

        let request_id = u64::from_le_bytes(metadata[0..8].try_into().unwrap());
        let server_id = u128::from_le_bytes(metadata[8..24].try_into().unwrap());

        let message_type = metadata[24];
        let term = u64::from_le_bytes(metadata[25..33].try_into().unwrap());
        let body = if message_type == RPCBodyKind::RequestVoteRequest as u8 {
            RequestVoteRequest::decode(reader, metadata, request_id, term)
        } else if message_type == RPCBodyKind::RequestVoteResponse as u8 {
            RequestVoteResponse::decode(reader, metadata, request_id, term)
        } else if message_type == RPCBodyKind::AppendEntriesRequest as u8 {
            AppendEntriesRequest::decode(reader, metadata, request_id, term)
        } else if message_type == RPCBodyKind::AppendEntriesResponse as u8 {
            AppendEntriesResponse::decode(reader, metadata, request_id, term)
        } else {
            return Err(format!("Unknown request type: {}.", message_type));
        };

        Ok(RPCMessage {
            from: server_id,
            body: body?,
        })
    }

    fn encode<T: std::io::Write>(&self, sender_id: u128, writer: BufWriter<T>) {
        let encoder = &mut RPCMessageEncoder::new(self.request_id(), sender_id, writer);
        match &self.body {
            RPCBody::RequestVoteRequest(rvr) => rvr.encode(encoder),
            RPCBody::RequestVoteResponse(rvr) => rvr.encode(encoder),
            RPCBody::AppendEntriesRequest(aer) => aer.encode(encoder),
            RPCBody::AppendEntriesResponse(aer) => aer.encode(encoder),
        };
    }
}

#[derive(Clone)]
struct Logger {
    server_id: u128,
    debug: bool,
}

impl Logger {
    fn log<S: AsRef<str> + std::fmt::Display>(
        &self,
        term: u64,
        log_length: u64,
        condition: Condition,
        msg: S,
    ) {
        if !self.debug {
            return;
        }

        println!(
            "[S: {: <3} T: {: <3} L: {: <3} C: {}] {}",
            self.server_id,
            term,
            log_length,
            match condition {
                Condition::Leader => "L",
                Condition::Candidate => "C",
                Condition::Follower => "F",
            },
            msg
        );
    }
}

struct RPCManager {
    cluster: Vec<ServerConfig>,
    server_id: u128,
    stream_sender: mpsc::Sender<RPCMessage>,
    stream_receiver: mpsc::Receiver<RPCMessage>,
    stop_mutex: Arc<Mutex<bool>>,
    logger: Logger,
}

impl RPCManager {
    fn new(server_id: u128, cluster: Vec<ServerConfig>, logger: Logger) -> RPCManager {
        let (stream_sender, stream_receiver): (
            mpsc::Sender<RPCMessage>,
            mpsc::Receiver<RPCMessage>,
        ) = mpsc::channel();
        RPCManager {
            logger,
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

        let thread_stop = self.stop_mutex.clone();
        let thread_stream_sender = self.stream_sender.clone();
        std::thread::spawn(move || {
            loop {
                let listener = match std::net::TcpListener::bind(address) {
                    Ok(l) => l,
                    Err(e) => panic!("Could not bind to {address}: {e}."),
                };

                for stream in listener.incoming().flatten() {
                    // For this logic to be triggered, we must create a
                    // connection to our own server after setting
                    // `thread_stop` to `true`.
                    let stop = thread_stop.lock().unwrap();
                    if *stop {
                        return;
                    }

                    let bufreader = BufReader::new(stream);
                    match RPCMessage::decode(bufreader) {
                        Ok(msg) => thread_stream_sender.send(msg).unwrap(),
                        Err(msg) => panic!("Could not read request. Error: {}.", msg),
                    }
                }
            }
        });
    }

    fn send(
        &mut self,
        log_length: u64,
        condition: Condition,
        to_server_id: u128,
        message: &RPCMessage,
    ) {
        let address = self.address_from_id(to_server_id);
        let server_id = self.server_id;

        self.logger.log(
            message.term(),
            log_length,
            condition,
            format!("Sending {:?} to {}.", message.body, to_server_id),
        );
        let stream = if let Ok(stream) = std::net::TcpStream::connect(address) {
            stream
        } else {
            self.logger.log(
                message.term(),
                log_length,
                condition,
                format!("Could not connect to {to_server_id}."),
            );
            return;
        };
        let bufwriter = BufWriter::new(stream.try_clone().unwrap());
        message.encode(server_id, bufwriter);
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
    election_frequency: Duration,

    // Random.
    random_seed: [u64; 4],

    // Logger.
    logger_debug: bool,

    page_cache_size: usize,
}

pub struct Server<SM: StateMachine> {
    config: Config,

    sm: SM,
    rpc_manager: RPCManager,

    state: Mutex<State>,

    client_id: u128,
    apply_sender: mpsc::Sender<Vec<u8>>,
}

impl<SM: StateMachine> Drop for Server<SM> {
    fn drop(&mut self) {
        self.stop();
    }
}

impl<SM: StateMachine> Server<SM> {
    pub fn apply(&mut self, commands: Vec<Vec<u8>>, command_ids: Vec<u128>) -> ApplyResult {
        assert_eq!(commands.len(), command_ids.len());

        // Append commands to local durable state if leader.
        let mut state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Leader {
            return ApplyResult::NotALeader;
        }

        let mut entries = Vec::with_capacity(commands.len());
        for (i, &id) in command_ids.iter().enumerate() {
            assert_ne!(id, 0);

            entries.push(LogEntry {
                index: 0,
                term: state.durable.current_term,
                command: commands[i].clone(),
                client_serial_id: id,
                client_id: self.client_id,
            });
        }

        state.durable.append(&mut entries);

        // TODO: How to handle timeouts?
        ApplyResult::Ok
    }

    fn handle_request_vote_request(
        &mut self,
        request: RequestVoteRequest,
        _: u128,
    ) -> Option<RPCBody> {
        let mut state = self.state.lock().unwrap();
        let term = state.durable.current_term;
        let false_request = RPCBody::RequestVoteResponse(RequestVoteResponse {
            request_id: request.request_id,
            term,
            vote_granted: false,
        });

        if request.term < term {
            return Some(false_request);
        }
        // If it isn't less than, local state would already have been
        // modified so me.term == request.term in handle_message.
        assert_eq!(request.term, term);

        let canvote =
            state.durable.voted_for == 0 || state.durable.voted_for == request.candidate_id;
        if !canvote {
            return Some(false_request);
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

        let log_length = state.durable.next_log_index;
        let last_log_term = state.durable.last_log_term;
        let vote_granted = request.last_log_term > last_log_term
            || (request.last_log_term == last_log_term
                && (request.last_log_index >= log_length - 1));
        state.log(format!(
            "RVR mll: {log_length}, mlt: {}; rll: {}, rlt: {}",
            last_log_term, request.last_log_index, request.last_log_term
        ));

        if vote_granted {
            state.durable.update(term, request.candidate_id);

            // Reset election timer.
            //
            // "If election timeout elapses without receiving AppendEntries
            // RPC from current leader or granting vote to candidate:
            // convert to candidate" - Reference [0] Page 4
            state.reset_election_timeout();
        }

        let msg = RPCBody::RequestVoteResponse(RequestVoteResponse {
            request_id: request.request_id,
            term,
            vote_granted,
        });
        Some(msg)
    }

    fn handle_request_vote_response(
        &mut self,
        response: RequestVoteResponse,
        _: u128,
    ) -> Option<RPCBody> {
        let mut state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Candidate {
            return None;
        }

        let quorum = self.config.cluster.len() / 2;
        assert!(quorum > 0 || (self.config.cluster.len() == 1 && quorum == 0));

        if response.vote_granted {
            state.volatile.votes += 1;
            // This will not handle the case where a single
            // server-cluster needs to become the leader.
            if state.volatile.votes == quorum {
                drop(state);
                self.candidate_become_leader();
            }
        }

        None
    }

    fn handle_append_entries_request(
        &mut self,
        mut request: AppendEntriesRequest,
        from: u128,
    ) -> Option<RPCBody> {
        let mut state = self.state.lock().unwrap();
        let term = state.durable.current_term;

        let false_response = |match_index| -> Option<RPCBody> {
            Some(RPCBody::AppendEntriesResponse(AppendEntriesResponse {
                request_id: request.request_id,
                term,
                success: false,
                match_index,
            }))
        };

        // "1. Reply false if term < currentTerm (§5.1)." - Reference [0] Page 4
        if request.term < term {
            state.log(format!(
                "Cannot accept AppendEntries from {} because it is out of date ({} < {}).",
                from, request.term, term
            ));
            return false_response(0);
        }

        // "If AppendEntries RPC received from new leader: convert to
        // follower." - Reference [0] Page 4
        if state.volatile.condition == Condition::Candidate {
            state.transition(Condition::Follower, 0, 0);
        }

        if state.volatile.condition != Condition::Follower {
            assert_eq!(state.volatile.condition, Condition::Leader);
            state.log(format!(
                "Cannot accept AppendEntries from {} because I am a leader.",
                from
            ));
            return false_response(0);
        }

        // Reset heartbeat timer because we've now heard from a valid leader.
        state.reset_election_timeout();

        // "Reply false if log doesn’t contain an entry at prevLogIndex
        // whose term matches prevLogTerm (§5.3)." - Reference [0] Page 4
        if request.prev_log_index > 0 {
            if request.prev_log_index >= state.durable.next_log_index {
                state.log(format!("Cannot accept AppendEntries from {} because prev_log_index ({}) is ahead of my log ({}).", from, request.prev_log_index, state.durable.next_log_index));
                return false_response(std::cmp::max(state.durable.next_log_index, 1) - 1);
            }

            let e = state.durable.log_at_index(request.prev_log_index);
            if e.term != request.prev_log_term {
                assert!(request.prev_log_index > 0);
                state.log(format!("Cannot accept AppendEntries from {} because prev_log_term ({}) does not match mine ({}).", from, request.prev_log_term, e.term));
                return false_response(request.prev_log_index - 1);
            }
        }

        // "If an existing entry conflicts with a new one (same index
        // but different terms), delete the existing entry and all that
        // follow it (§5.3)." - Reference [0] Page 4
        let mut append_offset = 0;
        let mut real_index = request.prev_log_index + 1;
        for entry in request.entries.iter() {
            if real_index == state.durable.next_log_index {
                // Found a new entry, no need to look it up.
                break;
            }

            let e = state.durable.log_at_index(real_index);
            if e.term != entry.term {
                break;
            }

            real_index += 1;
            append_offset += 1;
        }

        // 4. Append any new entries not already in the log
        state
            .durable
            .append_from_index(&mut request.entries[append_offset..], real_index);

        // "If leaderCommit > commitIndex, set commitIndex =
        // min(leaderCommit, index of last new entry)." - Reference [0] Page 4
        if request.leader_commit > state.volatile.commit_index {
            state.volatile.commit_index = std::cmp::min(
                request.leader_commit,
                std::cmp::max(state.durable.next_log_index, 1) - 1,
            );
        }

        Some(RPCBody::AppendEntriesResponse(AppendEntriesResponse {
            request_id: request.request_id,
            term,
            success: true,
            match_index: request.prev_log_index + request.entries.len() as u64,
        }))
    }

    fn handle_append_entries_response(
        &mut self,
        response: AppendEntriesResponse,
        from: u128,
    ) -> Option<RPCBody> {
        let mut state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Leader {
            return None;
        }

        let server_index = self
            .config
            .cluster
            .iter()
            .position(|server| server.id == from)
            .unwrap();
        if response.success {
            let new_next_index = response.match_index + 1;
            assert!(new_next_index >= state.volatile.next_index[server_index]);
            state.volatile.next_index[server_index] = new_next_index;

            assert!(response.match_index >= state.volatile.match_index[server_index]);
            state.volatile.match_index[server_index] = response.match_index;
            state.log(format!(
                "AppendEntries request to {from} successful. New match index: {}.",
                response.match_index
            ));
        } else {
            // If the request is false, match_index servers as the
            // next index to try.
            state.volatile.next_index[server_index] = std::cmp::max(1, response.match_index);
            state.log(format!(
                "AppendEntries request to {from} not successful. New next index: {}.",
                state.volatile.next_index[server_index]
            ));
        }

        None
    }

    fn handle_message(&mut self, message: RPCMessage) -> Option<(RPCMessage, u128)> {
        // "If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (§5.1)."
        // - Reference [0] Page 4
        let mut state = self.state.lock().unwrap();
        if message.term() > state.durable.current_term {
            if state.volatile.condition != Condition::Follower {
                let term_diff = message.term() - state.durable.current_term;
                state.transition(Condition::Follower, term_diff, 0);
            } else {
                state.durable.update(message.term(), 0);
            }
        }
        drop(state);

        let rsp = match message.body {
            RPCBody::RequestVoteRequest(r) => self.handle_request_vote_request(r, message.from),
            RPCBody::RequestVoteResponse(r) => self.handle_request_vote_response(r, message.from),
            RPCBody::AppendEntriesRequest(r) => self.handle_append_entries_request(r, message.from),
            RPCBody::AppendEntriesResponse(r) => {
                self.handle_append_entries_response(r, message.from)
            }
        };

        Some((
            RPCMessage {
                body: rsp?,
                from: self.config.server_id,
            },
            message.from,
        ))
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

        let log_length = state.durable.next_log_index;
        // NOTE: Maybe the starting point to check can be optimized
        // but be careful.  One iteration tried to do the highest
        // common match_index. The problem is that if one node is
        // down, the highest common index may even be 0 even though
        // the cluster *could* make progress.

        // Check from front to back.
        for i in (0..log_length).rev() {
            let mut quorum = quorum_needed;

            // We don't need to keep checking backwards for a quorum
            // once we get to the current quorum.
            if state.volatile.commit_index == i {
                break;
            }

            state.log(format!("Checking for quorum ({quorum}) at index: {i}."));

            assert!(quorum > 0 || (self.config.cluster.len() == 1 && quorum == 0));
            let e = state.durable.log_at_index(i);
            for (server_index, &match_index) in state.volatile.match_index.iter().enumerate() {
                // self always counts as part of quorum, so skip it in
                // the count. quorum_needed already takes self into
                // consideration (`len() / 2` not `len() / 2 + 1`).
                if self.config.server_index == server_index {
                    continue;
                }

                // "If there exists an N such that N > commitIndex, a majority
                // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
                // set commitIndex = N (§5.3, §5.4)." - Reference [0] Page 4
                if match_index >= i && e.term == state.durable.current_term {
                    state.log(format!("Exists for ({}) at {i}.", server_index));
                    quorum -= 1;
                    if quorum == 0 {
                        break;
                    }
                } else if match_index >= i {
                    state.log(format!(
                        "Does not exist for ({}) at {i} (term is {}, our term is {}).",
                        server_index, e.term, state.durable.current_term,
                    ));
                } else {
                    state.log(format!("Does not exist for ({}) at {i}.", server_index));
                }
            }

            if quorum == 0 {
                state.volatile.commit_index = i;
                state.log(format!("New quorum at index: {i}."));
                break;
            } else {
                state.log(format!("No quorum yet at index: {i}."));
            }
        }
    }

    fn leader_send_heartbeat(&mut self) {
        // "Upon election: send initial empty AppendEntries RPCs
        // (heartbeat) to each server; repeat during idle periods to
        // prevent election timeouts (§5.2)." - Reference [0] Page 4
        let mut state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Leader {
            return;
        }

        let time_for_heartbeat = state.volatile.election_timeout > Instant::now();
        // "The broadcast time should be an order of magnitude less
        // than the election timeout so that leaders can reliably send
        // the heartbeat messages required to keep followers from
        // starting elections" - Reference [0] Page 10
        state.volatile.election_timeout = Instant::now() + (self.config.election_frequency / 10);

        let my_server_id = self.config.server_id;
        for (i, server) in self.config.cluster.iter().enumerate() {
            // Skip self.
            if server.id == my_server_id {
                continue;
            }

            let next_index = state.volatile.next_index[i];
            let prev_log_index = std::cmp::max(next_index, 1) - 1;

            // Handle common case where we don't need to look up a log
            // from pagecache if we're at the latest entry.
            let prev_log_term =
                if prev_log_index == std::cmp::max(state.durable.next_log_index, 1) - 1 {
                    state.durable.last_log_term
                } else {
                    let prev_log = state.durable.log_at_index(prev_log_index);
                    prev_log.term
                };

            let mut entries = vec![];
            if state.durable.next_log_index > next_index {
                let max_entries = 0xFF; // Length must fit into a byte.
                for i in 0..max_entries {
                    let index = i + next_index;
                    // At most as many logs as we currently have.
                    if index == state.durable.next_log_index {
                        break;
                    }

                    entries.push(state.durable.log_at_index(index));
                }
            } else if !time_for_heartbeat {
                // No need to send a blank request at this time.
                continue;
            }

            state.log(format!(
                "Sending AppendEntries request. Logs: {}.",
                entries.len()
            ));

            let msg = RPCMessage {
                from: my_server_id,
                body: RPCBody::AppendEntriesRequest(AppendEntriesRequest {
                    request_id: state.next_request_id(),
                    term: state.durable.current_term,
                    leader_id: my_server_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: state.volatile.commit_index,
                }),
            };
            self.rpc_manager.send(
                state.durable.next_log_index,
                state.volatile.condition,
                server.id,
                &msg,
            );
        }
    }

    fn follower_maybe_become_candidate(&mut self) {
        // "If election timeout elapses without receiving AppendEntries
        // RPC from current leader or granting vote to candidate:
        // convert to candidate." - Reference [0] Page 4
        let state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Follower {
            return;
        }

        if Instant::now() > state.volatile.election_timeout {
            drop(state);
            self.follower_become_candidate();
        }
    }

    fn candidate_maybe_timeout(&mut self) {
        let mut state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Candidate {
            return;
        }

        // Election timed out. Revert to follower and restart election.
        if Instant::now() > state.volatile.election_timeout {
            state.transition(Condition::Follower, 0, 0);
        }
    }

    fn candidate_become_leader(&mut self) {
        let mut state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Candidate {
            return;
        }

        state.volatile.reset();
        state.transition(Condition::Leader, 0, 0);
        state.volatile.election_timeout = Instant::now();

        for i in 0..self.config.cluster.len() {
            // "for each server, index of the next log entry
            // to send to that server (initialized to leader
            // last log index + 1)" - Reference [0] Page 4
            state.volatile.next_index[i] = state.durable.next_log_index;

            // "for each server, index of highest log entry
            // known to be replicated on server
            // (initialized to 0, increases monotonically)" - Reference [0] Page 4
            state.volatile.match_index[i] = 0;
        }

        // "First, a leader must have the latest information on which
        // entries are committed. The Leader Completeness Property
        // guarantees that a leader has all committed entries, but at
        // the start of its term, it may not know which those are. To
        // find out, it needs to commit an entry from its term. Raft
        // handles this by having each leader commit a blank no-op
        // entry into the log at the start of its term." - Reference
        // [0] Page 13
        let term = state.durable.current_term;
        state.durable.append(&mut [LogEntry {
            index: 0,
            command: vec![],
            term,
            client_serial_id: 0,
            client_id: 0,
        }]);

        drop(state);

        self.leader_send_heartbeat();
    }

    fn follower_become_candidate(&mut self) {
        let mut state = self.state.lock().unwrap();
        if state.volatile.condition != Condition::Follower {
            return;
        }

        // "On conversion to candidate, start election:
        //   • Increment currentTerm
        //   • Vote for self
        //   • Reset election timer
        //   • Send RequestVote RPCs to all other servers" - Reference [0] Page 4
        state.transition(Condition::Candidate, 1, self.config.server_id);
        state.reset_election_timeout();

        // Trivial case. In a single-server cluster, the server is the
        // leader.
        if self.config.cluster.len() == 1 {
            drop(state);
            self.candidate_become_leader();
            return;
        }

        for server in self.config.cluster.iter() {
            // Skip self.
            if server.id == self.config.server_id {
                continue;
            }

            let msg = &RPCMessage {
                body: RPCBody::RequestVoteRequest(RequestVoteRequest {
                    request_id: state.next_request_id(),
                    term: state.durable.current_term,
                    candidate_id: self.config.server_id,
                    last_log_index: std::cmp::max(state.durable.next_log_index, 1) - 1,
                    last_log_term: state.durable.last_log_term,
                }),
                from: self.config.server_id,
            };

            self.rpc_manager.send(
                state.durable.next_log_index,
                state.volatile.condition,
                server.id,
                msg,
            );
        }
    }

    fn apply_entries(&mut self) {
        let mut state = self.state.lock().unwrap();
        let mut to_apply = Vec::<Vec<u8>>::new();
        let starting_index = state.volatile.last_applied + 1;
        while state.volatile.last_applied < state.volatile.commit_index {
            state.volatile.last_applied += 1;
            let last_applied = state.volatile.last_applied;
            to_apply.push(state.durable.log_at_index(last_applied).command);
        }

        if !to_apply.is_empty() {
            let results = self.sm.apply(to_apply);
            for (i, result) in results.into_iter().enumerate() {
                let e = state.durable.log_at_index(starting_index + i as u64);
                if e.client_id == self.client_id {
                    self.apply_sender.send(result).unwrap();
                }
            }

            state.log(format!("Entries applied: {}.", state.volatile.last_applied));
        }
    }

    pub fn init(&mut self) {
        self.restore();

        self.rpc_manager.start();

        let mut state = self.state.lock().unwrap();
        state.reset_election_timeout();
        if self.config.cluster.len() == 1 {
            state.transition(Condition::Leader, 0, 0);
        }
    }

    pub fn stop(&mut self) {
        let mut state = match self.state.lock() {
            Ok(s) => s,
            Err(p) => p.into_inner(),
        };
        state.log("Stopping.");
        // Prevent server from accepting any more log entries.
        if state.volatile.condition != Condition::Follower {
            state.transition(Condition::Follower, 0, 0);
        }
        state.stopped = true;

        // Stop RPCManager.
        let mut stop = self.rpc_manager.stop_mutex.lock().unwrap();
        *stop = true;
        drop(stop);

        // Make an empty connection to self so the stop_mutex logic gets triggered.
        let address = self.rpc_manager.address_from_id(self.config.server_id);
        _ = std::net::TcpStream::connect(address);
    }

    pub fn tick(&mut self) {
        let t1 = Instant::now();
        let state = self.state.lock().unwrap();
        if state.stopped {
            return;
        }
        state.log(format!(
            "Tick start. Log length: {}, commit index: {}. Timeout in: {}ms.",
            state.durable.next_log_index,
            state.volatile.commit_index,
            (state.volatile.election_timeout - Instant::now()).as_millis(),
        ));
        drop(state);

        // Leader operations.
        self.leader_send_heartbeat();
        self.leader_maybe_new_quorum();

        // Follower operations.
        self.follower_maybe_become_candidate();

        // Candidate operations.
        self.candidate_maybe_timeout();

        // All condition operations.
        self.apply_entries();

        // Read from the backlog at least once and for at most 5ms.
        let until = Instant::now() + Duration::from_millis(5);
        loop {
            if let Ok(msg) = self.rpc_manager.stream_receiver.try_recv() {
                let state = self.state.lock().unwrap();

                // "If a server receives a request with a stale term
                // number, it rejects the request." - Reference [0] Page 5.
                // Also: https://github.com/ongardie/raft.tla/blob/974fff7236545912c035ff8041582864449d0ffe/raft.tla#L413.
                let stale = msg.term() < state.durable.current_term;

                state.log(format!(
                    "{} message from {}: {:?}.",
                    if stale { "Dropping stale" } else { "Received" },
                    msg.from,
                    msg.body,
                ));
                drop(state);
                if stale {
                    continue;
                }

                if let Some((response, from)) = self.handle_message(msg) {
                    let state = self.state.lock().unwrap();
                    let condition = state.volatile.condition;
                    let log_length = state.durable.next_log_index;
                    drop(state);

                    self.rpc_manager
                        .send(log_length, condition, from, &response);
                }
            } else {
                break;
            }

            if Instant::now() > until {
                break;
            }
        }

        let took = (Instant::now() - t1).as_millis();
        if took > 0 {
            let state = self.state.lock().unwrap();
            state.log(format!("WARNING! Tick completed in {}ms.", took));
        }
    }

    pub fn restore(&self) {
        let mut state = self.state.lock().unwrap();
        state.durable.restore();
    }

    pub fn new(
        client_id: u128,
        data_directory: &std::path::Path,
        sm: SM,
        config: Config,
    ) -> (Server<SM>, mpsc::Receiver<Vec<u8>>) {
        for server in config.cluster.iter() {
            assert_ne!(server.id, 0);
        }

        // 0 is reserved for control messages.
        assert!(client_id > 0);

        let cluster_size = config.cluster.len();
        let logger = Logger {
            server_id: config.server_id,
            debug: config.logger_debug,
        };
        let rpc_manager = RPCManager::new(config.server_id, config.cluster.clone(), logger.clone());
        let election_frequency = config.election_frequency;

        let rand = Random::new(config.random_seed);
        let id = config.server_id;
        let page_cache_size = config.page_cache_size;

        let (apply_sender, apply_receiver): (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) =
            mpsc::channel();

        (
            Server {
                client_id,

                rpc_manager,
                config,
                sm,

                apply_sender,

                state: Mutex::new(State {
                    durable: DurableState::new(data_directory, id, page_cache_size),
                    volatile: VolatileState::new(cluster_size, election_frequency, rand),
                    logger,
                    stopped: false,
                }),
            },
            apply_receiver,
        )
    }
}

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

        let mut durable = DurableState::new(&tmp.dir, 1, 1);
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

        let mut durable = DurableState::new(&tmp.dir, 1, 1);
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
        let mut durable = DurableState::new(&tmp.dir, 1, 1);
        durable.restore();
        assert_eq!(durable.next_log_index, 1);
        durable.append(&mut v);
        assert_eq!(durable.next_log_index, 3);
        let prev_offset = durable.next_log_offset;
        drop(durable);

        // Start up and restore. Should have three entries.
        let mut durable = DurableState::new(&tmp.dir, 1, 1);
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
        let mut durable = DurableState::new(&tmp.dir, 1, 1);
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
            page_cache_size: 100,
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

struct Random {
    state: [u64; 4],
}

impl Random {
    fn new(seed: [u64; 4]) -> Random {
        Random { state: seed }
    }

    #[allow(dead_code)]
    fn seed() -> [u64; 4] {
        let os = std::env::consts::OS;
        assert!(os == "linux" || os == "macos");

        let mut file = std::fs::File::options()
            .read(true)
            .open("/dev/urandom")
            .unwrap();
        let mut seed = [0; 4];
        let mut bytes = vec![0; 8 * seed.len()];
        file.read_exact(bytes.as_mut_slice()).unwrap();
        for i in 0..seed.len() {
            seed[i] = u64::from_le_bytes(bytes[i * 8..(i + 1) * 8].try_into().unwrap());
        }
        seed
    }

    // Port of https://prng.di.unimi.it/xoshiro256plusplus.c.
    fn next(&mut self) -> u64 {
        let result: u64 = self.state[0].wrapping_add(self.state[3]);

        let t: u64 = self.state[1] << 17;

        self.state[2] ^= self.state[0];
        self.state[3] ^= self.state[1];
        self.state[1] ^= self.state[2];
        self.state[0] ^= self.state[3];

        self.state[2] ^= t;

        self.state[3] = self.state[3].rotate_left(45);

        result
    }

    fn generate_u64(&mut self) -> u64 {
        self.next()
    }

    fn generate_bool(&mut self) -> bool {
        self.generate_f64() < 0.0
    }

    fn generate_f64(&mut self) -> f64 {
        let i = self.next();

        // Reinterpret integer bytes as f64 bytes.
        f64::from_le_bytes(i.to_le_bytes())
    }

    fn generate_u32(&mut self) -> u32 {
        let i = self.next();

        // Reinterpret integer bytes as u32 bytes.
        u32::from_le_bytes(i.to_le_bytes()[0..4].try_into().unwrap())
    }

    fn generate_percent(&mut self) -> f32 {
        let u = self.generate_u32();
        (u as f64 / u32::MAX as f64) as f32
    }

    #[allow(dead_code)]
    fn generate_seed(&mut self) -> [u64; 4] {
        [self.next(), self.next(), self.next(), self.next()]
    }
}

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

        let page_cache_size = match std::env::var("PAGECACHE") {
            Ok(var) => match var.parse::<usize>() {
                Ok(size) => size,
                _ => 1000,
            },
            _ => 1000,
        };

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
                page_cache_size,
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

        const BATCHES: usize = 100;
        const BATCH_SIZE: usize = 1000;
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
        for i in 0..BATCHES {
            let mut batch = vec![vec![]; BATCH_SIZE];
            for j in 0..BATCH_SIZE {
                batch[j] = vec![];
                for k in 0..INNER_BATCH {
                    let msg = i * BATCH_SIZE + j * BATCH_SIZE + k;
                    batch[j].extend(msg.to_le_bytes().to_vec());
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
                for _ in 0..INNER_BATCH {
                    expected_msg.extend(match_index.to_le_bytes().to_vec());
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
