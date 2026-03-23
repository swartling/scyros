use std::collections::{HashMap, HashSet};

pub struct CandidateEntry {
    pub matches: usize,
    pub length: usize,
    pub last_token_seen_pos: usize,
}

pub struct CandidateMap {
    entries: HashMap<blake3::Hash, CandidateEntry>,
    match_histogram: HashMap<usize, HashSet<blake3::Hash>>,
}

impl Default for CandidateMap {
    fn default() -> Self {
        CandidateMap::new()
    }
}

impl CandidateMap {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            match_histogram: HashMap::new(),
        }
    }

    pub fn add_candidate(
        &mut self,
        function_id: blake3::Hash,
        length: usize,
        new_matches: usize,
        last_token_seen_pos: usize,
    ) {
        let entry = self.entries.entry(function_id).or_insert(CandidateEntry {
            matches: 0,
            length,
            last_token_seen_pos,
        });

        // Update the match histogram
        if entry.matches > 0 {
            if let Some(bucket) = self.match_histogram.get_mut(&entry.matches) {
                bucket.remove(&function_id);
            }
        }

        entry.matches += new_matches;
        entry.length = length;
        entry.last_token_seen_pos = last_token_seen_pos;

        self.match_histogram
            .entry(entry.matches)
            .or_default()
            .insert(function_id);
    }

    pub fn count_candidates_with_n_matches(&self, n: usize, mode: &str) -> usize {
        if mode == "exact" {
            self.match_histogram
                .get(&n)
                .map(|bucket| bucket.len())
                .unwrap_or(0)
        } else if mode == "at_least" {
            self.match_histogram
                .iter()
                .filter(|(&matches, _)| matches >= n)
                .map(|(_, bucket)| bucket.len())
                .sum()
        } else {
            panic!("Invalid mode: {}", mode);
        }
    }
}
