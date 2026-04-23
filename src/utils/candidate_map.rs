use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

pub struct CandidateEntry {
    pub matches: usize,
    pub length: usize,
    pub last_token_seen_pos: (usize, usize), // (token_position, cumulative_count)
}

pub struct CandidateMap {
    entries: HashMap<blake3::Hash, CandidateEntry>,
    match_histogram: HashMap<usize, HashSet<blake3::Hash>>,
    pending_updates: Vec<(blake3::Hash, usize, (usize, usize))>, // (function_id, new_matches, last_token_seen_pos)
    min_length: usize,
    max_length: usize,
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
            min_length: usize::MAX,
            max_length: 0,
            pending_updates: Vec::new(),
        }
    }

    pub fn get_token_matches(&self, function_id: &blake3::Hash) -> usize {
        self.entries
            .get(function_id)
            .map(|entry| entry.matches)
            .unwrap_or(0)
    }

    pub fn add_pending_update(
        &mut self,
        function_id: blake3::Hash,
        new_matches: usize,
        last_token_seen_pos: (usize, usize),
    ) {
        self.pending_updates
            .push((function_id, new_matches, last_token_seen_pos));
    }

    pub fn apply_pending_updates(
        &mut self,
        function_paths_and_lengths: &HashMap<blake3::Hash, (&str, usize)>,
    ) {
        let updates = self.pending_updates.drain(..).collect::<Vec<_>>();
        for (function_id, new_matches, last_token_seen_pos) in updates {
            self.add_candidate(
                function_id,
                function_paths_and_lengths,
                new_matches,
                last_token_seen_pos,
            );
        }
    }

    pub fn add_candidate(
        &mut self,
        function_id: blake3::Hash,
        function_paths_and_lengths: &std::collections::HashMap<blake3::Hash, (&str, usize)>,
        new_matches: usize,
        last_token_seen_pos: (usize, usize),
    ) {
        let entry = match self.entries.entry(function_id) {
            Entry::Occupied(occupied) => occupied.into_mut(),
            Entry::Vacant(vacant) => {
                let length = function_paths_and_lengths
                    .get(&function_id)
                    .map(|(_, count)| *count)
                    .unwrap_or(0);
                let last_token_seen_pos = (0, 0); // Initialize to (0, 0) for new candidates
                self.min_length = self.min_length.min(length);
                self.max_length = self.max_length.max(length);
                vacant.insert(CandidateEntry {
                    matches: 0,
                    length,
                    last_token_seen_pos,
                })
            }
        };

        // Update the match histogram
        if entry.matches > 0 {
            if let Some(bucket) = self.match_histogram.get_mut(&entry.matches) {
                bucket.remove(&function_id);
            }
        }

        entry.matches += new_matches;
        entry.last_token_seen_pos = last_token_seen_pos;
        self.match_histogram
            .entry(entry.matches)
            .or_default()
            .insert(function_id);
    }

    pub fn length_range(&self) -> Option<(usize, usize)> {
        if self.entries.is_empty() {
            None
        } else {
            Some((self.min_length, self.max_length))
        }
    }

    pub fn get_candidates_with_n_matches(&self, n: usize, mode: &str) -> HashSet<blake3::Hash> {
        if mode == "exact" {
            self.match_histogram.get(&n).cloned().unwrap_or_default()
        } else if mode == "at_least" {
            self.match_histogram
                .iter()
                .filter(|(&matches, _)| matches >= n)
                .flat_map(|(_, bucket)| bucket.clone())
                .collect()
        } else {
            panic!("Invalid mode: {}", mode);
        }
    }

    pub fn get_last_token_seen_pos(&self, function_id: &blake3::Hash) -> (usize, usize) {
        self.entries
            .get(function_id)
            .map(|entry| entry.last_token_seen_pos)
            .unwrap_or((0, 0))
    }

    pub fn update_last_token_seen_pos(
        &mut self,
        function_id: &blake3::Hash,
        new_pos: (usize, usize),
    ) {
        if let Some(entry) = self.entries.get_mut(function_id) {
            entry.last_token_seen_pos = new_pos;
        }
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

    pub fn verification_cost_estimate(&self, n: usize, origin_word_count: &usize) -> usize {
        let mut number_of_candidates = self.count_candidates_with_n_matches(n, "at_least"); //the candidates that have already reached n matches

        let mut survivors = 0usize;
        for candidate in &self.pending_updates {
            let function_id = candidate.0;
            let current_matches = self.get_token_matches(&function_id);
            if n > 1 && current_matches == n - 1 {
                // if n==1 the pending list is empty as they have already been applied
                survivors += 1;
            }
        }
        number_of_candidates += survivors; //add the candidates that are about to reach n matches
                                           // I am disregarding the candidates with less than n-1 matches that will also reach n_matches due to new_matches>1
                                           // But as I understand it they should always satisfy property 1
                                           // A candidate doesn't get to come back after being eliminated once
                                           // Also it's a very rare edge case
        let length_range = self.length_range().unwrap_or((usize::MAX, 0));
        let average_length = if length_range.0 == usize::MAX {
            0
        } else {
            (length_range.0 + length_range.1) / 2
        };
        number_of_candidates * (*origin_word_count + average_length)
    }
}
