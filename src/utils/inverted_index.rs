use blake3::Hash;
use std::collections::HashMap;
pub struct InvertedIndex {
    map: HashMap<Vec<u8>, Vec<(Hash, usize, (usize, usize))>>, // token -> Vec<(function_id, count, (token_position, cumulative_count))>
}

impl Default for InvertedIndex {
    fn default() -> Self {
        InvertedIndex::new()
    }
}

impl InvertedIndex {
    pub fn new() -> Self {
        InvertedIndex {
            map: HashMap::default(),
        }
    }

    pub fn add(
        &mut self,
        token: &Vec<u8>,
        function_id: Hash,
        count: usize,
        token_position: usize,
        cumulative_count: usize,
    ) {
        //token_position is the index of the token.
        // cumulative_count is the number of words seen up to and including this token including duplicates
        self.map.entry(token.to_owned()).or_default().push((
            function_id,
            count,
            (token_position, cumulative_count),
        ));
    }

    pub fn get(&self, token: &Vec<u8>) -> Option<&Vec<(Hash, usize, (usize, usize))>> {
        self.map.get(token)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn len_tokens(&self) -> usize {
        self.map.values().map(|v| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn token_frequency(&self, token: &Vec<u8>, count_duplicates: bool) -> usize {
        if let Some(functions) = self.get(token) {
            if count_duplicates {
                functions.iter().map(|(_, count, _)| *count).sum()
            } else {
                functions.len()
            }
        } else {
            0
        }
    }
}
