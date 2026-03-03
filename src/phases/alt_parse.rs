// Copyright 2025 Andrea Gilot
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Parse all the files in the input file and extract the functions whose body contains one of the provided keywords.
//! All parsed files repositories are logged in a CSV file where statistics about the functions are stored.
//! These statistics include the number of lines of code, the number of words, the number of keywords matched, the number of conditional statements, loops,
//! and the maximum nesting level of these statements.
//! The name of the log file is the same as the input file with the extension `.functions`.
//! The functions are stored in a folder with the same name as the file and the extension `_functions`.
//! The supported languages are C, C++, Java, Python and Fortran.

use clap::ArgAction;
use clap::{Arg, Command};
use indicatif::ProgressBar;
use polars::prelude::*;
use rand::rngs::StdRng;
use rand::seq::SliceRandom as _;
use rand::SeedableRng;

use std::iter::FromIterator as _;
use std::vec;
use std::{collections::HashSet, fmt::Write, io::Write as IOWrite, sync::Mutex};
use tree_sitter::{Language, Node, Parser, Tree};

use crate::utils::error::*;
use crate::utils::fs::*;
use crate::utils::regex::*;
use crate::utils::{csv::*, logger::Logger};

/// Command line arguments parsing.
pub fn cli() -> Command {
    Command::new("alt_parse")
        .about("Parse all the files in the dataset and extract functions")
        .long_about(
            "Parse all the files in the input file and extract functions \
            All parsed files repositories are logged in a CSV file where statistics about the functions are stored. \
            These statistics include the number of lines of code, the number of words, the number of keywords matched, the number of conditional statements, loops,
            and the maximum nesting level of these statements.\n\
            The name of the log file is the same as the input file with the extension \".functions\". \
            The functions are stored in a folder with the same name as the file and the extension \"_functions\".\n\
            The supported languages are C, C++, Java, Python and Fortran."
        )
        .disable_version_flag(true)
        .arg(
            Arg::new("input")
                .short('i')
                .long("input")
                .value_name("INPUT_FILE.csv")
                .help("Path to the input csv file to use. It must be a valid CSV file where the first column is the path to the file and the \
                       second column is the extension of the file. Other columns are ignored.")
                .required(true)
        )
        .arg(
            Arg::new("output")
                .short('o')
                .long("output")
                .value_name("OUTPUT_FILE.csv")
                .help("Path to the output csv file storing the functions statistics.")
                .required(false),
        )
        .arg(
            Arg::new("logs")
                .short('l')
                .long("logs")
                .value_name("LOGS_FOLDER")
                .help("Path to the folder where the logs are stored. The default is the current folder.")
                .required(false),
        )
/*         .arg(
            Arg::new("keywords")
                .short('k')
                .long("keywords")
                .num_args(1..)
                .action(ArgAction::Append)
                .value_name("KEYWORDS_FILES.json")
                .help("List of files containing the list of extensions and keywords to use. The files must be in JSON format.\n\
                    The extensions should be written without the period (`java` instead of `.java`). The files must have the following structure:\n    \
                        {\n        \
                            \"extensions\": {\n            \
                                \"ext1\": [\"kw11\", \"kw12\", ...],\n            \
                                \"ext2\": [\"kw21\", \"kw22\", ...],\n            \
                                ...\n        \
                            },\n        \
                            \"keywords\": [\"kw1\", \"kw2\", ...]\n    \
                        }")
                .required(true)
        ) */
        .arg(
            Arg::new("lang")
                .long("lang")
                .num_args(1..)
                .action(ArgAction::Append)
                .value_name("LANGUAGES")
                .help("List of languages to parse. The supported languages are C, C++, C#, Fortran, Go, Java, Python and Typescript.")
                .required(false)
        )
        .arg(
            Arg::new("force")
                .short('f')
                .long("force")
                .help("Override the output file if it already exists.")
                .default_value("false")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("threads")
                .short('n')
                .help("Number of threads to use.")
                .default_value("1")
                .value_parser(clap::value_parser!(usize))
        )
        .arg(
            Arg::new("seed")
                .short('s')
                .long("seed")
                .value_name("SEED")
                .help("Seed used to randomly shuffle the input file.")
                .default_value("8155495201244430235")
                .value_parser(clap::value_parser!(u64)),
        )
        .arg(
            Arg::new("failures")
            .long("failures")
            .value_name("POLICY")
            .help("Failure policy when a file or a function has a parsing error.\n\
            ignore: continue parsing\n\
            skip-file: replace the file statistics with an error row in the output file, does not extract any function from the file\n\
            skip-function: replace the function statistics with an error row in the output file\n\
            abort: stop the program")
            .default_value("ignore")
            .value_parser(["ignore", "skip-file", "skip-function", "abort"]),
        )
}

/// Simplified version that extracts all functions without caring about keywords or complex metrics.
/// This version only extracts function definitions and basic statistics.
///
/// # Arguments
///
/// * `input_file` - Path to the input csv file to use.
/// * `output_file` - Path to the output csv file storing the functions statistics.
/// * `logs_file` - Path to the output csv file storing the files statistics.
/// * `threads` - Number of threads to use.
///
/// All functions are extracted and logged in a CSV file with basic statistics only:
/// id, path, name, position, language, loc, words, params, parse_error
///
pub fn run(
    input_path: &str,
    output_path: Option<&str>,
    logs_path: Option<&str>,
    opt_languages: Option<Vec<&str>>,
    fail_policy: &str,
    threads: usize,
    seed: u64,
    force: bool,
    logger: &mut Logger,
) -> Result<(), Error> {
    let supported_languages: HashSet<&'static str> = vec![
        "c",
        "c++",
        "c#",
        "java",
        "python",
        "fortran",
        "typescript",
        "go",
        "scala",
    ]
    .into_iter()
    .collect::<HashSet<_>>();

    let languages: Vec<&str> = match opt_languages {
        Some(l) => {
            for lang in l.iter() {
                if !supported_languages.contains(lang) {
                    Error::new(&format!("Unsupported language: {}", lang)).to_res()?;
                }
            }
            l
        }
        None => {
            logger.log("No language specified, using all supported languages")?;
            supported_languages.into_iter().collect()
        }
    };

    let languages_series = Series::new(
        "language_filter".into(),
        languages
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>(),
    );

    let default_output_path: String = format!("{}.functions_simple.csv", input_path);
    let output_path: &str = output_path.unwrap_or(&default_output_path);

    match check_path(output_path) {
        Ok(_) => {
            if force {
                logger.log(&format!("Overriding existing file: {}", output_path))?;
            } else {
                Error::new(&format!(
                    "File {} already exists. Use --force to override it.",
                    output_path
                ))
                .to_res()?;
            }
        }
        Err(_) => logger.log(&format!("Creating new file: {}", output_path))?,
    }

    let default_logs_path: String = format!("{}.function_logs_simple.csv", input_path);
    let logs_path: &str = logs_path.unwrap_or(&default_logs_path);

    match check_path(logs_path) {
        Ok(_) => {
            if force {
                logger.log(&format!("Overriding existing file: {}", logs_path))?;
            } else {
                Error::new(&format!(
                    "File {} already exists. Use --force to override it.",
                    logs_path
                ))
                .to_res()?;
            }
        }
        Err(_) => logger.log(&format!("Creating new file: {}", logs_path))?,
    }

    let mut input_file = open_csv(
        input_path,
        Some(Schema::from_iter(vec![
            Field::new("id".into(), DataType::UInt32),
            Field::new("name".into(), DataType::String),
            Field::new("language".into(), DataType::String),
        ])),
        Some(vec!["id", "name", "language"]),
    )?;

    let n_files_before = input_file.height();

    logger.log(&format!(
        "  {} files found in the input file, filtering by selected languages",
        n_files_before
    ))?;

    input_file = map_err(
        input_file
            .lazy()
            .filter(col("language").is_in(lit(languages_series)))
            .collect(),
        "Error filtering languages",
    )?;

    let n_files = input_file.height();

    logger.log(&format!(
        "  {} files found after filtering ({:.2} %)",
        n_files,
        if n_files_before == 0 {
            0
        } else {
            n_files / n_files_before * 100
        }
    ))?;

    logger.log_seed(seed)?;

    let mut shuffled_idx = (0..input_file.height()).collect::<Vec<usize>>();

    logger.log_completion("Loading files in random order", || {
        let mut rng: StdRng = SeedableRng::seed_from_u64(seed);
        shuffled_idx.shuffle(&mut rng);
        Ok(())
    })?;

    let shuffled_rows = shuffled_idx.into_iter().map(|idx| {
        let row = input_file.get_row(idx).unwrap().0;
        match (row[0].clone(), row[1].clone(), row[2].clone()) {
            (AnyValue::UInt32(id), AnyValue::String(path), AnyValue::String(lang)) => Ok((
                id,
                path.replace("-was_comma-", ",")
                    .replace("-was_quote-", "\""),
                lang,
            )),
            _ => Err(idx),
        }
    });

    const OUTPUT_COLS: usize = 9;
    const LOGS_COLS: usize = 5;

    let word_counter: Matcher = Matcher::words_matcher();

    let mut output_file = CSVFile::new(output_path, FileMode::Overwrite)?;

    let header: [&str; OUTPUT_COLS] = [
        "id",
        "path",
        "name",
        "position",
        "language",
        "loc",
        "words",
        "params",
        "parse_error",
    ];

    output_file.write_header(&header)?;

    let mut logs_file = CSVFile::new(logs_path, FileMode::Overwrite)?;

    let logs_header: [&str; LOGS_COLS] = ["id", "name", "language", "functions", "parse_error"];

    logs_file.write_header(&logs_header)?;

    let iter = Mutex::new(shuffled_rows.into_iter());

    let (tx, rx) =
        crossbeam_channel::unbounded::<Option<Result<(String, Option<String>), Error>>>();

    map_err_debug(
        crossbeam::thread::scope(|s| {
            for _ in 0..threads {
                s.spawn(|_| {
                    let my_tx = tx.clone();
                    loop {
                        let next_item: Option<Result<(u32, String, &str), usize>> = {
                            let mut iter_guard = iter.lock().unwrap();
                            iter_guard.next()
                        };

                        match next_item {
                            Some(row) => match row {
                                Ok((project_id, file_name, language)) => match analyze_file_simple(
                                    project_id,
                                    &file_name,
                                    language,
                                    fail_policy,
                                    &word_counter,
                                ) {
                                    Ok(s) => {
                                        my_tx.send(Some(Ok(s))).unwrap();
                                    }
                                    Err(e) => {
                                        my_tx.send(Some(e.to_res())).unwrap();
                                        break;
                                    }
                                },
                                Err(row_nr) => {
                                    let _ = my_tx.send(Some(
                                        Error::new(&format!("Could not parse row {}", row_nr))
                                            .to_res(),
                                    ));
                                }
                            },
                            None => {
                                my_tx.send(None).unwrap();
                                break;
                            }
                        }
                    }
                });
            }

            let mut ended_threads = 0;

            let progress = ProgressBar::new(n_files as u64);
            progress.set_style(map_err(
                indicatif::ProgressStyle::default_bar().template("{elapsed} {wide_bar} {percent}%"),
                "Invalid progress bar style",
            )?);

            while let Ok(msg) = rx.recv() {
                match msg {
                    Some(Ok((output, opt_log))) => {
                        map_err(
                            write!(&mut output_file, "{}", output),
                            &format!("Error writing {}", output_path),
                        )?;
                        if let Some(log) = opt_log {
                            map_err(
                                writeln!(&mut logs_file, "{}", log),
                                &format!("Error writing {}", logs_path),
                            )?;
                        }
                        progress.inc(1);
                    }
                    Some(Err(e)) => {
                        e.chain("Error in child thread").to_res::<()>()?;
                    }
                    None => {
                        ended_threads += 1;
                        if ended_threads == threads {
                            break;
                        }
                    }
                }
            }
            progress.finish();
            Ok(())
        }),
        "Error in one of the threads",
    )?
}

/// Simplified version of analyze_file that extracts all functions without keyword filtering.
fn analyze_file_simple(
    project_id: u32,
    path: &str,
    language: &str,
    fail_policy: &str,
    word_counter: &Matcher,
) -> Result<(String, Option<String>), Error> {
    match language_to_grammar(language) {
        Some(grammar) => {
            let mut parser: Parser = Parser::new();
            map_err(parser.set_language(&grammar.lang), "Cannot load grammar")?;
            match load_file(path, 1024 * 1024 * 1024)? {
                Ok(source_code) => {
                    let target_folder: String = format!("{}.functions_simple", path);
                    create_dir(&target_folder)?;

                    let tree: Tree = ok_or_else(
                        parser.parse(&source_code, None),
                        &format!("Error parsing file {}", path),
                    )?;

                    let file_has_parse_error: bool = tree.root_node().has_error();

                    if file_has_parse_error && fail_policy == "skip-file" {
                        Ok((String::new(), None))
                    } else if file_has_parse_error && fail_policy == "abort" {
                        Error::new(&format!("Parse error in file {}", path)).to_res()
                    } else {
                        let root: Node<'_> = tree.root_node();
                        let (output, total_functions) = extract_functions_simple(
                            project_id,
                            &root,
                            &target_folder,
                            language,
                            &grammar,
                            &source_code,
                            fail_policy,
                            word_counter,
                        )?;

                        let error_position: String = if file_has_parse_error {
                            position_to_string(find_first_error_position(&root))
                        } else {
                            "none".to_string()
                        };

                        Ok((
                            output,
                            Some(format!(
                                "{},{},{},{},{}",
                                project_id,
                                path.replace(",", "-was_comma-")
                                    .replace("\"", "-was_quote-"),
                                language,
                                total_functions,
                                error_position,
                            )),
                        ))
                    }
                }

                Err(_) => Ok((
                    String::new(),
                    Some(format!(
                        "{},{},{},-1,{}",
                        project_id,
                        path.replace(",", "-was_comma-")
                            .replace("\"", "-was_quote-"),
                        language,
                        "none",
                    )),
                )),
            }
        }
        None => Error::new(&format!("Unsupported language: {}", language)).to_res(),
    }
}

/// Simplified version of extract_functions that extracts all functions without keyword filtering.
fn extract_functions_simple(
    project_id: u32,
    root: &Node,
    target_folder: &str,
    language: &str,
    grammar: &Grammar,
    source: &[u8],
    fail_policy: &str,
    word_counter: &Matcher,
) -> Result<(String, usize), Error> {
    let mut builder: String = String::new();
    let mut functions: usize = 0;

    let mut call_stack: Vec<Node> = Vec::new();
    call_stack.push(*root);
    let mut cursor = root.walk();

    while let Some(node) = call_stack.pop() {
        if grammar.function_nodes.contains(node.kind()) {
            let has_error: bool = node.has_error();

            if (has_error && fail_policy == "skip-function")
                || (language == "java" && find_fields(&node, "body").is_empty())
            {
                continue;
            } else {
                let function_source_code: &[u8] = node_source_code(&node, source);
                let function_position: (usize, usize) = (
                    node.start_position().row + 1,
                    node.start_position().column + 1,
                );

                let error_position: String = if has_error {
                    position_to_string(find_first_error_position(&node).map(|(row, col)| {
                        let error_row = row - function_position.0 + 1;
                        if row == function_position.0 {
                            (error_row, col - function_position.1 + 1)
                        } else {
                            (error_row, col)
                        }
                    }))
                } else {
                    "none".to_string()
                };

                let function_code_with_strings: &Vec<u8> =
                    &remove_kind_from_source(function_source_code, &node, &grammar.comment_nodes);
                /*
                let tree_without_comments: Tree = ok_or_else(
                    parser.parse(function_code_with_strings, None),
                    &format!(
                        "Error parsing code for function {}/{}",
                        target_folder, functions
                    ),
                )?;

                let function_code = &remove_kind_from_source(
                    function_code_with_strings,
                    &tree_without_comments.root_node(),
                    &grammar.string_literal_nodes,
                ); */

                let function_path: String = format!(
                    "{}/{}-{}",
                    target_folder, function_position.0, function_position.1
                );

                map_err(
                    std::fs::write(&function_path, function_source_code),
                    &format!("Cannot write function code to {}", function_path),
                )?;

                let params_vec: Vec<Node<'_>> =
                    find_first_node_of_kind(&node, &grammar.param_seq_nodes, true);

                let mut name: String = String::from_utf8_lossy(
                    find_first_field(&node, grammar.name_field)
                        .map(|n| node_source_code(&n, source))
                        .unwrap_or(b""),
                )
                .to_string();
                if let Some(idx) = name.find('(') {
                    name.truncate(idx);
                }
                name = name.chars().filter(|c| !c.is_whitespace()).collect();

                let mut n_param: usize = 0;
                for params in params_vec {
                    n_param += count_nodes_of_kind(&params, &grammar.param_nodes).0;
                }

                map_err(
                    writeln!(
                        &mut builder,
                        "{},{},{},{},{},{},{},{},{}",
                        project_id,
                        &function_path
                            .replace(",", "-was_comma-")
                            .replace("\"", "-was_quote-"),
                        name.replace(",", "-was_comma-")
                            .replace("\"", "-was_quote-"),
                        position_to_string(Some(function_position)),
                        language,
                        count_text_lines(function_code_with_strings),
                        word_counter.count_matches_in_text(function_code_with_strings),
                        n_param,
                        error_position,
                    ),
                    &format!("Error writing function statistics of {}", function_path),
                )?;

                functions += 1;
            }
        } else {
            for c in node
                .children(&mut cursor)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
            {
                call_stack.push(c);
            }
        }
    }
    Ok((builder, functions))
}

/// Returns the source code of a node in the parse tree
///
/// # Arguments
///
/// * `n` - The node to extract the source code from.
/// * `source` - The source code of the whole file.
fn node_source_code<'a>(n: &Node, source: &'a [u8]) -> &'a [u8] {
    &source[n.start_byte()..n.end_byte()]
}

/// Grammar of a programming language.
#[allow(dead_code)]
struct Grammar {
    /// The programming language the grammar belongs to.
    lang: Language,

    /// Nodes representing comments.
    comment_nodes: HashSet<&'static str>,

    /// Nodes representing string literals.
    string_literal_nodes: HashSet<&'static str>,

    /// Nodes representing loops.
    loop_nodes: HashSet<&'static str>,

    /// Nodes representing conditional statements.
    cond_nodes: HashSet<&'static str>,

    /// Nodes representing functions or methods.
    function_nodes: HashSet<&'static str>,

    /// Nodes representing function or method calls.
    function_call_nodes: HashSet<&'static str>,

    /// Nodes representing a sequence of parameters of a function or method.  
    param_seq_nodes: HashSet<&'static str>,

    /// Nodes representing a parameter of a function or method.
    param_nodes: HashSet<&'static str>,

    /// The field name of the parameter type.
    param_type_field: Option<&'static str>,

    /// The field name of the function or method name.
    name_field: &'static str,
}

/// Returns the grammar for the C programming language.
fn c_grammar() -> Grammar {
    Grammar {
        lang: tree_sitter_c::LANGUAGE.into(),
        comment_nodes: vec!["comment"].into_iter().collect(),
        string_literal_nodes: vec!["string_literal"].into_iter().collect(),
        loop_nodes: vec!["for_statement", "while_statement", "do_statement"]
            .into_iter()
            .collect(),
        cond_nodes: vec!["if_statement", "switch_statement", "conditional_expression"]
            .into_iter()
            .collect(),
        function_nodes: vec!["function_definition"].into_iter().collect(),
        function_call_nodes: vec!["call_expression"].into_iter().collect(),
        param_seq_nodes: vec!["parameter_list"].into_iter().collect(),
        param_nodes: vec!["parameter_declaration"].into_iter().collect(),
        param_type_field: Some("type"),
        name_field: "declarator",
    }
}

/// Returns the grammar for the C++ programming language.
fn cpp_grammar() -> Grammar {
    Grammar {
        lang: tree_sitter_cpp::LANGUAGE.into(),
        comment_nodes: vec!["comment"].into_iter().collect(),
        string_literal_nodes: vec!["string_literal"].into_iter().collect(),
        loop_nodes: vec!["for_range_loop", "for_statement", "while_statement"]
            .into_iter()
            .collect(),
        cond_nodes: vec!["if_statement", "switch_statement", "conditional_expression"]
            .into_iter()
            .collect(),
        function_nodes: vec!["function_definition", "template_declaration"]
            .into_iter()
            .collect(),
        function_call_nodes: vec!["call_expression"].into_iter().collect(),
        param_seq_nodes: vec!["parameter_list"].into_iter().collect(),
        param_nodes: vec!["parameter_declaration", "variadic_parameter_declaration"]
            .into_iter()
            .collect(),
        param_type_field: Some("type"),
        name_field: "declarator",
    }
}

/// Returns the grammar for the C# programming language.
fn cs_grammar() -> Grammar {
    Grammar {
        lang: tree_sitter_c_sharp::LANGUAGE.into(),
        comment_nodes: vec!["comment"].into_iter().collect(),
        string_literal_nodes: vec![
            "string_literal",
            "verbatim_string_literal",
            "raw_string_literal",
        ]
        .into_iter()
        .collect(),
        loop_nodes: vec!["for_statement", "while_statement", "do_statement"]
            .into_iter()
            .collect(),
        cond_nodes: vec!["if_statement", "switch_statement", "conditional_expression"]
            .into_iter()
            .collect(),
        function_nodes: vec![
            "method_declaration",
            "constructor_declaration",
            "operator_declaration",
        ]
        .into_iter()
        .collect(),
        function_call_nodes: vec!["invocation_expression"].into_iter().collect(),
        param_seq_nodes: vec!["parameter_list"].into_iter().collect(),
        param_nodes: vec!["parameter"].into_iter().collect(),
        param_type_field: Some("type"),
        name_field: "name",
    }
}

/// Returns the grammar for the TypeScript programming language.
fn ts_grammar() -> Grammar {
    Grammar {
        lang: tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
        comment_nodes: vec!["comment"].into_iter().collect(),
        string_literal_nodes: vec!["string_fragment"].into_iter().collect(),
        loop_nodes: vec!["for_statement", "for_in_statement", "while_statement"]
            .into_iter()
            .collect(),
        cond_nodes: vec!["if_statement", "switch_statement", "ternary_expression"]
            .into_iter()
            .collect(),
        function_nodes: vec!["function_declaration", "method_definition"]
            .into_iter()
            .collect(),
        function_call_nodes: vec![
            "new_expression",
            "call_expression",
            "decorator_call_expression",
        ]
        .into_iter()
        .collect(),
        param_seq_nodes: vec!["formal_parameters"].into_iter().collect(),
        param_nodes: vec!["required_parameter", "optional_parameter"]
            .into_iter()
            .collect(),
        param_type_field: Some("type"),
        name_field: "name",
    }
}

/// Returns the grammar for the Go programming language.
fn go_grammar() -> Grammar {
    Grammar {
        lang: tree_sitter_go::LANGUAGE.into(),
        comment_nodes: vec!["comment"].into_iter().collect(),
        string_literal_nodes: vec!["raw_string_literal", "interpreted_string_literal"]
            .into_iter()
            .collect(),
        loop_nodes: vec!["for_statement"].into_iter().collect(),
        cond_nodes: vec![
            "if_statement",
            "type_switch_statement",
            "expression_switch_statement",
        ]
        .into_iter()
        .collect(),
        function_nodes: vec!["function_declaration", "method_declaration"]
            .into_iter()
            .collect(),
        function_call_nodes: vec!["call_expression"].into_iter().collect(),
        param_seq_nodes: vec!["parameter_list"].into_iter().collect(),
        param_nodes: vec!["parameter_declaration", "variadic_parameter_declaration"]
            .into_iter()
            .collect(),
        param_type_field: Some("type"),
        name_field: "name",
    }
}

/// Returns the grammar for the Java programming language.
fn java_grammar() -> Grammar {
    Grammar {
        lang: tree_sitter_java::LANGUAGE.into(),
        comment_nodes: vec!["line_comment", "block_comment"].into_iter().collect(),
        string_literal_nodes: vec!["string_literal"].into_iter().collect(),
        loop_nodes: vec![
            "for_statement",
            "enhanced_for_statement",
            "while_statement",
            "do_statement",
        ]
        .into_iter()
        .collect(),
        cond_nodes: vec!["if_statement", "ternary_expression", "switch_expression"]
            .into_iter()
            .collect(),
        function_nodes: vec!["method_declaration", "compact_constructor_declaration"]
            .into_iter()
            .collect(),
        function_call_nodes: vec!["method_invocation", "explicit_constructor_invocation"]
            .into_iter()
            .collect(),
        param_seq_nodes: vec!["formal_parameters"].into_iter().collect(),
        param_nodes: vec!["formal_parameter"].into_iter().collect(),
        param_type_field: Some("type"),
        name_field: "name",
    }
}

/// Returns the grammar for the Scala programming language.
fn scala_grammar() -> Grammar {
    Grammar {
        lang: tree_sitter_scala::LANGUAGE.into(),
        comment_nodes: vec!["comment", "block_comment"].into_iter().collect(),
        string_literal_nodes: vec!["string"].into_iter().collect(),
        loop_nodes: vec!["for_expression", "while_expression", "do_while_expression"]
            .into_iter()
            .collect(),
        cond_nodes: vec!["if_expression", "match_expression"]
            .into_iter()
            .collect(),
        function_nodes: vec!["function_definition"].into_iter().collect(),
        function_call_nodes: vec!["call_expression"].into_iter().collect(),
        param_seq_nodes: vec!["parameters"].into_iter().collect(),
        param_nodes: vec!["parameter"].into_iter().collect(),
        param_type_field: Some("type"),
        name_field: "name",
    }
}

/// Returns the grammar for the Fortran programming language.
fn fortran_grammar() -> Grammar {
    Grammar {
        lang: tree_sitter_fortran::LANGUAGE.into(),
        comment_nodes: vec!["preproc_comment", "comment"].into_iter().collect(),
        string_literal_nodes: vec!["string_literal"].into_iter().collect(),
        loop_nodes: vec![
            "loop_control_expression",
            "where_statement",
            "forall_statement",
            "concurrent_statement",
            "while_statement",
        ]
        .into_iter()
        .collect(),
        cond_nodes: vec![
            "if_statement",
            "arithmetic_if_statement",
            "select_case_statement",
            "select_rank_statement",
            "select_type_statement",
        ]
        .into_iter()
        .collect(),
        function_nodes: vec!["function", "subroutine"].into_iter().collect(),
        function_call_nodes: vec!["call_expression", "subroutine_call"]
            .into_iter()
            .collect(),
        param_seq_nodes: vec!["parameters"].into_iter().collect(),
        param_nodes: vec!["identifier"].into_iter().collect(),
        param_type_field: None,
        name_field: "name",
    }
}

/// Returns the grammar for the Python programming language.
fn python_grammar() -> Grammar {
    Grammar {
        lang: tree_sitter_python::LANGUAGE.into(),
        comment_nodes: vec!["comment"].into_iter().collect(),
        string_literal_nodes: vec!["string"].into_iter().collect(),
        loop_nodes: vec!["for_statement", "while_statement"]
            .into_iter()
            .collect(),
        cond_nodes: vec!["if_statement", "conditional_expression", "match_statement"]
            .into_iter()
            .collect(),
        function_nodes: vec!["function_definition", "lambda"].into_iter().collect(),
        function_call_nodes: vec!["call"].into_iter().collect(),
        param_seq_nodes: vec!["parameters"].into_iter().collect(),
        param_nodes: vec!["parameter"].into_iter().collect(),
        param_type_field: None,
        name_field: "name",
    }
}

/// Returns the grammar corresponding to the given language.
///
/// # Arguments
///
/// * `language` - The language of the file.
///
/// # Returns
///
/// The grammar corresponding to the language or `None` if the language is not supported.
fn language_to_grammar(lang: &str) -> Option<Grammar> {
    match lang {
        "c" => Some(c_grammar()),
        "c++" => Some(cpp_grammar()),
        "c#" => Some(cs_grammar()),
        "java" => Some(java_grammar()),
        "fortran" => Some(fortran_grammar()),
        "python" => Some(python_grammar()),
        "typescript" => Some(ts_grammar()),
        "go" => Some(go_grammar()),
        "scala" => Some(scala_grammar()),
        _ => None,
    }
}

/// Counts the number of nodes of given kinds in a tree.
///
/// # Arguments
///
/// * `node` - The root node of the tree.
/// * `kind` - The kinds of nodes to count.
///
/// # Returns
///
/// A tuple containing the number of nodes of the given kind and the maximum nesting level of these nodes.
///
/// # Example
///
/// The function applied to a node representing the following code will return `(2, 2)` if the kind is `if_statement`:
///
/// ```c
/// int main(int a, int b) {
///     if (b > 0) {
///         if (a > b) {
///             return a;
///         } else {
///             return b;
///         }
///     }
///     return 0;
///  }
/// ```
///
fn count_nodes_of_kind(root: &Node, kinds: &HashSet<&str>) -> (usize, usize) {
    let mut node_count = 0;
    let mut max_nesting = 0;

    let mut cursor = root.walk();

    // Simulating call stack
    let mut call_stack: Vec<(Node, usize)> = Vec::new();
    call_stack.push((*root, 1));

    while let Some((node, depth)) = call_stack.pop() {
        let is_of_kind = kinds.contains(node.kind());

        if is_of_kind {
            node_count += 1;
            max_nesting = max_nesting.max(depth);
        }

        // We don't reverse nodes for performance (yields the same result)
        for child in node.children(&mut cursor) {
            call_stack.push((child, if is_of_kind { depth + 1 } else { depth }));
        }
    }

    (node_count, max_nesting)
}

fn find_first_node<'a>(
    node: &Node<'a>,
    pred: &dyn Fn(&Node) -> bool,
    breadth: bool,
) -> Vec<Node<'a>> {
    let mut cursor = node.walk();
    let mut call_stack: Vec<(Node, usize)> = Vec::new();
    call_stack.push((*node, 0));

    let mut res: Vec<Node<'a>> = Vec::new();
    let mut max_depth: Option<usize> = None;

    while let Some((node, depth)) = call_stack.pop() {
        if max_depth.filter(|&d| depth > d).is_some() {
            return res;
        } else if pred(&node) {
            if breadth {
                res.push(node);
                if max_depth.is_none() {
                    max_depth = Some(depth);
                }
            } else {
                return vec![node];
            }
        } else if breadth {
            let mut end_queue: Vec<(Node, usize)> =
                node.children(&mut cursor).map(|c| (c, depth + 1)).collect();
            end_queue.extend(call_stack);
            call_stack = end_queue;
        } else {
            for c in node
                .children(&mut cursor)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
            {
                call_stack.push((c, 0));
            }
        }
    }
    vec![]
}

fn find_first_node_of_kind<'a>(
    root: &Node<'a>,
    kind: &HashSet<&str>,
    breadth: bool,
) -> Vec<Node<'a>> {
    find_first_node(root, &|n: &Node| kind.contains(n.kind()), breadth)
}

/// Finds the first error node in the tree
///
/// # Arguments
///
/// * `root` - The root node of the tree.
///
/// # Returns
///
/// The first error node found in the tree, or `None` if no error node is found.
fn find_first_error_node<'a>(root: &Node<'a>) -> Option<Node<'a>> {
    find_first_node(root, &|n: &Node| n.is_error() || n.is_missing(), false)
        .into_iter()
        .next()
}

fn find_first_error_position(root: &Node) -> Option<(usize, usize)> {
    find_first_error_node(root).map(|n| (n.start_position().row + 1, n.start_position().column + 1))
}

fn position_to_string(position: Option<(usize, usize)>) -> String {
    match position {
        Some((row, col)) => format!("{}:{}", row, col),
        None => "not-found".to_string(),
    }
}

fn find_fields<'a>(root: &Node<'a>, field: &str) -> Vec<Node<'a>> {
    let mut res: Vec<Node<'a>> = Vec::new();
    let mut ids: HashSet<usize> = HashSet::new();

    let mut cursor = root.walk();

    // Simulating call stack
    let mut call_stack: Vec<Node> = Vec::new();
    call_stack.push(*root);

    while let Some(node) = call_stack.pop() {
        for c in node.children_by_field_name(field, &mut node.walk()) {
            res.push(c);
            ids.insert(c.id());
        }

        // We don't reverse nodes for performance (yields the same result)
        for c in node
            .children(&mut cursor)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            if !ids.contains(&c.id()) {
                call_stack.push(c);
            }
        }
    }

    res
}

fn find_first_field<'a>(root: &Node<'a>, field: &str) -> Option<Node<'a>> {
    let mut cursor = root.walk();

    // Simulating call stack
    let mut call_stack: Vec<Node> = Vec::new();
    call_stack.push(*root);

    while let Some(node) = call_stack.pop() {
        if let Some(c) = node.child_by_field_name(field) {
            return Some(c);
        }

        // We don't reverse nodes for performance (yields the same result)
        for c in node
            .children(&mut cursor)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            call_stack.push(c);
        }
    }

    None
}

fn find_kind<'a>(root: &Node<'a>, kinds: &HashSet<&str>) -> Vec<Node<'a>> {
    let mut res: Vec<Node<'a>> = Vec::new();

    let mut cursor = root.walk();

    // Simulating call stack
    let mut call_stack: Vec<Node> = Vec::new();
    call_stack.push(*root);

    while let Some(node) = call_stack.pop() {
        if kinds.contains(node.kind()) {
            res.push(node);
        } else {
            // We don't reverse nodes for performance (yields the same result)
            for c in node.children(&mut cursor) {
                call_stack.push(c);
            }
        }
    }

    res
}

fn remove_kind_from_source(source: &[u8], root: &Node, kinds: &HashSet<&str>) -> Vec<u8> {
    let mut nodes = find_kind(root, kinds);
    nodes.sort_by_key(|b| std::cmp::Reverse(b.start_byte()));
    // Disable mutability
    let nodes = nodes;

    let root_start = root.start_byte();
    let mut new_source = source.to_vec();
    for n in nodes {
        new_source.drain(n.start_byte() - root_start..n.end_byte() - root_start);
    }
    new_source
}

//--------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::path::Path;

    use polars::prelude::SortMultipleOptions;

    use crate::utils::dataframes::*;
    use crate::utils::fs::*;

    use super::*;

    const TEST_DATA: &str = "tests/data/phases/parse";

    fn test_parse(input_file_path: &str, languages: Option<Vec<&str>>, should_pass: bool) {
        let input_df = open_csv(&input_file_path, None, None);
        assert!(input_df.is_ok());
        let input_df = input_df.unwrap();
        assert!(has_column(&input_df, "name"));
        let input_df = input_df.column("name").unwrap().str().unwrap();

        let output_file_path = format!("{}.functions.csv", input_file_path);
        assert!(delete_file(&output_file_path, true).is_ok());

        let logs_file_path = format!("{}.function_logs.csv", input_file_path);
        assert!(delete_file(&logs_file_path, true).is_ok());

        for path in input_df {
            assert!(path.is_some());
            assert!(delete_dir(&format!("{}.functions", path.unwrap()), true).is_ok());
        }

        if should_pass {
            assert!(run(
                input_file_path,
                None,
                None,
                languages,
                "ignore",
                8,
                0,
                false,
                &mut Logger::new()
            )
            .is_ok());

            let logs_df = open_csv(&logs_file_path, None, None);
            assert!(logs_df.is_ok());
            let logs_df = logs_df.unwrap();
            assert!(has_column(&logs_df, "name"));
            let sorted_logs_df = logs_df
                .sort(vec!["name"], SortMultipleOptions::new())
                .unwrap();

            let expected_logs_df = open_csv(
                &format!("{}.function_logs.csv.expected", input_file_path),
                None,
                None,
            );
            assert!(expected_logs_df.is_ok());
            let expected_logs_df = expected_logs_df.unwrap();
            assert!(has_column(&expected_logs_df, "name"));
            let sorted_expected_logs_df = expected_logs_df
                .sort(vec!["name"], SortMultipleOptions::new())
                .unwrap();
            assert!(sorted_expected_logs_df.equals(&sorted_logs_df));

            let output_df = open_csv(&output_file_path, None, None);
            assert!(output_df.is_ok());
            let output_df = output_df.unwrap();
            assert!(has_column(&output_df, "path"));
            let sorted_output_df = output_df
                .sort(vec!["path"], SortMultipleOptions::new())
                .unwrap();

            let expected_df = open_csv(&format!("{}.expected", output_file_path), None, None);
            assert!(expected_df.is_ok());
            let expected_df = expected_df.unwrap();
            assert!(has_column(&expected_df, "path"));
            let sorted_expected_df = expected_df
                .sort(vec!["path"], SortMultipleOptions::new())
                .unwrap();

            assert!(sorted_expected_df.equals(&sorted_output_df));

            for path in sorted_output_df.column("path").unwrap().str().unwrap() {
                assert!(path.is_some());
                let path = Path::new(path.unwrap());
                assert!(path.exists());
                let expected_path_name = format!(
                    "{}.expected/{}",
                    path.parent().unwrap().to_str().unwrap(),
                    path.file_name().unwrap().to_str().unwrap()
                );
                let expected_path = Path::new(&expected_path_name);
                assert_eq!(
                    std::fs::read_to_string(path).unwrap(),
                    std::fs::read_to_string(expected_path).unwrap()
                );
            }
        } else {
            assert!(run(
                input_file_path,
                None,
                None,
                languages,
                "ignore",
                8,
                0,
                false,
                &mut Logger::new()
            )
            .is_err());
        }

        assert!(delete_file(&output_file_path, true).is_ok());
        assert!(delete_file(&logs_file_path, true).is_ok());

        for path in input_df {
            assert!(path.is_some());
            assert!(delete_dir(&format!("{}.functions", path.unwrap()), true).is_ok());
        }
    }

    #[test]
    fn parse_fp() {
        let input_file_path = format!("{}/to_parse.csv", TEST_DATA);

        test_parse(&input_file_path, None, true);
    }

    #[test]
    fn parse_go() {
        let input_file_path = format!("{}/parse_go.csv", TEST_DATA);

        test_parse(&input_file_path, None, true);
    }

    #[test]
    fn invalid_file() {
        let input_file_path = format!("{}/invalid.csv", TEST_DATA);

        test_parse(&input_file_path, None, true);
    }

    #[test]
    fn invalid_lang() {
        let input_file_path = format!("{}/empty.csv", TEST_DATA);

        test_parse(&input_file_path, Some(["rust"].to_vec()), false);
    }

    #[test]
    fn empty() {
        let input_file_path = format!("{}/empty.csv", TEST_DATA);

        test_parse(&input_file_path, Some(["c"].to_vec()), true);
    }
}
