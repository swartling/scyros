# Scyros

[![Actions status](https://github.com/fxpl/scyros/actions/workflows/ci.yml/badge.svg)](https://github.com/fxpl/scyros/actions)
![License](https://img.shields.io/crates/l/scyros.svg)
[![Rust](https://img.shields.io/badge/rust-1.93-blue)](
https://releases.rs/docs/1.93.0/
)
[![Crates.io](https://img.shields.io/crates/v/scyros.svg)](https://crates.io/crates/scyros)

A framework to design sound, reproducible and scalable mining repositories studies on GitHub.

### Scyros is...

- 🧪 **Reproducibility-first**: declarative configuration and deterministic execution to enable repeatable experiments.
- 📈 **Scalable**: designed for large-scale repository mining studies on GitHub.
- 🧱 **Soundness-focused**: encourages transparent, bias-aware, and methodologically explicit study design.
- ⚙️ **Modular**: independent, reusable modules that can be composed into custom data-processing pipelines.

## Table of Contents

- [Installation](#installation)
- [Tutorial](#tutorial)
- [Usage](#usage)
- [Authentication and Rate Limits](#authentication-and-rate-limits)
- [Citing Scyros](#citing-scyros)
- [License](#license)
- [Change Log](#change-log)

## Installation

### Prebuilt binaries

Prebuilt binaries for macOS, Linux, and Windows are available on the project's [GitHub Releases page](https://github.com/fxpl/scyros/releases), along with installer scripts.

### Using a package manager

Scyros is available through several package managers.

### Cargo

Scyros is published on [crates.io](https://crates.io/crates/scyros) and can be installed with Cargo:

```bash
cargo install scyros
```

### Nix

If you use Nix with flakes enabled, you can install Scyros directly from GitHub:

```nix
nix profile install github:fxpl/scyros
```


### Build from source

Install Rust (version 1.94 or newer) by following the instructions on the [official website](https://rust-lang.org/tools/install/).

Then clone the repository and build:
```bash
git clone git@github.com:fxpl/scyros.git
cd scyros
cargo build --release
```

The binary is produced at `target/release/scyros`. You can optionally move it to a directory in your PATH for easier access.

## Tutorial

If you'd like to see how to use Scyros in practice, check out the [interactive tutorial](https://github.com/fxpl/scyros-tutorial)!

## Usage

To discover available commands and modules:

```bash
scyros --help
```

Each module provides its own usage documentation. For example, to inspect the module used to sample random repositories from GitHub:

```bash
scyros ids --help
```

## Authentication and Rate Limits

Some modules interact with the GitHub API and require personal access tokens (PATs). Tokens can be created by following GitHub’s documentation: [https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token).

⚠️ Never commit or share your tokens publicly.

Tokens must be provided as a CSV file passed via a command-line argument. The file must contain a single column named token, with one token per line:
```csv
    token
    fa56454....
    hj73647.... 
```

GitHub enforces API rate limits. Using multiple tokens from the same account does not increase these limits. Users are expected to comply with GitHub’s API terms and rate-limit policies:
- [Rate limits for the REST API](https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2022-11-28)
- [Terms of Service](https://docs.github.com/en/site-policy/github-terms/github-terms-of-service)

## Citing Scyros

Scyros is introduced and described in the following large-scale empirical study. If you use Scyros in academic work, please cite:.

```bibtex
@misc{gilot2026largescalestudyfloatingpointusage,
    title={Floating-Point Usage on GitHub: A Large-Scale Study of Statically Typed Languages}, 
    author={Andrea Gilot and Tobias Wrigstad and Eva Darulova},
    year={2026},
    eprint={2509.04936},
    archivePrefix={arXiv},
    primaryClass={cs.PL},
    url={https://arxiv.org/abs/2509.04936}, 
}   
```
> Gilot, A., Wrigstad, T., & Darulova, E. (2026). Floating-Point Usage on GitHub: A Large-Scale Study of Statically Typed Languages. arXiv. https://arxiv.org/abs/2509.04936

## License
This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

## Change Log
See [CHANGELOG.md](CHANGELOG.md) for a detailed list of changes and updates.