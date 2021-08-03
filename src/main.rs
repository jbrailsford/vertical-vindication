use crossbeam::channel::{unbounded, Receiver, Sender};
use crossbeam::deque::{Injector, Worker};
use crossbeam::sync::Parker;
use crossbeam::thread;
use lazy_static::lazy_static;
use regex::Regex;
use reqwest::{
    blocking::Client,
    header::{HeaderMap, HeaderValue},
};
use serde::Deserialize;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Arc;
use std::time::{Duration, Instant};
use structopt::StructOpt;

lazy_static! {
    static ref HUNK: Regex = Regex::new(r#"@@\s(-|\+)(?P<a_start_line>\d+),(?P<a_extra_lines>\d+)\s(-|\+)(?P<b_start_line>\d+),(?P<b_extra_lines>\d+)\s@@"#).unwrap();
}

type GenericError = Box<dyn std::error::Error>;
type StartRange = (Option<u64>, Option<u64>);

#[derive(StructOpt, Clone)]
struct Config {
    #[structopt(short, long)]
    username: String,
    #[structopt(short, long)]
    token: String,
    #[structopt(short, long, default_value = "fix")]
    search_term: String,
    #[structopt(short, long, default_value = "1")]
    max_pages: u64,
}

#[derive(Debug, Deserialize)]
struct CommitSearchResult {
    total_count: u64,
    incomplete_results: bool,
    items: Vec<CommitSearchResultItem>,
}

#[derive(Debug, Deserialize)]
struct CommitSearchResultItem {
    url: String,
}

#[derive(Debug, Deserialize)]
struct Commit {
    files: Option<Vec<CommitFile>>,
}

#[derive(Debug, Deserialize)]
struct CommitFile {
    filename: String,
    status: String,
    patch: Option<String>,
}

#[derive(Clone)]
struct Crawler {
    done: Arc<AtomicBool>,
    config: Config,
}

impl Crawler {
    fn from_config(config: Config) -> Self {
        Crawler {
            config,
            done: Arc::new(AtomicBool::new(false)),
        }
    }

    fn get_client() -> Result<Client, GenericError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "Accept",
            HeaderValue::from_static("application/vnd.github.cloak-preview+json"),
        );
        headers.insert(
            reqwest::header::USER_AGENT,
            HeaderValue::from_static("vertical-vindication"),
        );

        Client::builder()
            .default_headers(headers)
            .build()
            .map_err(Into::into)
    }

    fn search(
        &self,
        client: &Client,
        page: u64,
    ) -> Result<(bool, CommitSearchResult), GenericError> {
        let results: CommitSearchResult = client
            .get(format!(
                "https://api.github.com/search/commits?q={}&per_page=100&page={}",
                &self.config.search_term, page
            ))
            .basic_auth(&self.config.username, Some(&self.config.token))
            .send()?
            .json()?;

        Ok((results.incomplete_results, results))
    }

    fn handle_commit(&self, client: &Client, url: &str) -> Option<u64> {
        let commit: Commit = client
            .get(url)
            .basic_auth(&self.config.username, Some(&self.config.token))
            .send()
            .ok()?
            .json()
            .ok()?;

        if let Some(files) = commit.files {
            if let Some(patch) = &files[0].patch {
                let mut captures = HUNK.captures_iter(patch);
                let mut range: StartRange = (None, None);
                if let Some(item) = captures.nth(0) {
                    range.0 = Some(u64::from_str(&item["a_start_line"]).ok()?);
                }

                if range.0.is_some() {
                    if let Some(item) = captures.last() {
                        range.1 = Some(u64::from_str(&item["a_start_line"]).ok()?);
                    }
                }

                return match range {
                    (Some(s), Some(e)) => Some(e - s),
                    _ => return None,
                };
            }
        }

        None
    }

    fn run(self) {
        let queue: Injector<CommitSearchResultItem> = Injector::new();
        let (s_results, r_results): (Sender<u64>, Receiver<u64>) = unbounded();

        thread::scope(|s| {
            for _ in 0..4 {
                let queue = &queue;
                let done = &self.done;
                let crawler = &self;
                let s_results = s_results.clone();

                s.spawn(move |_| {
                    let worker = Worker::new_fifo();
                    let client = Crawler::get_client().unwrap();
                    let parker = Parker::new();

                    while !done.load(SeqCst) || !worker.is_empty() || !queue.is_empty() {
                        if worker.is_empty() {
                            while queue.steal_batch(&worker).is_retry() {
                                parker.park_timeout(Duration::from_secs(1));
                            }
                        }

                        while let Some(item) = worker.pop() {
                            // This needs ratelimiting
                            crawler
                                .handle_commit(&client, &item.url)
                                .map(|n| s_results.send(n).unwrap());
                        }

                        if queue.is_empty() {
                            parker.park_timeout(Duration::from_secs(1));
                        }
                    }

                    std::mem::drop(s_results);
                });
            }

            drop(s_results);

            s.spawn(|_| {
                let client = Crawler::get_client().unwrap();
                let mut search_requests: u32 = 0;
                let mut timer = Instant::now();
                let time_limit = Duration::from_secs(60);

                for page in 1..self.config.max_pages + 1 {
                    // Basic rate limiting, probably flawed
                    if search_requests == 30 && timer.elapsed() <= time_limit {
                        std::thread::sleep(timer.elapsed() - time_limit);
                        search_requests = 0;
                        timer = Instant::now();
                    } else if timer.elapsed() > time_limit {
                        timer = Instant::now()
                            + Duration::from_secs(timer.elapsed().as_secs() % time_limit.as_secs());
                    }

                    let mut response = self.search(&client, page);
                    search_requests += 1;

                    while response.is_err() {
                        response = self.search(&client, 1);
                    }

                    let results = response.unwrap();
                    for item in results.1.items {
                        queue.push(item);
                    }

                    if !results.0 {
                        break;
                    }
                }

                self.done.store(true, SeqCst);
            });

            s.spawn(|_| {
                while let Ok(n) = r_results.recv() {
                    println!("{}", n); // Aggregate + output or histogram here
                }
            });
        })
        .unwrap();
    }
}

fn main() -> Result<(), GenericError> {
    let config = Config::from_args();
    Crawler::from_config(config).run();

    Ok(())
}
