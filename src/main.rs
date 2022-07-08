use abbs_meta::{db::commit::CommitDb, git::Repository, package, Config};
use tracing::{debug, Level};

#[async_std::main]
async fn main() {
    init_log();

    let config = Config::from_file("config.toml").unwrap();
    let repo = Repository::open(&config.abbs_path, config.thread).unwrap();

    let commit_db = CommitDb::open(&config.commits_db_path).await.unwrap();
    let updated_pkgs = commit_db.update(&repo).await.unwrap();
    debug!("{:?}", updated_pkgs);

    let git_repo = git2::Repository::open(&config.abbs_path).unwrap();
    package::generate_changelog("rust", &git_repo, &commit_db)
        .await
        .unwrap();

    //let pkgs = package::scan_packages(&config.abbs_path);
    //println!("{:?}", pkgs);
}

fn init_log() {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .init();
    //let subscriber = FmtSubscriber::builder()
    //    .with_max_level(Level::DEBUG)
    //    .with_file(true)
    //    .with_line_number(true)
    //    .finish();
    //tracing::subscriber::set_global_default(subscriber).unwrap();
}
