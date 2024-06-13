# Database Schema

## Tables

### commits

Record each changed package in each git commit.

```sql
create table commits
(
    -- package name e.g. aarty
    pkg_name     varchar                  not null,
    -- package version e.g. 0.6.1
    pkg_version  varchar                  not null,
    -- path to package spec e.g. app-utils/aarty/spec
    spec_path    varchar                  not null,
    -- path to package defines e.g. app-utils/aarty/autobuild/defines
    defines_path varchar                  not null,
    -- tree e.g. aosc-os-abbs
    tree         varchar                  not null,
    -- git branch e.g. origin/aarty-0.6.1
    branch       varchar                  not null,
    -- git commit hash e.g. 72c602d9a0e79aa2ce5d1e8943864cdd545e9b57
    commit_id    varchar                  not null,
    -- git commit time e.g. 2024-05-11 16:31:39.000000 +00:00
    commit_time  timestamp with time zone not null,
    -- file status e.g. Modified/Added/Deleted
    status       varchar                  not null,
    constraint "pk-commits"
        primary key (pkg_name, pkg_version, tree, branch, commit_id)
);
```

### histories

Record the history which commit each commit points to like a time-series DB.

```sql
create table histories
(
    -- git commit hash where the branch was
    commit_id varchar                  not null,
    -- timestamp when this event occurred
    timestamp timestamp with time zone not null,
    -- git tree e.g. aosc-os-abbs
    tree      varchar                  not null,
    -- git branch e.g. origin/aarty-0.6.1
    branch    varchar                  not null,
    -- unused id for primary key
    id        serial
        primary key
);
```

### tree

Record aosc git trees: aosc-os-abbs, aosc-os-bsps

```sql
create table trees
(
    -- tree id
    tid        serial
        primary key,
    -- tree name e.g. aosc-os-abbs
    name       varchar not null,
    -- category e.g. base
    category   varchar not null,
    -- url e.g. https://github.com/AOSC-Dev/aosc-os-abbs/
    url        varchar not null,
    -- name of main branch e.g. stable
    mainbranch varchar not null
);
```