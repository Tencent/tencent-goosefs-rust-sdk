// Copyright (C) 2026 Tencent. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded-concurrency batch RPCs (`MAX_BATCH_RPC_IN_FLIGHT` = 64).

use futures::stream::{self, StreamExt};
use goosefs_sdk::fs::options::{CreateFileOptions, DeleteOptions, OpenFileOptions};
use goosefs_sdk::fs::FileSystem;
use goosefs_sdk::io::GoosefsFileInStream;

use crate::error::Error;
use crate::handle::JavaFsHandle;
use crate::Result;

pub(crate) const BATCH_CONCURRENCY_LIMIT: usize = 64;

pub(crate) fn even_pairs(pairs: Vec<String>) -> Result<Vec<(String, String)>> {
    if !pairs.len().is_multiple_of(2) {
        return Err(Error::Sdk(goosefs_sdk::error::Error::InvalidArgument {
            message: "pairs must have even length (flat src, dst, src, dst, ...)".into(),
        }));
    }
    Ok(pairs
        .as_chunks::<2>()
        .0
        .iter()
        .map(|c| (c[0].clone(), c[1].clone()))
        .collect())
}

pub(crate) async fn batch_get_status(
    handle: JavaFsHandle,
    paths: Vec<String>,
) -> Result<Vec<goosefs_sdk::fs::URIStatus>> {
    let fs = handle.fs;
    stream::iter(paths.into_iter().map(move |p| {
        let fs = fs.clone();
        async move { fs.get_status(&p).await.map_err(Error::from) }
    }))
    .buffered(BATCH_CONCURRENCY_LIMIT)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect()
}

pub(crate) async fn batch_exists(handle: JavaFsHandle, paths: Vec<String>) -> Result<Vec<bool>> {
    let fs = handle.fs;
    stream::iter(paths.into_iter().map(move |p| {
        let fs = fs.clone();
        async move { fs.exists(&p).await.map_err(Error::from) }
    }))
    .buffered(BATCH_CONCURRENCY_LIMIT)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect()
}

/// Opens every path; on the first error, already-opened streams are dropped.
pub(crate) async fn batch_open_file(
    handle: JavaFsHandle,
    paths: Vec<String>,
    options: OpenFileOptions,
) -> Result<Vec<GoosefsFileInStream>> {
    let fs = handle.fs;
    let results: Vec<Result<GoosefsFileInStream>> = stream::iter(paths.into_iter().map(move |p| {
        let fs = fs.clone();
        let opts = options.clone();
        async move { fs.open_file(&p, opts).await.map_err(Error::from) }
    }))
    .buffered(BATCH_CONCURRENCY_LIMIT)
    .collect()
    .await;

    let mut opened = Vec::new();
    for r in results {
        match r {
            Ok(stream) => opened.push(stream),
            Err(e) => {
                drop(opened);
                return Err(e);
            }
        }
    }
    Ok(opened)
}

pub(crate) async fn batch_create_file(
    handle: JavaFsHandle,
    paths: Vec<String>,
    options: CreateFileOptions,
) -> Result<Vec<i64>> {
    let fs = handle.fs;
    stream::iter(paths.into_iter().map(move |p| {
        let fs = fs.clone();
        let opts = options.clone();
        async move {
            let mut writer = fs.create_file(&p, opts).await?;
            writer.close().await?;
            Ok(0i64)
        }
    }))
    .buffered(BATCH_CONCURRENCY_LIMIT)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect()
}

pub(crate) async fn batch_create_dir(
    handle: JavaFsHandle,
    paths: Vec<String>,
    recursive: bool,
) -> Result<()> {
    let fs = handle.fs;
    stream::iter(paths.into_iter().map(move |p| {
        let fs = fs.clone();
        async move { fs.mkdir(&p, recursive).await.map_err(Error::from) }
    }))
    .buffered(BATCH_CONCURRENCY_LIMIT)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect()
}

pub(crate) async fn batch_rename(handle: JavaFsHandle, pairs: Vec<(String, String)>) -> Result<()> {
    let fs = handle.fs;
    stream::iter(pairs.into_iter().map(move |(src, dst)| {
        let fs = fs.clone();
        async move { fs.rename(&src, &dst).await.map_err(Error::from) }
    }))
    .buffered(BATCH_CONCURRENCY_LIMIT)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect()
}

pub(crate) async fn batch_delete(
    handle: JavaFsHandle,
    paths: Vec<String>,
    options: DeleteOptions,
) -> Result<()> {
    let fs = handle.fs;
    stream::iter(paths.into_iter().map(move |p| {
        let fs = fs.clone();
        let opts = options.clone();
        async move { fs.delete(&p, opts).await.map_err(Error::from) }
    }))
    .buffered(BATCH_CONCURRENCY_LIMIT)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect()
}

pub(crate) async fn batch_list_status(
    handle: JavaFsHandle,
    paths: Vec<String>,
    recursive: bool,
) -> Result<Vec<Vec<goosefs_sdk::fs::URIStatus>>> {
    let fs = handle.fs;
    stream::iter(paths.into_iter().map(move |p| {
        let fs = fs.clone();
        async move { fs.list_status(&p, recursive).await.map_err(Error::from) }
    }))
    .buffered(BATCH_CONCURRENCY_LIMIT)
    .collect::<Vec<_>>()
    .await
    .into_iter()
    .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn even_pairs_splits_flat_list() {
        let pairs = even_pairs(vec!["a".into(), "b".into(), "c".into(), "d".into()]).unwrap();
        assert_eq!(
            pairs,
            vec![("a".into(), "b".into()), ("c".into(), "d".into())]
        );
    }

    #[test]
    fn even_pairs_rejects_odd_length() {
        assert!(even_pairs(vec!["a".into(), "b".into(), "c".into()]).is_err());
    }
}
