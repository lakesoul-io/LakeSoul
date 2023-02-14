/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

mod util;

use crate::hdfs::util::{coalesce_ranges, maybe_spawn_blocking, OBJECT_STORE_COALESCE_DEFAULT};
use crate::lakesoul_io_config::LakeSoulIOConfig;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::error::Result;
use datafusion_common::DataFusionError;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use hdrs::{Client, File};
use object_store::path::Path;
use object_store::Error::Generic;
use object_store::{GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore};
use parquet::data_type::AsBytes;
use std::cell::SyncUnsafeCell;
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::io::ErrorKind::NotFound;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};
use tokio_util::io::ReaderStream;

pub struct HDFS {
    client: Arc<Client>,
}

impl HDFS {
    pub fn try_new(config: LakeSoulIOConfig) -> Result<Self> {
        let uri = config
            .object_store_options
            .get("fs.defaultFS")
            .ok_or(DataFusionError::IoError(io::Error::new(
                ErrorKind::AddrNotAvailable,
                "fs.defaultFS is not set for hdfs object store"
            )))?;
        let user = config.object_store_options.get("fs.hdfs.user");
        let client = match user {
            None => Client::connect(uri.as_str()),
            Some(user) => Client::connect_as_user(uri.as_str(), user.as_str())
        }
        .map_err(|e| DataFusionError::IoError(e))?;

        Ok(Self {
            client: Arc::new(client),
        })
    }

    async fn is_file_exist(&self, path: &Path) -> object_store::Result<bool> {
        let t = path.to_string();
        let client = self.client.clone();
        maybe_spawn_blocking(Box::new(move || {
            let meta = client.metadata(t.as_str());
            match meta {
                Err(e) => {
                    if e.kind() == NotFound {
                        Ok(false)
                    } else {
                        Err(Generic {
                            store: "hdfs",
                            source: Box::new(e),
                        })
                    }
                }
                Ok(_) => Ok(true),
            }
        }))
        .await
    }

    async fn read_range_unblocking(
        file: Arc<SyncUnsafeCell<File>>,
        range: Range<usize>,
    ) -> object_store::Result<Bytes> {
        maybe_spawn_blocking(move || unsafe {
            file.get()
                .as_mut()
                .unwrap()
                .seek(SeekFrom::Start(range.start as u64))
                .map_err(|e| Generic {
                    store: "hdfs",
                    source: Box::new(e),
                })?;
            let to_read = range.end - range.start;
            let mut buf = vec![0; to_read];
            file.get()
                .as_mut()
                .unwrap()
                .read_exact(buf.as_mut_slice())
                .map_err(|e| Generic {
                    store: "hdfs",
                    source: Box::new(e),
                })?;
            Ok(buf.into())
        })
        .await
    }
}

impl Display for HDFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HDFSObjectStore")
    }
}

impl Debug for HDFS {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HDFSObjectStore")
    }
}

#[async_trait]
impl ObjectStore for HDFS {
    async fn put(&self, location: &Path, bytes: Bytes) -> object_store::Result<()> {
        let mut async_write = self
            .client
            .open_file()
            .write(true)
            .create(true)
            .append(true)
            .async_open(location.as_ref())
            .await
            .map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })?
            .compat();
        async_write.write_all(bytes.as_bytes()).await.map_err(|e| Generic {
            store: "hdfs",
            source: Box::new(e),
        })?;
        async_write.shutdown().await.map_err(|e| Generic {
            store: "hdfs",
            source: Box::new(e),
        })
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        // hdrs uses Unblocking underneath, so we don't have to
        // implement concurrent write
        let async_write = self
            .client
            .open_file()
            .write(true)
            .create(true)
            .truncate(true)
            .async_open(location.as_ref())
            .await
            .map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })?;
        Ok((location.to_string(), Box::new(async_write.compat_write())))
    }

    async fn abort_multipart(&self, location: &Path, multipart_id: &MultipartId) -> object_store::Result<()> {
        let file_exist = self.is_file_exist(location).await?;
        if file_exist {
            self.delete(location).await
        } else {
            Ok(())
        }
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        let path = location.to_string();
        let async_file = self
            .client
            .open_file()
            .read(true)
            .async_open(path.as_str())
            .await
            .map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })?;
        let reader_stream = ReaderStream::new(async_file.compat());
        Ok(GetResult::Stream(Box::pin(reader_stream.map_err(|e| Generic {
            store: "hdfs",
            source: Box::new(e),
        }))))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
        let file = Arc::new(SyncUnsafeCell::new(
            self.client
                .open_file()
                .read(true)
                .open(location.as_ref())
                .map_err(|e| Generic {
                    store: "hdfs",
                    source: Box::new(e),
                })?,
        ));
        HDFS::read_range_unblocking(file, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> object_store::Result<Vec<Bytes>> {
        // overwrite base method so that we don't have to open file multiple times
        // we don't use Hdrs::AsyncFile because it maintains a read pos state which makes it
        // not Sync.
        let file = Arc::new(SyncUnsafeCell::new(
            self.client
                .open_file()
                .read(true)
                .open(location.as_ref())
                .map_err(|e| Generic {
                    store: "hdfs",
                    source: Box::new(e),
                })?,
        ));
        coalesce_ranges(
            ranges,
            |range| HDFS::read_range_unblocking(file.clone(), range),
            OBJECT_STORE_COALESCE_DEFAULT,
        )
        .await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let path = location.to_string();
        let client = self.client.clone();
        maybe_spawn_blocking(move || {
            let meta = client.metadata(path.as_str()).map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })?;
            Ok(ObjectMeta {
                location: Path::parse(meta.path()).map_err(|e| Generic {
                    store: "hdfs",
                    source: Box::new(e),
                })?,
                last_modified: meta.modified().into(),
                size: meta.len() as usize,
            })
        })
        .await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let location = location.clone();
        let client = self.client.clone();
        maybe_spawn_blocking(move || match location.filename() {
            None => client.remove_dir(location.as_ref()).map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            }),
            Some(path) => client.remove_file(path).map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            }),
        })
        .await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>> {
        todo!()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        todo!()
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from = from.to_string();
        let to = to.to_string();
        let mut async_read = self
            .client
            .open_file()
            .read(true)
            .async_open(from.as_str())
            .await
            .map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })?
            .compat();
        let mut async_write = self
            .client
            .open_file()
            .truncate(true)
            .async_open(to.as_str())
            .await
            .map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })?
            .compat_write();
        tokio::io::copy(&mut async_read, &mut async_write)
            .await
            .map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })?;
        async_write.shutdown().await.map_err(|e| Generic {
            store: "hdfs",
            source: Box::new(e),
        })
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from = from.to_string();
        let to = to.to_string();
        let client = self.client.clone();
        maybe_spawn_blocking(move || {
            client.rename_file(from.as_str(), to.as_str()).map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })
        })
        .await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let t = to.to_string();
        let file_exist = self.is_file_exist(to).await?;
        if file_exist {
            Err(object_store::Error::AlreadyExists {
                path: t.clone(),
                source: "Destination already exist".into(),
            })
        } else {
            self.copy(from, to).await
        }
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let t = to.to_string();
        let file_exist = self.is_file_exist(to).await?;
        if file_exist {
            Err(object_store::Error::AlreadyExists {
                path: t.clone(),
                source: "Destination already exist".into(),
            })
        } else {
            self.rename(from, to).await
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::hdfs::HDFS;
    use crate::lakesoul_io_config::LakeSoulIOConfigBuilder;
    use object_store::path::Path;

    #[tokio::test]
    async fn test_hdfs() {
        let conf = LakeSoulIOConfigBuilder::new()
            .with_thread_num(2)
            .with_batch_size(8192)
            .with_max_row_group_size(250000)
            .with_object_store_option("fs.defaultFS".to_string(), "hdfs://localhost:9000".to_string())
            .with_object_store_option("fs.hdfs.user".to_string(), whoami::username())
            .build();
        let hdfs = HDFS::try_new(conf).unwrap();
        let path = Path::parse(format!("input/core-site.xml")).unwrap();
        println!("Path {} exists: {}", path, hdfs.is_file_exist(&path).await.unwrap());
    }
}
