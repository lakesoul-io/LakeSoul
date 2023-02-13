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

use crate::lakesoul_io_config::LakeSoulIOConfig;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::error::Result;
use datafusion_common::DataFusionError;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use hdrs::Client;
use object_store::path::Path;
use object_store::Error::Generic;
use object_store::{GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore};
use std::fmt::{Debug, Display, Formatter};
use std::io::ErrorKind::NotFound;
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
        let client = Client::connect("default").map_err(|e| DataFusionError::IoError(e))?;
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
        todo!()
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
        todo!()
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> object_store::Result<Vec<Bytes>> {
        todo!()
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

async fn maybe_spawn_blocking<F, T>(f: F) -> object_store::Result<T>
where
    F: FnOnce() -> object_store::Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => runtime.spawn_blocking(f).await?,
        Err(_) => f(),
    }
}
