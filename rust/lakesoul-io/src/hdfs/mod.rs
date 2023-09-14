// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

mod util;

use crate::hdfs::util::{coalesce_ranges, maybe_spawn_blocking, OBJECT_STORE_COALESCE_DEFAULT};
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
use object_store::{GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore};
use parquet::data_type::AsBytes;
use std::fmt::{Debug, Display, Formatter};
use std::io::ErrorKind::NotFound;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};
use tokio_util::io::ReaderStream;

pub struct Hdfs {
    client: Arc<Client>,
}

impl Hdfs {
    pub fn try_new(host: &str, config: LakeSoulIOConfig) -> Result<Self> {
        let user = config.object_store_options.get("fs.hdfs.user");
        let client = match user {
            None => Client::connect(host),
            Some(user) => Client::connect_as_user(host, user.as_str()),
        }
        .map_err(DataFusionError::IoError)?;

        Ok(Self {
            client: Arc::new(client),
        })
    }

    async fn is_file_exist(&self, path: &Path) -> object_store::Result<bool> {
        let t = add_leading_slash(path);
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

impl Display for Hdfs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HDFSObjectStore")
    }
}

impl Debug for Hdfs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "HDFSObjectStore")
    }
}

#[async_trait]
impl ObjectStore for Hdfs {
    async fn put(&self, location: &Path, bytes: Bytes) -> object_store::Result<()> {
        let location = add_leading_slash(location);
        let mut async_write = self
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
            })?
            .compat();
        async_write.write_all(bytes.as_bytes()).await.map_err(|e| Generic {
            store: "hdfs",
            source: Box::new(e),
        })?;
        async_write.flush().await.map_err(|e| Generic {
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
        let location = add_leading_slash(location);
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

    async fn abort_multipart(&self, location: &Path, _multipart_id: &MultipartId) -> object_store::Result<()> {
        let file_exist = self.is_file_exist(location).await?;
        if file_exist {
            self.delete(location).await
        } else {
            Ok(())
        }
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        let path = add_leading_slash(location);
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

    async fn get_opts(&self, location: &Path, _options: GetOptions) -> object_store::Result<GetResult> {
        self.get(location).await
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
        let location = add_leading_slash(location);
        let client = self.client.clone();
        maybe_spawn_blocking(move || {
            let mut file = client
                .open_file()
                .read(true)
                .open(location.as_ref())
                .map_err(|e| Generic {
                    store: "hdfs",
                    source: Box::new(e),
                })?;
            file.seek(SeekFrom::Start(range.start as u64)).map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })?;
            let to_read = range.end - range.start;
            let mut buf = vec![0; to_read];
            file.read_exact(buf.as_mut_slice()).map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })?;
            Ok(buf.into())
        })
        .await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> object_store::Result<Vec<Bytes>> {
        // tweak coalesce size and concurrency for hdfs
        coalesce_ranges(
            ranges,
            |range| self.get_range(location, range),
            OBJECT_STORE_COALESCE_DEFAULT,
        )
        .await
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let path = add_leading_slash(location);
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
                e_tag: None,
            })
        })
        .await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let t = add_leading_slash(location);
        let location = location.clone();
        let client = self.client.clone();
        maybe_spawn_blocking(move || match location.filename() {
            None => client.remove_dir(t.as_str()).map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            }),
            Some(_) => client.remove_file(t.as_str()).map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            }),
        })
        .await
    }

    async fn list(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>> {
        todo!()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> object_store::Result<ListResult> {
        todo!()
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from = add_leading_slash(from);
        let to = add_leading_slash(to);
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
        let from = add_leading_slash(from);
        let to = add_leading_slash(to);
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
        let t = add_leading_slash(to);
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
        let t = add_leading_slash(to);
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

fn add_leading_slash(path: &Path) -> String {
    ["/", path.as_ref().trim_start_matches('/')].join("")
}

#[cfg(test)]
mod tests {
    use crate::lakesoul_io_config::{create_session_context, LakeSoulIOConfigBuilder};
    use bytes::Bytes;
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use futures::StreamExt;
    use object_store::path::Path;
    use object_store::GetResult::Stream;
    use object_store::ObjectStore;
    use rand::distributions::{Alphanumeric, DistString};
    use rand::thread_rng;
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;
    use url::Url;

    fn bytes_to_string(bytes: Vec<Bytes>) -> String {
        unsafe {
            let mut vec = Vec::new();
            bytes.into_iter().for_each(|b| {
                let v = b.to_vec();
                vec.extend(v);
            });
            String::from_utf8_unchecked(vec)
        }
    }

    async fn read_file_from_hdfs(path: String, object_store: Arc<dyn ObjectStore>) -> String {
        let file = object_store.get(&Path::from(path)).await.unwrap();
        match file {
            Stream(s) => {
                let read_result = s
                    .collect::<Vec<object_store::Result<Bytes>>>()
                    .await
                    .into_iter()
                    .collect::<object_store::Result<Vec<Bytes>>>()
                    .unwrap();
                bytes_to_string(read_result)
            }
            _ => panic!("expect getting a stream"),
        }
    }

    #[tokio::test]
    async fn test_hdfs() {
        let files = vec![
            format!("/user/{}/input/hdfs-site.xml", whoami::username()),
            format!("/user/{}/input/yarn-site.xml", whoami::username()),
            format!("/user/{}/input/core-site.xml", whoami::username()),
        ];

        let mut conf = LakeSoulIOConfigBuilder::new()
            .with_thread_num(2)
            .with_batch_size(8192)
            .with_max_row_group_size(250000)
            .with_object_store_option("fs.defaultFS".to_string(), "hdfs://localhost:9000".to_string())
            .with_object_store_option("fs.hdfs.user".to_string(), whoami::username())
            .with_files(vec![
                format!("hdfs://localhost:9000{}", files[0]),
                format!("hdfs://{}", files[1]),
                files[2].clone(),
            ])
            .build();

        let sess_ctx = create_session_context(&mut conf).unwrap();

        let url = Url::parse(conf.files[0].as_str()).unwrap();
        let object_store = sess_ctx
            .runtime_env()
            .object_store(ObjectStoreUrl::parse(&url[..url::Position::BeforePath]).unwrap())
            .unwrap();

        assert_eq!(
            conf.files,
            vec![
                format!("hdfs://localhost:9000{}", files[0]),
                format!("hdfs://localhost:9000{}", files[1]),
                format!("hdfs://localhost:9000{}", files[2]),
            ]
        );
        let meta0 = object_store.head(&Path::from(files[0].as_str())).await.unwrap();
        assert_eq!(meta0.location, Path::from(files[0].as_str()));
        assert_eq!(meta0.size, 867);

        let s = read_file_from_hdfs(files[1].clone(), object_store.clone()).await;
        let path = format!("{}/etc/hadoop/yarn-site.xml", std::env::var("HADOOP_HOME").unwrap());
        let f = std::fs::read_to_string(path).unwrap();
        assert_eq!(s, f);

        // multipart upload and multi range get
        let write_path = format!("/user/{}/output/test.txt", whoami::username());
        let (_, mut write) = object_store
            .put_multipart(&Path::from(write_path.as_str()))
            .await
            .unwrap();
        let mut rng = thread_rng();

        let size = 64 * 1024 * 1024usize;
        let string = Alphanumeric.sample_string(&mut rng, size);
        let buf = string.as_bytes();

        let write_concurrency = 8;
        let step = size / write_concurrency;
        for i in 0..write_concurrency {
            let buf = &buf[i * step..(i + 1) * step];
            write.write_all(buf).await.unwrap();
        }
        write.flush().await.unwrap();
        write.shutdown().await.unwrap();
        drop(write);

        let read_concurrency = 16;
        let step = size / read_concurrency;
        let ranges = (0..read_concurrency)
            .into_iter()
            .map(|i| std::ops::Range::<usize> {
                start: i * step,
                end: (i + 1) * step,
            })
            .collect::<Vec<std::ops::Range<usize>>>();
        let mut result = Vec::new();
        for i in 0..16 {
            result.push(
                object_store
                    .get_range(&Path::from(write_path.as_str()), ranges[i].clone())
                    .await
                    .unwrap(),
            );
        }
        let result = bytes_to_string(result);
        assert_eq!(result, string);

        let result = object_store
            .get_ranges(&Path::from(write_path.as_str()), ranges.as_slice())
            .await
            .unwrap();
        let result = bytes_to_string(result);
        assert_eq!(result, string);
    }
}
