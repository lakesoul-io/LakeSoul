// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

mod util;

use crate::hdfs::util::{
    OBJECT_STORE_COALESCE_DEFAULT, coalesce_ranges, maybe_spawn_blocking,
};
use crate::lakesoul_io_config::LakeSoulIOConfig;
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::error::Result;
use datafusion_common::DataFusionError;
use futures::stream::{BoxStream, empty};
use futures::{FutureExt, StreamExt};
use hdrs::{Client, ClientBuilder, File};
use object_store::Error::{Generic, Precondition};
use object_store::path::Path;
use object_store::{
    Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult, UploadPart,
};
use std::fmt::{Debug, Display, Formatter};
use std::io::ErrorKind::NotFound;
use std::io::SeekFrom;
use std::ops::Range;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};
use tokio_util::io::ReaderStream;

pub struct Hdfs {
    client: Arc<Client>,
}

impl Hdfs {
    pub fn try_new(host: &str, config: LakeSoulIOConfig) -> Result<Self> {
        let user = config.object_store_options.get("fs.hdfs.user");
        let client_builder = ClientBuilder::new(host);
        let client_builder = match user {
            None => client_builder,
            Some(user) => client_builder.with_user(user.as_str()),
        };
        let client = client_builder.connect().map_err(DataFusionError::IoError)?;

        Ok(Self {
            client: Arc::new(client),
        })
    }

    fn file_exist(client: Arc<Client>, path: &str) -> object_store::Result<bool> {
        let meta = client.metadata(path);
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
    }

    async fn is_file_exist(&self, path: &Path) -> object_store::Result<bool> {
        let t = add_leading_slash(path);
        let client = self.client.clone();
        maybe_spawn_blocking(Box::new(move || Self::file_exist(client, t.as_str()))).await
    }

    async fn delete(client: Arc<Client>, location: &Path) -> object_store::Result<()> {
        let t = add_leading_slash(location);
        let location = location.clone();
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
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let location = add_leading_slash(location);
        let mut async_write = self.client.open_file();
        async_write.write(true);
        if opts.mode == PutMode::Create {
            async_write.create_new(true);
        } else {
            async_write.create(true).truncate(true);
        }
        let mut async_write = async_write
            .async_open(location.as_ref())
            .await
            .map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })?
            .compat_write();
        for mut bytes in payload.into_iter() {
            async_write
                .write_all_buf(&mut bytes)
                .await
                .map_err(|e| Generic {
                    store: "hdfs",
                    source: Box::new(e),
                })?
        }
        async_write.flush().await.map_err(|e| Generic {
            store: "hdfs",
            source: Box::new(e),
        })?;
        async_write.shutdown().await.map_err(|e| Generic {
            store: "hdfs",
            source: Box::new(e),
        })?;
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        // hdrs uses Unblocking underneath, so we don't have to
        // implement concurrent write
        let location_ = add_leading_slash(location);
        let async_write = self
            .client
            .open_file()
            .write(true)
            .create(true)
            .truncate(true)
            .async_open(location_.as_ref())
            .await
            .map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })?
            .compat_write();
        Ok(Box::new(HDFSMultiPartUpload {
            client: self.client.clone(),
            writer: Arc::new(Mutex::new(Box::new(async_write))),
            location: location.clone(),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let object_meta = self.head(location).await?;
        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(
                    empty::<object_store::Result<Bytes>>().boxed(),
                ),
                attributes: Attributes::default(),
                range: 0..object_meta.size,
                meta: object_meta,
            });
        }
        let location = add_leading_slash(location);
        let range = if let Some(r) = options.range {
            match r {
                GetRange::Bounded(range) => Ok(range),
                GetRange::Offset(offset) => {
                    if offset >= object_meta.size {
                        Err(Precondition {
                            path: location.clone(),
                            source: format!(
                                "Request offset {} invalid against file size {}",
                                offset, object_meta.size
                            )
                            .into(),
                        })
                    } else {
                        Ok(offset..object_meta.size)
                    }
                }
                GetRange::Suffix(last) => {
                    if last > object_meta.size {
                        Err(Precondition {
                            path: location.clone(),
                            source: format!(
                                "Request last offset {} invalid against file size {}",
                                last, object_meta.size
                            )
                            .into(),
                        })
                    } else {
                        Ok((object_meta.size - last)..object_meta.size)
                    }
                }
            }
        } else {
            Ok(0..object_meta.size)
        }?;

        let mut async_read = self
            .client
            .open_file()
            .read(true)
            .async_open(location.as_ref())
            .await
            .map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })?
            .compat();
        async_read
            .seek(SeekFrom::Start(range.start as u64))
            .await
            .map_err(|e| Generic {
                store: "hdfs",
                source: Box::new(e),
            })?;
        let read = async_read.take((range.end - range.start) as u64);
        let stream = ReaderStream::new(read);
        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream.map(|item| {
                item.map_err(|e| Generic {
                    store: "hdfs",
                    source: Box::new(e),
                })
            }))),
            meta: object_meta,
            range,
            attributes: Attributes::default(),
        })
    }

    async fn get_range(
        &self,
        location: &Path,
        range: Range<u64>,
    ) -> object_store::Result<Bytes> {
        let location = add_leading_slash(location);
        let client = self.client.clone();
        maybe_spawn_blocking(move || {
            let file = client
                .open_file()
                .read(true)
                .open(location.as_ref())
                .map_err(|e| Generic {
                    store: "hdfs",
                    source: Box::new(e),
                })?;
            let to_read = range.end - range.start;
            let mut buf = vec![0; to_read as usize];
            let read_size = read_at(&file, &mut buf, range.start)?;
            if read_size != to_read as usize {
                Err(Generic {
                    store: "hdfs",
                    source: format!("read file {} range not complete", location).into(),
                })
            } else {
                Ok(buf.into())
            }
        })
        .await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let location = add_leading_slash(location);
        let client = self.client.clone();
        let file = Arc::new(
            client
                .open_file()
                .read(true)
                .open(location.as_ref())
                .map_err(|e| Generic {
                    store: "hdfs",
                    source: Box::new(e),
                })?,
        );
        coalesce_ranges(
            ranges,
            move |range| {
                let location = location.clone();
                let file = file.clone();
                async move {
                    maybe_spawn_blocking(move || {
                        let to_read = range.end - range.start;
                        let mut buf = vec![0; to_read as usize];
                        let read_size = read_at(&file, &mut buf, range.start)?;
                        if read_size != to_read as usize {
                            Err(Generic {
                                store: "hdfs",
                                source: format!(
                                    "read file {} range not complete",
                                    location
                                )
                                .into(),
                            })
                        } else {
                            Ok(buf.into())
                        }
                    })
                    .await
                }
            },
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
                size: meta.len(),
                e_tag: None,
                version: None,
            })
        })
        .await
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        Hdfs::delete(self.client.clone(), location).await
    }

    fn list(
        &self,
        _prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        todo!()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
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
        Ok(())
    }

    async fn rename(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from = add_leading_slash(from);
        let to = add_leading_slash(to);
        let client = self.client.clone();
        maybe_spawn_blocking(move || {
            client
                .rename_file(from.as_str(), to.as_str())
                .map_err(|e| Generic {
                    store: "hdfs",
                    source: Box::new(e),
                })
        })
        .await
    }

    async fn copy_if_not_exists(
        &self,
        from: &Path,
        to: &Path,
    ) -> object_store::Result<()> {
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

    async fn rename_if_not_exists(
        &self,
        from: &Path,
        to: &Path,
    ) -> object_store::Result<()> {
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

fn read_at(file: &File, buf: &mut [u8], offset: u64) -> object_store::Result<usize> {
    file.read_at(buf, offset).map_err(|e| Generic {
        store: "hdfs",
        source: Box::new(e),
    })
}

struct HDFSMultiPartUpload {
    client: Arc<Client>,
    writer: Arc<Mutex<Box<dyn AsyncWrite + Send + Unpin>>>,
    location: Path,
}

impl Debug for HDFSMultiPartUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "HDFS MultiPartUpload at location {}",
            self.location
        ))
    }
}

#[async_trait]
impl MultipartUpload for HDFSMultiPartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let writer = self.writer.clone();
        async move {
            let mut writer = writer.lock().await;
            for mut bytes in data.into_iter() {
                writer
                    .write_all_buf(&mut bytes)
                    .await
                    .map_err(|e| Generic {
                        store: "hdfs",
                        source: Box::new(e),
                    })?;
            }
            Ok(())
        }
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut writer = self.writer.lock().await;
        writer.flush().await.map_err(|e| Generic {
            store: "hdfs",
            source: Box::new(e),
        })?;
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.complete().await?;
        let file_exist = Hdfs::file_exist(self.client.clone(), self.location.as_ref())?;
        if file_exist {
            Hdfs::delete(self.client.clone(), &self.location).await
        } else {
            Ok(())
        }
    }
}

fn add_leading_slash(path: &Path) -> String {
    ["/", path.as_ref().trim_start_matches('/')].join("")
}

#[cfg(test)]
mod tests {
    use crate::lakesoul_io_config::{LakeSoulIOConfigBuilder, create_session_context};
    use bytes::Bytes;
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use futures::StreamExt;
    use object_store::GetResultPayload::Stream;
    use object_store::ObjectStore;
    use object_store::buffered::BufWriter;
    use object_store::path::Path;
    use rand::distr::{Alphanumeric, SampleString};
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

    async fn read_file_from_hdfs(
        path: String,
        object_store: Arc<dyn ObjectStore>,
    ) -> String {
        let file = object_store.get(&Path::from(path)).await.unwrap();
        match file.payload {
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
        // multipart upload and multi range get
        let write_path = format!("/user/{}/output.test.txt", whoami::username());
        let complete_path = format!("hdfs://chenxu-dev:9000{}", write_path);
        let url = Url::parse(complete_path.as_str()).unwrap();
        let mut conf = LakeSoulIOConfigBuilder::new()
            .with_thread_num(2)
            .with_batch_size(8192)
            .with_max_row_group_size(250000)
            .with_object_store_option(
                "fs.defaultFS".to_string(),
                "hdfs://chenxu-dev:9000".to_string(),
            )
            .with_object_store_option("fs.hdfs.user".to_string(), whoami::username())
            .with_files(vec![write_path.clone()])
            .build();
        let sess_ctx = create_session_context(&mut conf).unwrap();
        println!("files: {:?}", conf.files);
        let object_store = sess_ctx
            .runtime_env()
            .object_store(
                ObjectStoreUrl::parse(&url[..url::Position::BeforePath]).unwrap(),
            )
            .unwrap();

        let mut write =
            BufWriter::new(object_store.clone(), Path::from(write_path.clone()));
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

        assert_eq!(conf.files, vec![complete_path,]);
        let meta0 = object_store
            .head(&Path::from(write_path.as_str()))
            .await
            .unwrap();
        assert_eq!(meta0.location, Path::from(write_path.as_str()));
        assert_eq!(meta0.size, size as u64);

        // test get
        let s = read_file_from_hdfs(write_path.clone(), object_store.clone()).await;
        assert_eq!(s, string);

        // test get_range
        let read_concurrency = 16;
        let step = size / read_concurrency;
        let ranges = (0..read_concurrency)
            .into_iter()
            .map(|i| std::ops::Range::<u64> {
                start: (i * step) as u64,
                end: ((i + 1) * step) as u64,
            })
            .collect::<Vec<std::ops::Range<u64>>>();
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

        // test get_ranges
        let result = object_store
            .get_ranges(&Path::from(write_path.as_str()), ranges.as_slice())
            .await
            .unwrap();
        let result = bytes_to_string(result);
        assert_eq!(result, string);
    }
}
