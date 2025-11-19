use crate::LakeSoulMetaDataError;

/// Qualify a file path
/// only support unix
pub fn qualify_path(path: &str) -> Result<String, LakeSoulMetaDataError> {
    match url::Url::parse(path) {
        Ok(mut url) => {
            match url.scheme() {
                "s3" | "s3a" => {
                    if url.domain().is_none() {
                        return Err(LakeSoulMetaDataError::Other(
                            "invalid s3 url".into(),
                        ));
                    }
                }
                _ => {}
            };
            let new_path = url
                .path()
                .split('/')
                .filter(|s| !s.is_empty())
                .collect::<Vec<&str>>()
                .join("/");
            url.set_path(&new_path);
            Ok(url.into())
        }
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            let path = path.trim_start_matches('/');
            // local filesystem
            qualify_path(&["file://", path].join("/"))
        }
        Err(e) => Err(LakeSoulMetaDataError::ParseUrlError(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualify_path_test() {
        assert_eq!(
            qualify_path("file:///path/to/file").unwrap(),
            "file:///path/to/file"
        );
        assert_eq!(
            qualify_path("/path/to/file").unwrap(),
            "file:///path/to/file"
        );
        assert_eq!(
            qualify_path("http://example.com").unwrap(),
            "http://example.com/" // end is '/'
        );
        assert!(qualify_path("ssh://@:xxx").is_err());
        assert_eq!(
            qualify_path("a//b/c//d//e//f/").unwrap(),
            "file:///a/b/c/d/e/f" // end is empty
        );
        assert_eq!(
            qualify_path("//a//b/c//d//e//f/").unwrap(),
            "file:///a/b/c/d/e/f" // end is empty
        );
        assert_eq!(
            qualify_path("file:///////a//b/c//////////d//e//f").unwrap(),
            "file:///a/b/c/d/e/f" // end is empty
        );
        assert_eq!(
            qualify_path("file:a//b/c//////////d//e//f").unwrap(),
            "file:///a/b/c/d/e/f" // end is empty
        );
        assert_eq!(
            qualify_path("file:/a//b/c//////////d//e//f").unwrap(),
            "file:///a/b/c/d/e/f" // end is empty
        );
        // a will be domain
        assert_eq!(
            qualify_path("file://a//b/c//////////d//e//f").unwrap(),
            "file://a/b/c/d/e/f"
        );
        assert_eq!(
            qualify_path("s3://a//b/c//////////d//e//f").unwrap(),
            "s3://a/b/c/d/e/f"
        );
        assert!(qualify_path("s3:///a//b/c//////////d//e//f").is_err(),);
        // let x = qualify_path("//a//b/c//d//e//f/");
        // let x = qualify_path("/a//b/c//d//e//f/");
    }
}
