use std::{fs, io::Error, path::Path};

use crate::{const_vars, DataResult, RealTimeData, DataResultFormat};
use mime::Mime;
use polars::export::chrono::Local;
use polars::frame::DataFrame;
use reqwest::{header, Request, Response};

///
/// http 请求工具类
///
pub struct HttpClient;

impl HttpClient {
    ///
    /// 执行http请求，调用call_back_body方法处理响应数据，返回DataResult
    ///
    pub(crate) async fn _exec(
        request: Request,
        call_back_body: fn(body: String) -> DataResult<DataFrame>,
    ) -> Result<DataResult<DataFrame>, anyhow::Error> {
        tracing::debug!("request url: {:?}", request);

        let http_client = reqwest::Client::new();

        let response = http_client.execute(request).await?;

        let body = response.text().await?;

        Ok(call_back_body(body))
    }

    /// 
    /// 使用 DataResultFormat 对结果进行处理
    /// 
    pub async fn exec_by_format(request: Request, format: impl DataResultFormat)-> Result<DataResult<DataFrame>, anyhow::Error> {
        tracing::debug!("request url: {:?}", request);

        let http_client = reqwest::Client::new();

        let response = http_client.execute(request).await?;

        match response.text().await {
            Ok(body) => {
                        //1. 处理body
                // match format.to_dataframe(Some(body))
                if let Ok(data_frame) = format.to_dataframe(Some(body)){
                    // 2. 格式化df
                    return Ok(format.format(data_frame.data));
                }
            }
            Err(e) => {
                tracing::warn!("http response text error:{}", e);
            },
        }
        
        Ok(DataResult::default())
        
    }

    

    /// 将服务器返回的 content-type 解析成 Mime 类型
    fn _get_content_type(resp: &Response) -> Option<Mime> {
        resp.headers()
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap().parse().unwrap())
    }

    ///
    /// TODO 扩展根据响应content-type类型处理解析方法
    ///
    fn _process_body_to_dataframe(
        m: Option<Mime>,
        body: &String,
    ) -> Result<DataResult<DataFrame>, anyhow::Error> {
        match m {
            None => Ok(DataResult::<DataFrame>::from(body.to_string())),
            Some(v) if v == mime::APPLICATION_JSON => {
                Ok(DataResult::<DataFrame>::from(body.to_string()))
            }
            _ => Ok(DataResult::<DataFrame>::from(body.to_string())),
        }
    }
}

///
/// 环境工具
///
pub struct Envs;
impl Envs {
    ///
    /// 本地缓存目录
    ///
    pub fn cache_temp_home() -> String {
        dotenvy::var(const_vars::CACHE_TEMP_HOME).unwrap()
    }
}

pub struct DateUtils;

impl DateUtils {
    ///
    /// 年-月-日格式的日期
    ///
    pub fn now_fmt_ymd() -> String {
        let now = Local::now();
        now.format("%Y-%m-%d").to_string()
    }
}

pub struct IoUtils;

impl IoUtils {
    ///
    /// mkdir -p
    ///
    pub fn create_dir_recursive(path: &Path) -> Result<(), Error> {
        if path.exists() {
            Ok(())
        } else {
            match path.parent() {
                Some(parent) => IoUtils::create_dir_recursive(parent),
                None => fs::create_dir(path),
            }
        }
    }
}
