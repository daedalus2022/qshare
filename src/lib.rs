use crate::utils::{DateUtils, Envs};

use async_trait::async_trait;
use core::convert::From;
use polars::export::chrono::NaiveDate;
use polars::frame::row::Row;
use polars::frame::DataFrame;
use polars::io::{SerReader, SerWriter};
use polars::prelude::{CsvReader, CsvWriter, JsonFormat, JsonReader, Schema};
use reqwest::Request;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use utils::IoUtils;

pub mod cffex;
pub mod const_vars;
pub mod sina;
pub mod utils;

///
/// enum 数据驱动
/// trait 模式驱动
///
/// 数据服务，通过数据源和实现的数据类型增加获取数据的能力
///
#[derive(Debug, Clone, Default)]
pub struct DataResult<T> {
    pub data_id: Option<String>,
    pub data: Option<T>,
}

///
/// HLOC
///
#[derive(Debug, Deserialize, Serialize)]
pub struct StockData {
    pub symbol: String,
    pub date: String,
    pub low: f64,
    pub close: f64,
    pub open: f64,
    pub high: f64,
    pub volume: f64,
}

///
/// 缓存结果，避免频繁调用导致ip被封
///
pub trait ResultCached<T> {
    ///
    /// 判断缓存是否存在
    ///
    fn is_cached(&self) -> bool;

    ///
    /// 缓存数据
    ///
    fn cache(&self);

    ///
    /// 加载缓存
    ///
    fn load(&self, schema: Option<Schema>) -> Result<DataResult<T>, anyhow::Error>;
}

///
/// 实时行情
///
#[async_trait]
pub trait RealTimeData {
    ///
    /// 实时行情
    ///
    async fn real_time_data(&self) -> Result<DataResult<DataFrame>, anyhow::Error>;

    ///
    /// 加载缓存时schema信息
    ///
    fn load_cached_schema(&self) -> Option<Schema>;
}

///
/// data result 格式化处理
///
pub trait DataResultFormat {
    ///
    /// 对提供的数据进行处理生成DataFrame
    ///
    fn to_dataframe(&self, source: Option<String>) -> anyhow::Result<DataResult<DataFrame>>;

    ///
    /// 列别名
    ///
    fn col_alias(&self) -> Option<Vec<(&str, &str)>>;

    ///
    /// 格式化程序，包括列名重命名及加载缓存时schema信息
    ///
    fn format(&self, data_result_format: Option<DataFrame>) -> DataResult<DataFrame>;
}

///
/// http data source
///
pub trait HttpSource {
    ///
    /// 构造请求
    ///
    fn request(&self) -> Request;

    ///
    /// id 生产策略
    ///
    fn id(&self) -> String;
}

///
/// 历史行情
///
#[async_trait]
pub trait HistoryData {
    ///
    /// 日行情
    ///
    async fn history_daily(
        self,
        market: &str,
        symbol: &str,
        start: NaiveDate,
        end: NaiveDate,
    ) -> Result<DataResult<DataFrame>, anyhow::Error>;
}

impl ResultCached<DataFrame> for DataResult<DataFrame> {
    fn is_cached(&self) -> bool {
        match &self.data_id {
            None => false,
            Some(id) => Path::new(&DataResult::cache_file_name(id)).exists(),
        }
    }

    fn cache(&self) {
        match &self.data_id {
            None => {
                tracing::warn!("data_id 为空，缓存文件失败");
            }
            Some(id) => {
                let cache_file = DataResult::cache_file_name(id);
                let file_result = File::create(&cache_file);
                match file_result {
                    Ok(mut file) => {
                        CsvWriter::new(&mut file)
                            .has_header(true)
                            .finish(&mut self.data.clone().unwrap())
                            .expect("缓存数据存储失败");
                    }
                    Err(e) => {
                        tracing::warn!("{}缓存文件创建失败{}", &cache_file, e);
                    }
                }
            }
        }
    }

    fn load(&self, schema_opt: Option<Schema>) -> Result<DataResult<DataFrame>, anyhow::Error> {
        match &self.data_id {
            None => {
                tracing::warn!("data_id 为空，加载缓存文件失败");
                Ok(DataResult::empty())
            }
            Some(id) => {
                let cache_file = DataResult::cache_file_name(id);
                tracing::debug!("load file path:{:?}", &cache_file);

                let data_frame_result = CsvReader::from_path(&cache_file);

                match data_frame_result {
                    Ok(csv_file) => {
                        if let Some(schema) = schema_opt {
                            let data_frame = csv_file
                                .has_header(true)
                                .with_schema(&schema)
                                .with_parse_dates(true)
                                .finish()
                                .expect("TODO: panic message");

                            return Ok(DataResult {
                                data_id: Some(id.clone()),
                                data: Some(data_frame.clone()),
                            });
                        } else {
                            tracing::warn!(
                                "load的schema为None,缓存数据可能加载错误或为空,请提供！！！"
                            )
                        }

                        Ok(DataResult::empty())
                    }
                    Err(e) => {
                        tracing::warn!("加载缓存文件{}, 解析失败:{}", &cache_file, e);
                        Ok(DataResult::empty())
                    }
                }
            }
        }
    }
}

impl DataResult<DataFrame> {
    pub fn new(data_id: String, data_frame: DataFrame) -> DataResult<DataFrame> {
        DataResult {
            data_id: Some(data_id),
            data: Some(data_frame),
        }
    }
    fn empty() -> DataResult<DataFrame> {
        DataResult::<DataFrame> {
            data_id: None,
            data: Some(DataFrame::empty()),
        }
    }

    fn cache_file_name(data_id: &String) -> String {
        let cache_temp_home = Envs::cache_temp_home();
        let path = Path::new(&cache_temp_home);
        if !&path.try_exists().ok().unwrap() && IoUtils::create_dir_recursive(path).is_err() {
            tracing::warn!("{} 缓存目录创建失败", &cache_temp_home);
        }

        format_args!(
            "{}/{}-{}{}",
            cache_temp_home,
            &data_id,
            utils::DateUtils::now_fmt_ymd(),
            ".csv"
        )
        .to_string()
    }
}

impl From<Row<'_>> for StockData {
    fn from(value: Row) -> Self {
        let data = value.0;
        StockData {
            symbol: data[0].get_str().unwrap().to_string(),
            date: DateUtils::now_fmt_ymd(),
            low: data[6].to_string().parse().unwrap(),
            close: data[2].to_string().parse().unwrap(),
            open: data[7].to_string().parse().unwrap(),
            high: data[5].to_string().parse().unwrap(),
            volume: data[3].to_string().parse().unwrap(),
        }
    }
}

impl From<String> for DataResult<DataFrame> {
    fn from(value: String) -> Self {
        tracing::debug!("to DataResult from:{}", value);

        let file = Cursor::new(value);
        let df = JsonReader::new(file)
            .with_json_format(JsonFormat::Json)
            .infer_schema_len(Some(1024))
            .with_batch_size(10)
            .finish()
            .unwrap();

        DataResult::<DataFrame>::new("".to_string(), df)
    }
}
