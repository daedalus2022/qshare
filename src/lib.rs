use crate::utils::{DateUtils, Envs};
use async_trait::async_trait;
use core::convert::From;
use polars::datatypes::DataType;
use polars::export::chrono::NaiveDate;
use polars::frame::row::Row;
use polars::frame::DataFrame;
use polars::io::{SerReader, SerWriter};
use polars::prelude::{CsvReader, CsvWriter, JsonFormat, JsonReader, Schema};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use utils::IoUtils;

pub mod cffex;
pub mod const_vars;
pub mod sina;
pub mod utils;

pub mod update_local_temp_data;

///
/// enum 数据驱动
/// trait 模式驱动
///
/// 数据服务，通过数据源和实现的数据类型增加获取数据的能力
///
#[derive(Debug, Clone)]
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
    fn load(&self) -> Result<DataResult<T>, anyhow::Error>;
}

///
/// 实时行情
///
#[async_trait]
pub trait RealTimeData {
    ///
    /// 指数实时行情
    ///
    async fn real_time_spot_data(self) -> Result<DataResult<DataFrame>, anyhow::Error>;

    ///
    /// 股票实时行情
    ///
    async fn real_time_spot_em_data(self) -> Result<DataResult<DataFrame>, anyhow::Error>;
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

    fn load(&self) -> Result<DataResult<DataFrame>, anyhow::Error> {
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
                        //代码,名称,最新价,涨跌幅,涨跌额,成交量,成交额,振幅,最高,最低,今开,昨收,量比,换手率,市盈率-动态,市净率,总市值,流通市值,涨速,5分钟涨跌,60日涨跌幅,年初至今涨跌幅,symbol
                        let mut schema = Schema::new();
                        schema.with_column("代码".to_string(), DataType::Utf8);
                        schema.with_column("名称".to_string(), DataType::Utf8);
                        schema.with_column("最新价".to_string(), DataType::Float64);
                        schema.with_column("涨跌幅".to_string(), DataType::Float64);
                        schema.with_column("涨跌额".to_string(), DataType::Float64);
                        schema.with_column("成交量".to_string(), DataType::Float64);
                        schema.with_column("成交额".to_string(), DataType::Float64);
                        schema.with_column("振幅".to_string(), DataType::Float64);
                        schema.with_column("最高".to_string(), DataType::Float64);
                        schema.with_column("最低".to_string(), DataType::Float64);
                        schema.with_column("今开".to_string(), DataType::Float64);
                        schema.with_column("昨收".to_string(), DataType::Float64);
                        schema.with_column("量比".to_string(), DataType::Float64);
                        schema.with_column("换手率".to_string(), DataType::Float64);
                        schema.with_column("市盈率-动态".to_string(), DataType::Float64);
                        schema.with_column("市净率".to_string(), DataType::Float64);
                        schema.with_column("总市值".to_string(), DataType::Float64);
                        schema.with_column("流通市值".to_string(), DataType::Float64);
                        schema.with_column("涨速".to_string(), DataType::Float64);
                        schema.with_column("5分钟涨跌".to_string(), DataType::Float64);
                        schema.with_column("60日涨跌幅".to_string(), DataType::Float64);
                        schema.with_column("年初至今涨跌幅".to_string(), DataType::Float64);
                        schema.with_column("symbol".to_string(), DataType::Utf8);

                        let data_frame = csv_file
                            .has_header(true)
                            .with_schema(&schema)
                            .with_parse_dates(true)
                            .finish()
                            .expect("TODO: panic message");

                        Ok(DataResult {
                            data_id: Some(id.clone()),
                            data: Some(data_frame.clone()),
                        })
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

        if !&Path::new(&cache_temp_home).try_exists().ok().unwrap() {
            if IoUtils::create_dir_recursive(Path::new(&cache_temp_home)).is_err() {
                tracing::warn!("{} 缓存目录创建失败", &cache_temp_home)
            }
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
