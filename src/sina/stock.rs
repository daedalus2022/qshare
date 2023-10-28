use crate::utils::HttpClient;
use crate::{DataResult, RealTimeData, ResultCached};
use anyhow::Error;
use async_trait::async_trait;
use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::io::SerReader;
use polars::prelude::{col, IntoLazy, JsonFormat, JsonReader, Schema};
use reqwest::{Method, Request, Url};
use std::io::Cursor;

///
/// 新浪财经数据源
///
pub struct SinaDataSource {}

///
/// enum 数据驱动
/// trait 模式驱动
///
#[async_trait]
impl RealTimeData for SinaDataSource {
    ///
    ///新浪财经-行情中心首页-A股-分类-所有指数
    ///大量采集会被目标网站服务器封禁 IP, 如果被封禁 IP, 请 10 分钟后再试
    ///http://vip.stock.finance.sina.com.cn/mkt/#hs_s
    ///:return: 所有指数的实时行情数据
    ///
    async fn real_time_spot_data(self) -> Result<DataResult<DataFrame>, anyhow::Error> {
        let url = Url::parse_with_params("http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeDataSimple",
                                         &[("page", "1"),
                                             ("num", "400"),
                                             ("sort", "symbol"),
                                             ("asc", "1"),
                                             ("node", "hs_s"),
                                             ("_s_r_a", "page"),
                                         ]).unwrap();

        let request = Request::new(Method::GET, url.clone());

        let digest = md5::compute(url.clone().as_str().as_bytes());

        tracing::debug!("digest:{:?}, url: {}", digest, url);

        let data_id = format!("{:?}", digest).to_string();

        let result: DataResult<DataFrame> =
            DataResult::<DataFrame>::new(data_id.clone(), DataFrame::empty());
        if result.clone().is_cached() {
            result.load()
        } else {
            let mut result = HttpClient::exec(request, |b| -> DataResult<DataFrame> {
                DataResult::<DataFrame>::from(b)
            })
            .await?;

            result.data_id = Some(data_id.clone());

            tracing::debug!("response dataframe: {:?}", &result);

            result.clone().cache();

            Ok(result)
        }
    }

    async fn real_time_spot_em_data(self) -> Result<DataResult<DataFrame>, Error> {
        let url = Url::parse_with_params("http://82.push2.eastmoney.com/api/qt/clist/get",
                                         &[("pn", "1"),
                                             ("pz", "50000"),
                                             ("po", "1"),
                                             ("np", "1"),
                                             ("ut", "bd1d9ddb04089700cf9c27f6f7426281"),
                                             ("fltt", "2"),
                                             ("invt", "2"),
                                             ("fid", "f3"),
                                             ("fs", "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048"),
                                             ("fields", "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152"),
                                             ("_", "1623833739532"),
                                         ]).unwrap();

        let request = Request::new(Method::GET, url.clone());

        let digest = md5::compute(url.clone().as_str().as_bytes());

        tracing::debug!("digest:{:?}, url: {}", digest, url);

        let data_id = format!("{:?}", digest).to_string();

        let result: DataResult<DataFrame> =
            DataResult::<DataFrame>::new(data_id.clone(), DataFrame::empty());
        if result.clone().is_cached() {
            result.load()
        } else {
            let mut result = HttpClient::exec(request, |body| -> DataResult<DataFrame> {
                let start = body.find("[").unwrap();
                let length = body.len();
                let json_data = String::from(&body[start..length - 2]).replace("\"-\"", "-1");

                let file = Cursor::new(json_data);

                let mut schema = Schema::new();
                schema.with_column("f12".to_string(), DataType::Utf8);

                let df = JsonReader::new(file)
                    .with_json_format(JsonFormat::Json)
                    .with_schema(&schema)
                    .infer_schema_len(Some(1024))
                    .with_batch_size(10)
                    .finish()
                    .unwrap();

                let df = df
                    .lazy()
                    .with_column(col("f12").alias("代码"))
                    .with_column(col("f14").alias("名称"))
                    .with_column(col("f2").alias("最新价"))
                    .with_column(col("f3").alias("涨跌幅"))
                    .with_column(col("f4").alias("涨跌额"))
                    .with_column(col("f5").alias("成交量"))
                    .with_column(col("f6").alias("成交额"))
                    .with_column(col("f7").alias("振幅"))
                    .with_column(col("f15").alias("最高"))
                    .with_column(col("f16").alias("最低"))
                    .with_column(col("f17").alias("今开"))
                    .with_column(col("f18").alias("昨收"))
                    .with_column(col("f10").alias("量比"))
                    .with_column(col("f8").alias("换手率"))
                    .with_column(col("f9").alias("市盈率-动态"))
                    .with_column(col("f23").alias("市净率"))
                    .with_column(col("f20").alias("总市值"))
                    .with_column(col("f21").alias("流通市值"))
                    .with_column(col("f22").alias("涨速"))
                    .with_column(col("f11").alias("5分钟涨跌"))
                    .with_column(col("f24").alias("60日涨跌幅"))
                    .with_column(col("f25").alias("年初至今涨跌幅"))
                    .with_column(col("f12").alias("symbol"))
                    .drop_columns(vec![
                        "f140", "f141", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9",
                        "f10", "f12", "f13", "f14", "f15", "f16", "f17", "f18", "f20", "f21",
                        "f23", "f24", "f25", "f22", "f11", "f62", "f128", "f136", "f115", "f152",
                    ])
                    .collect()
                    .unwrap();

                tracing::debug!("列名定义: {:?}", &df);

                DataResult::<DataFrame>::new("".to_string(), df)
            })
            .await?;

            result.data_id = Some(data_id.clone());

            tracing::debug!("response dataframe: {:?}", &result);

            result.clone().cache();

            Ok(result)
        }
    }
}
