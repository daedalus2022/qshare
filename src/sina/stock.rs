use crate::utils::HttpClient;
use crate::{DataResult, RealTimeData, ResultCached, DataResultFormat, HttpSource};
use anyhow::Error;
use async_trait::async_trait;
use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::io::SerReader;
use polars::lazy::dsl::Expr;
use polars::prelude::{col, IntoLazy, JsonFormat, JsonReader, Schema};
use reqwest::{Method, Request, Url};
use std::collections::HashMap;
use std::io::Cursor;

///
/// 新浪财经数据源-股指
///
#[derive(Clone, Debug)]
pub struct SinaSpotDataSource {
}

///
/// 新浪财经数据源-股票
///
#[derive(Clone, Debug)]
pub struct SinaSpotEmDataSource {}

impl HttpSource for SinaSpotEmDataSource {
    fn request(&self)->Request {
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

        Request::new(Method::GET, url.clone())
    }

    fn id(&self) -> String {
        let url = self.request().url().clone();
        let digest = md5::compute(url.as_str().as_bytes());

        tracing::debug!("digest:{:?}, url: {}", digest, url);

        format!("{:?}", digest).to_string()
    }
}

impl DataResultFormat for SinaSpotEmDataSource {
    ///
    /// 列别名
    ///
    fn col_alias(&self) -> Option<HashMap<&'static str, &'static str>> {
        let mut ca = HashMap::<&str, &str>::new();

        ca.insert("f12", "代码");
        ca.insert("f14", "名称");
        ca.insert("f2", "最新价");
        ca.insert("f3", "涨跌幅");
        ca.insert("f4", "涨跌额");
        ca.insert("f5", "成交量");
        ca.insert("f6", "成交额");
        ca.insert("f7", "振幅");
        ca.insert("f15", "最高");
        ca.insert("f16", "最低");
        ca.insert("f17", "今开");
        ca.insert("f18", "昨收");
        ca.insert("f10", "量比");
        ca.insert("f8", "换手率");
        ca.insert("f9", "市盈率-动态");
        ca.insert("f23", "市净率");
        ca.insert("f20", "总市值");
        ca.insert("f21", "流通市值");
        ca.insert("f22", "涨速");
        ca.insert("f11", "5分钟涨跌");
        ca.insert("f24", "60日涨跌幅");
        ca.insert("f25", "年初至今涨跌幅");
        ca.insert("f12", "symbol");

        Some(ca)
    }

    fn format(&self, data_result_format: Option<DataFrame>)->DataResult<DataFrame> {
        if let Some(result) = data_result_format{
            // // 列名重命名
            let mut col_alias_exprs: Vec<Expr> = vec![];
            let mut drop_clos: Vec<&str> = vec![];

            if let Some(col_alias) = self.col_alias() {
                for (c, a) in col_alias {
                    col_alias_exprs.push(col(c).alias(a));
                    if "symbol" != c {
                        drop_clos.push(c);
                    }
                }
            }

            // df列格式化
            let df = result
                .lazy()
                .with_columns(col_alias_exprs) // 重命名列名
                .drop_columns(drop_clos) //删除重命名列名
                .collect()
                .unwrap();

            return DataResult{ data_id: None, data: Some(df) }
        }

        DataResult::default()
    }

    fn to_dataframe(&self, source:Option<String>)->anyhow::Result<DataResult<DataFrame>> {
        if let Some(body) = source{
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

            tracing::debug!("列名定义: {:?}", &df);

            return Ok(DataResult::<DataFrame>::new("".to_string(), df));
        }

        Ok(DataResult::default())
    }
}

#[async_trait]
impl RealTimeData for SinaSpotEmDataSource {
    ///
    /// 加载缓存时scheam信息
    ///
    fn load_cached_schema(&self) -> Option<Schema> {
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

        Some(schema)
    }

    ///
    ///新浪财经-行情中心首页-A股-分类-所有股票
    ///大量采集会被目标网站服务器封禁 IP, 如果被封禁 IP, 请 10 分钟后再试
    ///http://vip.stock.finance.sina.com.cn/mkt/#hs_s
    ///:return: 所有股票的实时行情数据
    ///
    async fn real_time_data(&self) -> Result<DataResult<DataFrame>, Error> {
        let data_id = self.id().clone();
        let result: DataResult<DataFrame> =
            DataResult::<DataFrame>::new(data_id.clone(), DataFrame::empty());
        if result.clone().is_cached() {
            if let Some(schema) = self.load_cached_schema() {
                result.load(Some(schema))
            } else {
                result.load(None)
            }
        } else {
            if let Ok(mut result)  = HttpClient::exec_by_format(self.request(), self.clone()).await{
                result.data_id = Some(data_id);
                result.clone().cache();
                return Ok(result);
            }
            Ok(result)
        }
    }
}

// #[async_trait]
// impl RealTimeData for SinaSpotDataSource {
//     ///
//     /// 列别名
//     ///
//     fn col_alias(&self) -> Option<HashMap<&'static str, &'static str>> {
//         let mut ca = HashMap::<&str, &str>::new();
//         //symbol,name,trade,pricechange,changepercent,buy,sell,settlement,open,high,low,volume,amount,code,ticktime
//         ca.insert("symbol", "代码");
//         ca.insert("name", "名称");
//         ca.insert("trade", "最新价");
//         ca.insert("changepercent", "涨跌幅");
//         ca.insert("pricechange", "涨跌额");
//         ca.insert("volume", "成交量");
//         ca.insert("amount", "成交额");
//         ca.insert("buy", "买");
//         ca.insert("high", "最高");
//         ca.insert("low", "最低");
//         ca.insert("open", "今开");
//         ca.insert("settlement", "昨收");
//         ca.insert("sell", "卖");
//         ca.insert("ticktime", "时间");

//         Some(ca)
//     }

//     ///
//     /// 加载缓存时scheam信息
//     ///
//     fn load_cached_schema(&self) -> Option<Schema> {
//         //代码,名称,最新价,涨跌幅,涨跌额,成交量,成交额,振幅,最高,最低,今开,昨收,量比,换手率,市盈率-动态,市净率,总市值,流通市值,涨速,5分钟涨跌,60日涨跌幅,年初至今涨跌幅,symbol
//         let mut schema = Schema::new();

//         schema.with_column("代码".to_string(), DataType::Utf8);
//         schema.with_column("名称".to_string(), DataType::Utf8);
//         schema.with_column("最新价".to_string(), DataType::Float64);
//         schema.with_column("涨跌幅".to_string(), DataType::Float64);
//         schema.with_column("涨跌额".to_string(), DataType::Float64);
//         schema.with_column("成交量".to_string(), DataType::Float64);
//         schema.with_column("成交额".to_string(), DataType::Float64);
//         schema.with_column("买".to_string(), DataType::Float64);
//         schema.with_column("最高".to_string(), DataType::Float64);
//         schema.with_column("最低".to_string(), DataType::Float64);
//         schema.with_column("今开".to_string(), DataType::Float64);
//         schema.with_column("昨收".to_string(), DataType::Float64);
//         schema.with_column("卖".to_string(), DataType::Float64);
//         schema.with_column("时间".to_string(), DataType::Utf8);
//         schema.with_column("symbol".to_string(), DataType::Utf8);

//         Some(schema)
//     }
//     ///
//     ///新浪财经-行情中心首页-A股-分类-所有指数
//     ///大量采集会被目标网站服务器封禁 IP, 如果被封禁 IP, 请 10 分钟后再试
//     ///http://vip.stock.finance.sina.com.cn/mkt/#hs_s
//     ///:return: 所有指数的实时行情数据
//     ///
//     async fn real_time_data(&self) -> Result<DataResult<DataFrame>, anyhow::Error> {
//         let url = Url::parse_with_params("http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeDataSimple",
//                                          &[("page", "1"),
//                                              ("num", "400"),
//                                              ("sort", "symbol"),
//                                              ("asc", "1"),
//                                              ("node", "hs_s"),
//                                              ("_s_r_a", "page"),
//                                          ]).unwrap();

//         let request = Request::new(Method::GET, url.clone());

//         let digest = md5::compute(url.clone().as_str().as_bytes());

//         tracing::debug!("digest:{:?}, url: {}", digest, url);

//         let data_id = format!("{:?}", digest).to_string();

//         let result: DataResult<DataFrame> =
//             DataResult::<DataFrame>::new(data_id.clone(), DataFrame::empty());
//         if result.clone().is_cached() {
//             //symbol,name,trade,pricechange,changepercent,buy,sell,settlement,open,high,low,volume,amount,code,ticktime
//             if let Some(schema) = self.load_cached_schema() {
//                 result.load(Some(schema))
//             } else {
//                 result.load(None)
//             }
//         } else {
//             let mut result = HttpClient::exec(request, |b| -> DataResult<DataFrame> {
//                 tracing::debug!("response: {:?}", &b);
//                 DataResult::<DataFrame>::from(b)
//             })
//             .await?;

//             result.data_id = Some(data_id.clone());

//             // 列名重命名
//             let mut col_alias_exprs = vec![];
//             let mut drop_clos = vec![];
//             if let Some(col_alias) = self.col_alias() {
//                 for (c, a) in col_alias {
//                     col_alias_exprs.push(col(c).alias(a));
//                     if "symbol" != c {
//                         drop_clos.push(c);
//                     }
//                 }
//             }

//             // df列格式化
//             let df = result
//                 .data
//                 .unwrap()
//                 .lazy()
//                 .with_columns(col_alias_exprs) // 重命名列名
//                 .drop_columns(drop_clos) //删除重命名列名
//                 .collect()
//                 .unwrap();

//             result.data = Some(df);

//             result.clone().cache();

//             Ok(result)
//         }
//     }
// }
