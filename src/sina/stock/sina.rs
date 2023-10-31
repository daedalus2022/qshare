use anyhow::Error;
use async_trait::async_trait;
use polars::{
    lazy::dsl::{col, Expr},
    prelude::{DataFrame, DataType, IntoLazy, Schema},
};
use reqwest::{Method, Request, Url};

use crate::{
    utils::HttpClient, DataResult, DataResultFormat, HttpSource, RealTimeData, ResultCached,
};

///
///
/// 从新浪财经-指数获取所有指数的实时行情数据, 大量抓取容易封IP
/// http://vip.stock.finance.sina.com.cn/mkt/#hs_s
///
/// stock_zh_index_spot
///
///
#[derive(Clone, Debug)]
pub struct SinaIndexSpotDataSource {}

impl DataResultFormat for SinaIndexSpotDataSource {
    fn col_alias(&self) -> Option<Vec<(&str, &str)>> {
        //symbol,name,trade,pricechange,changepercent,buy,sell,settlement,open,high,low,volume,amount,code,ticktime
        let ca = vec![
            ("symbol", "代码"),
            ("name", "名称"),
            ("trade", "最新价"),
            ("changepercent", "涨跌幅"),
            ("pricechange", "涨跌额"),
            ("volume", "成交量"),
            ("amount", "成交额"),
            ("buy", "买"),
            ("high", "最高"),
            ("low", "最低"),
            ("open", "今开"),
            ("settlement", "昨收"),
            ("sell", "卖"),
            ("ticktime", "时间"),
            ("code", "code"),
        ];

        Some(ca)
    }
    fn format(&self, data_result_format: Option<DataFrame>) -> DataResult<DataFrame> {
        if let Some(result) = data_result_format {
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

            return DataResult {
                data_id: None,
                data: Some(df),
            };
        }

        DataResult::default()
    }

    fn to_dataframe(&self, source: Option<String>) -> anyhow::Result<DataResult<DataFrame>> {
        if let Some(body) = source {
            tracing::debug!("response: {:?}", &body);

            return Ok(DataResult::<DataFrame>::from(body));
        }

        Ok(DataResult::default())
    }
}

impl HttpSource for SinaIndexSpotDataSource {
    fn request(&self) -> Request {
        let url = Url::parse_with_params("http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeDataSimple",
                                         &[("page", "1"),
                                             ("num", "400"),
                                             ("sort", "symbol"),
                                             ("asc", "1"),
                                             ("node", "hs_s"),
                                             ("_s_r_a", "page"),
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

#[async_trait]
impl RealTimeData for SinaIndexSpotDataSource {
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
            if let Ok(mut result) = HttpClient::exec_by_format(self.request(), self.clone()).await {
                result.data_id = Some(data_id);
                result.clone().cache();
                return Ok(result);
            }
            Ok(result)
        }
    }

    ///
    /// 加载缓存时scheam信息
    ///
    fn load_cached_schema(&self) -> Option<Schema> {
        //代码,名称,最新价,涨跌幅,涨跌额,成交量,成交额,振幅,最高,最低,今开,昨收,量比,换手率,市盈率-动态,市净率,总市值,流通市值,涨速,5分钟涨跌,60日涨跌幅,年初至今涨跌幅,symbol
        let mut schema = Schema::new();

        // symbol,代码,名称,最新价,涨跌幅,涨跌额,成交量,成交额,买,最高,最低,今开,昨收,卖,时间

        schema.with_column("symbol".to_string(), DataType::Utf8);
        schema.with_column("代码".to_string(), DataType::Utf8);
        schema.with_column("名称".to_string(), DataType::Utf8);
        schema.with_column("最新价".to_string(), DataType::Float64);
        schema.with_column("涨跌幅".to_string(), DataType::Float64);
        schema.with_column("涨跌额".to_string(), DataType::Float64);
        schema.with_column("成交量".to_string(), DataType::Float64);
        schema.with_column("成交额".to_string(), DataType::Float64);
        schema.with_column("买".to_string(), DataType::Float64);
        schema.with_column("最高".to_string(), DataType::Float64);
        schema.with_column("最低".to_string(), DataType::Float64);
        schema.with_column("今开".to_string(), DataType::Float64);
        schema.with_column("昨收".to_string(), DataType::Float64);
        schema.with_column("卖".to_string(), DataType::Float64);
        schema.with_column("时间".to_string(), DataType::Utf8);

        Some(schema)
    }
}
