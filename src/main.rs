use qshare::{
    sina::stock::{eastmoney::EastmoneySpotEmDataSource, sina::SinaIndexSpotDataSource},
    RealTimeData,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. 东方财富数据源获取股票实时行情
    let data_source = EastmoneySpotEmDataSource {};
    // 2. 获取实时行情
    let df = data_source.real_time_data().await?;
    // 3. 打印行情数据
    println!("股票实时行情{:?}", df.data.unwrap());

    // 1. sina数据源获取股指实时行情
    let data_source = SinaIndexSpotDataSource {};
    // 2. 获取实时行情
    let df = data_source.real_time_data().await?;
    // 3. 打印行情数据
    println!("股指实时行情{:?}", df.data.unwrap());

    Ok(())
}
