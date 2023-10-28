#[cfg(test)]
mod tests {
    use std::path::Path;

    use qshare::{update_local_temp_data, utils::DateUtils};

    ///
    /// 以性能为主，减少csv和对象的序列化，能用文件处理使用文件处理
    ///
    #[tokio::test]
    async fn update_today_data() -> anyhow::Result<()> {
        // update_local_temp_data::update_today_data().await?;
        Ok(())
    }

    #[test]
    fn load() {
        let path = Path::new(
            "/Users/tom/work/01_code/github/rust/rbacktrader/temp/stock_data/source/sh000903.csv",
        );
        let data = update_local_temp_data::load_csv(&path).ok().unwrap();
        println!("now:{}", DateUtils::now_fmt_ymd());
        let is_none = data
            .iter()
            .find(|s| {
                println!("111{}", s.get(1).unwrap().to_string());
                s.get(1).unwrap().to_string() == DateUtils::now_fmt_ymd()
            })
            .is_none();
        println!("is_none:{}, {:?}", is_none, data[1].get(1));
    }
}