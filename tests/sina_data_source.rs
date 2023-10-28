#[cfg(test)]
mod test {
    use qshare::{sina::stock::SinaDataSource, RealTimeData};
    use tracing::Level;
    #[tokio::test]
    async fn data_service_works() -> anyhow::Result<()> {
        // 创建一个订阅者，将日志输出到writer
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG) // 设置日志级别为INFO
            .finish();

        // 将订阅者添加到全局的跟踪器
        tracing::subscriber::set_global_default(subscriber).expect("设置全局默认订阅者失败");

        let data_source = SinaDataSource {};
        let df = data_source.real_time_spot_data().await?;

        tracing::debug!("real time data is: {:?}", df);

        Ok(())
    }

    #[tokio::test]
    async fn data_service_works_real_time_spot_em_data() -> anyhow::Result<()> {
        // 创建一个订阅者，将日志输出到writer
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG) // 设置日志级别为INFO
            .finish();

        // 将订阅者添加到全局的跟踪器
        tracing::subscriber::set_global_default(subscriber).expect("设置全局默认订阅者失败");

        let data_source = SinaDataSource {};
        let df = data_source.real_time_spot_em_data().await?;

        tracing::debug!("real time data is: {:?}", df);

        Ok(())
    }

    #[test]
    fn test_md5() {
        let digest = md5::compute(b"abcdefghijklmnopqrstuvwxyz");
        assert_eq!(format!("{:x}", digest), "c3fcd3d76192e4007dfb496cca67e13b");
    }
}
