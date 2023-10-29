#[cfg(test)]
mod test {
    use qshare::{sina::stock::SinaDataSource, RealTimeData};

    #[tokio::test]
    async fn data_service_works() -> anyhow::Result<()> {
        let data_source = SinaDataSource {};
        let df = data_source.real_time_spot_data().await?;

        tracing::debug!("real time data is: {:?}", df);

        Ok(())
    }

    #[tokio::test]
    async fn data_service_works_real_time_spot_em_data() -> anyhow::Result<()> {
        let data_source = SinaDataSource {};
        let df = data_source.real_time_spot_em_data().await?;

        println!("real time data is: {:?}", df);

        Ok(())
    }

    #[test]
    fn test_md5() {
        let digest = md5::compute(b"abcdefghijklmnopqrstuvwxyz");
        assert_eq!(format!("{:x}", digest), "c3fcd3d76192e4007dfb496cca67e13b");
    }
}
