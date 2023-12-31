#[cfg(test)]
mod sina_data_source_works {
    use qshare::{sina::stock::sina::SinaIndexSpotDataSource, RealTimeData};

    #[tokio::test]
    async fn real_time_data_works() -> anyhow::Result<()> {
        let data_source = SinaIndexSpotDataSource {};
        {
            let df = data_source.real_time_data().await?;
            tracing::debug!("real time data is: {:?}", df);
        }
        let df = data_source.real_time_data().await?;
        tracing::debug!("real time data is: {:?}", df);

        let df = data_source.clone().real_time_data().await?;

        assert!(df.data.is_some());

        Ok(())
    }
}
