#[cfg(test)]
mod eastmoney_data_source_works {
    use qshare::{sina::stock::eastmoney::EastmoneySpotEmDataSource, RealTimeData};

    #[tokio::test]
    async fn real_time_data_works() -> anyhow::Result<()> {
        let data_source = EastmoneySpotEmDataSource {};
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
