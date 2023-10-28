use qshare::update_local_temp_data;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // update_local_temp_data::update_spot_today_data().await?;

    update_local_temp_data::update_spot_em_today_data().await?;
    Ok(())
}
