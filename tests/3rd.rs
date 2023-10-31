///
/// 三方API测试
///
///
#[cfg(test)]
mod test_3rd_works {
    use std::io::Cursor;

    use polars::prelude::{JsonFormat, JsonReader, SerReader};
    use serde_json::Value;

    #[test]
    fn md5_works() {
        let digest = md5::compute(b"abcdefghijklmnopqrstuvwxyz");
        assert_eq!(format!("{:x}", digest), "c3fcd3d76192e4007dfb496cca67e13b");
    }

    #[test]
    fn polars_from_json_str_to_df_works() {
        let str = "[{\"symbol\":\"sh000001\",\"name\":\"\\u4e0a\\u8bc1\\u6307\\u6570\",\"trade\":\"2978.7147\",\"pricechange\":\"-26.679\",\"changepercent\":\"-0.888\",\"buy\":\"0\",\"sell\":\"0\",\"settlement\":\"3005.3934\",\"open\":\"2995.3575\",\"high\":\"3006.2746\",\"low\":\"2977.1731\",\"volume\":251241582,\"amount\":290107550274,\"code\":\"000001\",\"ticktime\":\"14:49:47\"},{\"symbol\":\"sh000002\",\"name\":\"\\uff21\\u80a1\\u6307\\u6570\",\"trade\":\"3123.0694\",\"pricechange\":\"-28.020\",\"changepercent\":\"-0.889\",\"buy\":\"0\",\"sell\":\"0\",\"settlement\":\"3151.0892\",\"open\":\"3140.5584\",\"high\":\"3151.9899\",\"low\":\"3121.4373\",\"volume\":251112721,\"amount\":290011463607,\"code\":\"000002\",\"ticktime\":\"14:49:47\"},{\"symbol\":\"sh000003\",\"name\":\"\\uff22\\u80a1\\u6307\\u6570\",\"trade\":\"223.0712\",\"pricechange\":\"-0.430\",\"changepercent\":\"-0.193\",\"buy\":\"0\",\"sell\":\"0\",\"settlement\":\"223.5016\",\"open\":\"223.4595\",\"high\":\"224.4690\",\"low\":\"222.9191\",\"volume\":117275,\"amount\":57754823,\"code\":\"000003\",\"ticktime\":\"14:49:47\"}]";
        println!("str:{}", str);

        let file = Cursor::new(str);
        let df = JsonReader::new(file)
            .with_json_format(JsonFormat::Json)
            .infer_schema_len(Some(3))
            .with_batch_size(3)
            .finish();

        assert!(df.is_ok());
    }
    #[test]
    fn serde_from_str_to_json_works() {
        let str = "[{\"symbol\":\"sh000001\",\"name\":\"\\u4e0a\\u8bc1\\u6307\\u6570\",\"trade\":\"2978.7147\",\"pricechange\":\"-26.679\",\"changepercent\":\"-0.888\",\"buy\":\"0\",\"sell\":\"0\",\"settlement\":\"3005.3934\",\"open\":\"2995.3575\",\"high\":\"3006.2746\",\"low\":\"2977.1731\",\"volume\":251241582,\"amount\":290107550274,\"code\":\"000001\",\"ticktime\":\"14:49:47\"},{\"symbol\":\"sh000002\",\"name\":\"\\uff21\\u80a1\\u6307\\u6570\",\"trade\":\"3123.0694\",\"pricechange\":\"-28.020\",\"changepercent\":\"-0.889\",\"buy\":\"0\",\"sell\":\"0\",\"settlement\":\"3151.0892\",\"open\":\"3140.5584\",\"high\":\"3151.9899\",\"low\":\"3121.4373\",\"volume\":251112721,\"amount\":290011463607,\"code\":\"000002\",\"ticktime\":\"14:49:47\"},{\"symbol\":\"sh000003\",\"name\":\"\\uff22\\u80a1\\u6307\\u6570\",\"trade\":\"223.0712\",\"pricechange\":\"-0.430\",\"changepercent\":\"-0.193\",\"buy\":\"0\",\"sell\":\"0\",\"settlement\":\"223.5016\",\"open\":\"223.4595\",\"high\":\"224.4690\",\"low\":\"222.9191\",\"volume\":117275,\"amount\":57754823,\"code\":\"000003\",\"ticktime\":\"14:49:47\"}]";
        let json: Result<Value, serde_json::Error> = serde_json::from_str(str);
        assert!(json.is_ok());
    }
}
