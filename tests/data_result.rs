#[cfg(test)]
mod data_result_works {
    use polars::frame::DataFrame;
    use qshare::DataResult;
    #[test]
    fn data_result_from_json_str_works() {
        let str = "[{\"symbol\":\"sh000001\",\"name\":\"\\u4e0a\\u8bc1\\u6307\\u6570\",\"trade\":\"2978.7147\",\"pricechange\":\"-26.679\",\"changepercent\":\"-0.888\",\"buy\":\"0\",\"sell\":\"0\",\"settlement\":\"3005.3934\",\"open\":\"2995.3575\",\"high\":\"3006.2746\",\"low\":\"2977.1731\",\"volume\":251241582,\"amount\":290107550274,\"code\":\"000001\",\"ticktime\":\"14:49:47\"},{\"symbol\":\"sh000002\",\"name\":\"\\uff21\\u80a1\\u6307\\u6570\",\"trade\":\"3123.0694\",\"pricechange\":\"-28.020\",\"changepercent\":\"-0.889\",\"buy\":\"0\",\"sell\":\"0\",\"settlement\":\"3151.0892\",\"open\":\"3140.5584\",\"high\":\"3151.9899\",\"low\":\"3121.4373\",\"volume\":251112721,\"amount\":290011463607,\"code\":\"000002\",\"ticktime\":\"14:49:47\"},{\"symbol\":\"sh000003\",\"name\":\"\\uff22\\u80a1\\u6307\\u6570\",\"trade\":\"223.0712\",\"pricechange\":\"-0.430\",\"changepercent\":\"-0.193\",\"buy\":\"0\",\"sell\":\"0\",\"settlement\":\"223.5016\",\"open\":\"223.4595\",\"high\":\"224.4690\",\"low\":\"222.9191\",\"volume\":117275,\"amount\":57754823,\"code\":\"000003\",\"ticktime\":\"14:49:47\"}]";
        let dr = DataResult::<DataFrame>::from(str.to_string());
        assert!(dr.data.is_some());
        assert!(dr.data.unwrap().iter().len() > 0);
    }
}
