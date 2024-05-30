// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

mod hash_tests {
    use lakesoul_io::hash_utils::{HashValue, HASH_SEED};

    #[test]
    fn chrono_test() {
        let date = chrono::NaiveDate::parse_from_str("0001-01-01", "%Y-%m-%d").unwrap();
        let datetime = date.and_hms_opt(12, 12, 12).unwrap();
        let epoch_time = chrono::NaiveDateTime::from_timestamp_millis(0).unwrap();

        println!("{}", datetime.signed_duration_since(epoch_time).num_days() as i32);
        println!(
            "{}",
            chrono::NaiveDate::from_num_days_from_ce_opt(719162)
                .unwrap()
                .format("%Y-%m-%d")
        );
    }

    #[test]
    fn chrono_datetime_test() {
        let datetime = chrono::NaiveDateTime::parse_from_str(
            "1990-10-01 10:10:10.100000000",
            lakesoul_io::constant::FLINK_TIMESTAMP_FORMAT,
        )
        .unwrap();
        let epoch_time = chrono::NaiveDateTime::from_timestamp_millis(0).unwrap();

        println!("{}", datetime.signed_duration_since(epoch_time).num_days() as i32);
        println!(
            "{}",
            chrono::NaiveDate::from_num_days_from_ce_opt(719162)
                .unwrap()
                .format("%Y-%m-%d")
        );
    }

    #[test]
    fn hash_value_test() {
        // let hash = "321".hash_one(HASH_SEED) as i32;
        // dbg!(hash);
        assert_eq!(1.hash_one(HASH_SEED) as i32, -559580957);
        assert_eq!(2.hash_one(HASH_SEED) as i32, 1765031574);
        assert_eq!(3.hash_one(HASH_SEED) as i32, -1823081949);
        assert_eq!(4.hash_one(HASH_SEED) as i32, -397064898);

        assert_eq!(1u64.hash_one(HASH_SEED) as i32, -1712319331);
        assert_eq!(2u64.hash_one(HASH_SEED) as i32, -797927272);
        assert_eq!(3u64.hash_one(HASH_SEED) as i32, 519220707);
        assert_eq!(4u64.hash_one(HASH_SEED) as i32, 1344313940);

        assert_eq!(1.0f32.hash_one(HASH_SEED) as i32, -466301895);
        assert_eq!(2.0f32.hash_one(HASH_SEED) as i32, 1199227445);
        assert_eq!(3.0f32.hash_one(HASH_SEED) as i32, 1710391653);
        assert_eq!(4.0f32.hash_one(HASH_SEED) as i32, -1959694433);

        assert_eq!(1.0.hash_one(HASH_SEED) as i32, -460888942);
        assert_eq!(2.0.hash_one(HASH_SEED) as i32, -2030303457);
        assert_eq!(3.0.hash_one(HASH_SEED) as i32, 1075969934);
        assert_eq!(4.0.hash_one(HASH_SEED) as i32, 1290556682);

        assert_eq!("1".hash_one(HASH_SEED) as i32, 1625004744);
        assert_eq!("2".hash_one(HASH_SEED) as i32, 870267989);
        assert_eq!("3".hash_one(HASH_SEED) as i32, -1756013582);
        assert_eq!("4".hash_one(HASH_SEED) as i32, -2142269034);

        assert_eq!("321".hash_one("321".hash_one(HASH_SEED)) as i32, -218318595);

        assert_eq!("12".hash_one("1".hash_one(HASH_SEED)) as i32, 891492135);

        assert_eq!("22".hash_one("2".hash_one(HASH_SEED)) as i32, 1475972200);

        assert_eq!(0.0f32.hash_one(HASH_SEED) as i32, 933211791);
        assert_eq!((-0.0f32).hash_one(HASH_SEED) as i32, 933211791);
        assert_eq!(0.0.hash_one(HASH_SEED) as i32, -1670924195);
        assert_eq!((-0.0).hash_one(HASH_SEED) as i32, -1670924195);
        assert_eq!(49u8.hash_one(HASH_SEED) as i32, 766678906);
        assert_eq!(49.hash_one(HASH_SEED) as i32, 766678906);
        assert_eq!(false.hash_one(HASH_SEED) as i32, 933211791);

        assert_eq!(1065353216.hash_one(HASH_SEED) as i32, -466301895);
    }
}
