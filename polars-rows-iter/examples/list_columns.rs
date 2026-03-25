#![allow(dead_code)]

use polars::prelude::*;
use polars_rows_iter::*;

#[derive(Debug, FromDataFrameRow)]
struct SensorReading {
    sensor: String,
    measurements: Vec<f64>,
    tags: Option<Vec<Option<String>>>,
}

fn create_dataframe() -> PolarsResult<DataFrame> {
    let measurements = Column::new(
        "measurements".into(),
        vec![
            Series::new(PlSmallStr::EMPTY, &[1.0f64, 2.0, 3.0]),
            Series::new(PlSmallStr::EMPTY, &[4.5f64, 5.5]),
            Series::new(PlSmallStr::EMPTY, &[7.0f64]),
        ],
    );

    let tags = Column::new(
        "tags".into(),
        vec![
            Some(Series::new(PlSmallStr::EMPTY, &[Some("normal"), Some("peak"), None])),
            None,
            Some(Series::new(PlSmallStr::EMPTY, &[Some("low")])),
        ],
    );

    let sensor = Column::new("sensor".into(), &["temp_1", "temp_2", "temp_3"]);

    DataFrame::new(3, vec![sensor, measurements, tags])
}

fn main() -> PolarsResult<()> {
    let df = create_dataframe()?;

    println!("{df:?}");

    // Iterate rows as a typed struct
    println!("=== Row iteration ===");
    for row in df.rows_iter::<SensorReading>()? {
        let row = row?;
        println!("{row:?}");
    }

    // Scalar iteration over a single List column as Vec<f64>
    println!("\n=== Scalar iteration (Vec<f64>) ===");
    let all_measurements = df
        .scalar_iter("measurements")?
        .collect::<PolarsResult<Vec<Vec<f64>>>>()?;
    println!("{all_measurements:?}");

    // Scalar iteration over a nullable List column as raw Series
    println!("\n=== Scalar iteration (Option<Series>) ===");
    let raw_tags = df.scalar_iter("tags")?.collect::<PolarsResult<Vec<Option<Series>>>>()?;
    println!("{raw_tags:?}");

    Ok(())
}
