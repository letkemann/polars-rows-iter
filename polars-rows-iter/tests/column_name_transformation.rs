use polars::df;
use polars_rows_iter::*;

#[test]
fn test_prefix_transformation() {
    let df = df!(
        "col_a" => [1i32, 2, 3],
        "col_b" => [10.0f64, 20.0, 30.0],
    )
    .unwrap();

    #[derive(Debug, FromDataFrameRow, PartialEq)]
    #[from_dataframe(prefix("col_"))]
    struct Row {
        a: i32,
        b: f64,
    }

    let rows: Vec<Row> = df.rows_iter::<Row>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], Row { a: 1, b: 10.0 });
    assert_eq!(rows[1], Row { a: 2, b: 20.0 });
    assert_eq!(rows[2], Row { a: 3, b: 30.0 });
}

#[test]
fn test_postfix_transformation() {
    let df = df!(
        "id_col" => [1i32, 2, 3],
        "value_col" => [100.0f64, 200.0, 300.0],
    )
    .unwrap();

    #[derive(Debug, FromDataFrameRow, PartialEq)]
    #[from_dataframe(postfix("_col"))]
    struct Row {
        id: i32,
        value: f64,
    }

    let rows: Vec<Row> = df.rows_iter::<Row>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], Row { id: 1, value: 100.0 });
    assert_eq!(rows[1], Row { id: 2, value: 200.0 });
    assert_eq!(rows[2], Row { id: 3, value: 300.0 });
}

#[test]
fn test_prefix_and_postfix_transformation() {
    let df = df!(
        "tbl_id_fld" => [1i32, 2, 3],
        "tbl_name_fld" => ["a", "b", "c"],
    )
    .unwrap();

    #[derive(Debug, FromDataFrameRow, PartialEq)]
    #[from_dataframe(prefix("tbl_"), postfix("_fld"))]
    struct Row<'a> {
        id: i32,
        name: &'a str,
    }

    let rows: Vec<Row> = df.rows_iter::<Row>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], Row { id: 1, name: "a" });
    assert_eq!(rows[1], Row { id: 2, name: "b" });
    assert_eq!(rows[2], Row { id: 3, name: "c" });
}

#[test]
fn test_case_conversion_snake_to_pascal() {
    let df = df!(
        "UserId" => [1i32, 2, 3],
        "UserName" => ["alice", "bob", "charlie"],
    )
    .unwrap();

    #[derive(Debug, FromDataFrameRow, PartialEq)]
    #[from_dataframe(convert_case(Pascal))]
    struct Row<'a> {
        user_id: i32,
        user_name: &'a str,
    }

    let rows: Vec<Row> = df.rows_iter::<Row>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        Row {
            user_id: 1,
            user_name: "alice"
        }
    );
    assert_eq!(
        rows[1],
        Row {
            user_id: 2,
            user_name: "bob"
        }
    );
    assert_eq!(
        rows[2],
        Row {
            user_id: 3,
            user_name: "charlie"
        }
    );
}

#[test]
fn test_case_conversion_snake_to_camel() {
    let df = df!(
        "userId" => [1i32, 2, 3],
        "userName" => ["alice", "bob", "charlie"],
    )
    .unwrap();

    #[derive(Debug, FromDataFrameRow, PartialEq)]
    #[from_dataframe(convert_case(Camel))]
    struct Row<'a> {
        user_id: i32,
        user_name: &'a str,
    }

    let rows: Vec<Row> = df.rows_iter::<Row>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        Row {
            user_id: 1,
            user_name: "alice"
        }
    );
    assert_eq!(
        rows[1],
        Row {
            user_id: 2,
            user_name: "bob"
        }
    );
    assert_eq!(
        rows[2],
        Row {
            user_id: 3,
            user_name: "charlie"
        }
    );
}

#[test]
fn test_all_transformations_combined() {
    let df = df!(
        "db_UserId_col" => [1i32, 2, 3],
        "db_UserName_col" => ["alice", "bob", "charlie"],
        "db_UserEmail_col" => ["a@example.com", "b@example.com", "c@example.com"],
    )
    .unwrap();

    #[derive(Debug, FromDataFrameRow, PartialEq)]
    #[from_dataframe(prefix("db_"), convert_case(Pascal), postfix("_col"))]
    struct Row<'a> {
        user_id: i32,
        user_name: &'a str,
        user_email: &'a str,
    }

    let rows: Vec<Row> = df.rows_iter::<Row>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        Row {
            user_id: 1,
            user_name: "alice",
            user_email: "a@example.com"
        }
    );
    assert_eq!(
        rows[1],
        Row {
            user_id: 2,
            user_name: "bob",
            user_email: "b@example.com"
        }
    );
    assert_eq!(
        rows[2],
        Row {
            user_id: 3,
            user_name: "charlie",
            user_email: "c@example.com"
        }
    );
}

#[test]
fn test_prefix_with_case_conversion() {
    let df = df!(
        "tbl_UserId" => [1i32, 2, 3],
        "tbl_IsActive" => [true, false, true],
    )
    .unwrap();

    #[derive(Debug, FromDataFrameRow, PartialEq)]
    #[from_dataframe(prefix("tbl_"), convert_case(Pascal))]
    struct Row {
        user_id: i32,
        is_active: bool,
    }

    let rows: Vec<Row> = df.rows_iter::<Row>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        Row {
            user_id: 1,
            is_active: true
        }
    );
    assert_eq!(
        rows[1],
        Row {
            user_id: 2,
            is_active: false
        }
    );
    assert_eq!(
        rows[2],
        Row {
            user_id: 3,
            is_active: true
        }
    );
}

#[test]
fn test_postfix_with_case_conversion() {
    let df = df!(
        "userId_field" => [1i32, 2, 3],
        "userAge_field" => [25i32, 30, 35],
    )
    .unwrap();

    #[derive(Debug, FromDataFrameRow, PartialEq)]
    #[from_dataframe(convert_case(Camel), postfix("_field"))]
    struct Row {
        user_id: i32,
        user_age: i32,
    }

    let rows: Vec<Row> = df.rows_iter::<Row>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        Row {
            user_id: 1,
            user_age: 25
        }
    );
    assert_eq!(
        rows[1],
        Row {
            user_id: 2,
            user_age: 30
        }
    );
    assert_eq!(
        rows[2],
        Row {
            user_id: 3,
            user_age: 35
        }
    );
}

// NOTE: Currently #[column] attribute does NOT override struct-level transformations
// This is expected behavior - transformations apply to the column name expression
// If you need to override, you need to account for transformations in the #[column] value
// This test documents the current behavior
#[test]
fn test_column_attribute_with_transformations() {
    let df = df!(
        "prefix_custom_column_suffix" => [1i32, 2, 3],
        "prefix_normal_field_suffix" => [10.0f64, 20.0, 30.0],
    )
    .unwrap();

    #[derive(Debug, FromDataFrameRow, PartialEq)]
    #[from_dataframe(prefix("prefix_"), postfix("_suffix"))]
    struct Row {
        #[column("custom_column")]
        special: i32,
        normal_field: f64,
    }

    let rows: Vec<Row> = df.rows_iter::<Row>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        Row {
            special: 1,
            normal_field: 10.0
        }
    );
    assert_eq!(
        rows[1],
        Row {
            special: 2,
            normal_field: 20.0
        }
    );
    assert_eq!(
        rows[2],
        Row {
            special: 3,
            normal_field: 30.0
        }
    );
}

#[test]
fn test_optional_fields_with_transformations() {
    let df = df!(
        "db_id_col" => [1i32, 2, 3],
        "db_value_col" => [Some(10.0f64), None, Some(30.0)],
    )
    .unwrap();

    #[derive(Debug, FromDataFrameRow, PartialEq)]
    #[from_dataframe(prefix("db_"), postfix("_col"))]
    struct Row {
        id: i32,
        value: Option<f64>,
    }

    let rows: Vec<Row> = df.rows_iter::<Row>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        Row {
            id: 1,
            value: Some(10.0)
        }
    );
    assert_eq!(rows[1], Row { id: 2, value: None });
    assert_eq!(
        rows[2],
        Row {
            id: 3,
            value: Some(30.0)
        }
    );
}

#[test]
fn test_case_conversion_camel() {
    let df = df!(
        "userId" => [1i32, 2, 3],
        "isActive" => [true, false, true],
    )
    .unwrap();

    #[derive(Debug, FromDataFrameRow, PartialEq)]
    #[from_dataframe(convert_case(Camel))]
    struct Row {
        user_id: i32,
        is_active: bool,
    }

    let rows: Vec<Row> = df.rows_iter::<Row>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0],
        Row {
            user_id: 1,
            is_active: true
        }
    );
    assert_eq!(
        rows[1],
        Row {
            user_id: 2,
            is_active: false
        }
    );
    assert_eq!(
        rows[2],
        Row {
            user_id: 3,
            is_active: true
        }
    );
}

#[test]
fn test_empty_dataframe_with_transformations() {
    let df = df!(
        "prefix_id_suffix" => Vec::<i32>::new(),
        "prefix_value_suffix" => Vec::<f64>::new(),
    )
    .unwrap();

    #[derive(Debug, FromDataFrameRow, PartialEq)]
    #[from_dataframe(prefix("prefix_"), postfix("_suffix"))]
    struct Row {
        id: i32,
        value: f64,
    }

    let rows: Vec<Row> = df.rows_iter::<Row>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(rows.len(), 0);
}

#[test]
fn test_generic_struct_with_transformations() {
    let df = df!(
        "col_id" => [1i32, 2, 3],
        "col_value" => [100i32, 200, 300],
    )
    .unwrap();

    #[derive(Debug, FromDataFrameRow, PartialEq)]
    #[from_dataframe(prefix("col_"))]
    struct Row<T> {
        id: T,
        value: T,
    }

    let rows: Vec<Row<i32>> = df.rows_iter::<Row<i32>>().unwrap().map(|r| r.unwrap()).collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], Row { id: 1, value: 100 });
    assert_eq!(rows[1], Row { id: 2, value: 200 });
    assert_eq!(rows[2], Row { id: 3, value: 300 });
}
