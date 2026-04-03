use crate::error::Result;
use crate::output::{DocOutput, FireqlOutput};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum Format {
    #[default]
    Json,
    Csv,
    Table,
}

impl Format {
    pub fn format(&self, output: &FireqlOutput, pretty: bool) -> Result<String> {
        match self {
            Self::Json => format_json(output, pretty),
            Self::Csv => format_csv(output),
            Self::Table => format_table(output),
        }
    }
}

fn format_json(output: &FireqlOutput, pretty: bool) -> Result<String> {
    if pretty {
        Ok(serde_json::to_string_pretty(output)?)
    } else {
        Ok(serde_json::to_string(output)?)
    }
}

fn collect_field_names(rows: &[DocOutput]) -> Vec<String> {
    let mut names = std::collections::BTreeSet::new();
    for row in rows {
        names.extend(row.data.keys().cloned());
    }
    names.into_iter().collect()
}

fn build_row_data(rows: &[DocOutput]) -> (Vec<String>, Vec<Vec<String>>) {
    let field_names = collect_field_names(rows);
    let mut header = vec!["id".to_string(), "path".to_string()];
    header.extend(field_names.iter().map(|f| format!("data.{f}")));

    let data_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            let mut record = vec![row.id.clone(), row.path.clone()];
            for field in &field_names {
                let value = row
                    .data
                    .get(field)
                    .map(|v| v.to_plain_string())
                    .unwrap_or_default();
                record.push(value);
            }
            record
        })
        .collect();

    (header, data_rows)
}

fn format_csv(output: &FireqlOutput) -> Result<String> {
    let mut wtr = csv::Writer::from_writer(vec![]);

    match output {
        FireqlOutput::Rows(rows) => {
            if rows.is_empty() {
                return Ok(String::new());
            }
            let (header, data_rows) = build_row_data(rows);
            wtr.write_record(&header).map_err(csv_error)?;
            for record in &data_rows {
                wtr.write_record(record).map_err(csv_error)?;
            }
        }
        FireqlOutput::Affected { affected } => {
            wtr.write_record(["affected"]).map_err(csv_error)?;
            wtr.write_record([affected.to_string()])
                .map_err(csv_error)?;
        }
        FireqlOutput::Aggregation(map) => {
            if map.is_empty() {
                return Ok(String::new());
            }
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            wtr.write_record(keys.iter().map(|k| k.as_str()))
                .map_err(csv_error)?;
            let values: Vec<String> = keys.iter().map(|k| map[*k].to_plain_string()).collect();
            wtr.write_record(&values).map_err(csv_error)?;
        }
    }

    let bytes = wtr
        .into_inner()
        .map_err(|e| crate::error::FireqlError::Format(e.into_error().to_string()))?;
    Ok(String::from_utf8(bytes).expect("CSV output is always valid UTF-8"))
}

fn csv_error(e: csv::Error) -> crate::error::FireqlError {
    crate::error::FireqlError::Format(e.to_string())
}

fn format_table(output: &FireqlOutput) -> Result<String> {
    use comfy_table::presets::ASCII_FULL;
    use comfy_table::{ContentArrangement, Table};

    match output {
        FireqlOutput::Rows(rows) => {
            if rows.is_empty() {
                return Ok(String::new());
            }
            let (header, data_rows) = build_row_data(rows);

            let mut table = Table::new();
            table.load_preset(ASCII_FULL);
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(&header);
            for cells in data_rows {
                table.add_row(cells);
            }
            Ok(table.to_string())
        }
        FireqlOutput::Affected { affected } => {
            let mut table = Table::new();
            table.load_preset(ASCII_FULL);
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(["affected"]);
            table.add_row([affected.to_string()]);
            Ok(table.to_string())
        }
        FireqlOutput::Aggregation(map) => {
            if map.is_empty() {
                return Ok(String::new());
            }
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut table = Table::new();
            table.load_preset(ASCII_FULL);
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(keys.iter().map(|k| k.as_str()));
            let values: Vec<String> = keys.iter().map(|k| map[*k].to_plain_string()).collect();
            table.add_row(values);
            Ok(table.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::FireqlValue;
    use std::collections::HashMap;

    fn sample_rows() -> FireqlOutput {
        let mut data = HashMap::new();
        data.insert("name".to_string(), FireqlValue::String("Alice".to_string()));
        data.insert("age".to_string(), FireqlValue::Integer(30));
        FireqlOutput::Rows(vec![DocOutput {
            id: "user1".to_string(),
            path: "users/user1".to_string(),
            data,
        }])
    }

    fn sample_affected() -> FireqlOutput {
        FireqlOutput::Affected { affected: 5 }
    }

    fn sample_aggregation() -> FireqlOutput {
        let mut map = HashMap::new();
        map.insert("count".to_string(), FireqlValue::Integer(42));
        FireqlOutput::Aggregation(map)
    }

    #[test]
    fn json_rows() {
        let output = sample_rows();
        let result = Format::Json.format(&output, false).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.is_array());
        assert_eq!(parsed[0]["id"], "user1");
        assert_eq!(parsed[0]["path"], "users/user1");
        assert_eq!(parsed[0]["data"]["name"]["_firestore_type"], "string");
    }

    #[test]
    fn json_pretty_rows() {
        let output = sample_rows();
        let result = Format::Json.format(&output, true).unwrap();
        assert!(result.contains('\n'));
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.is_array());
    }

    #[test]
    fn json_affected() {
        let output = sample_affected();
        let result = Format::Json.format(&output, false).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["affected"], 5);
    }

    #[test]
    fn json_aggregation() {
        let output = sample_aggregation();
        let result = Format::Json.format(&output, false).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["count"]["_firestore_type"], "integer");
        assert_eq!(parsed["count"]["value"], 42);
    }

    #[test]
    fn csv_rows() {
        let output = sample_rows();
        let result = Format::Csv.format(&output, false).unwrap();
        let lines: Vec<&str> = result.trim().lines().collect();
        assert_eq!(lines.len(), 2);
        let header = lines[0];
        assert!(header.starts_with("id,path,"));
        let data_line = lines[1];
        assert!(data_line.starts_with("user1,users/user1,"));
        assert!(data_line.contains("Alice"));
        assert!(data_line.contains("30"));
    }

    #[test]
    fn csv_rows_column_order_alphabetical() {
        let output = sample_rows();
        let result = Format::Csv.format(&output, false).unwrap();
        let header = result.lines().next().unwrap();
        assert_eq!(header, "id,path,data.age,data.name");
    }

    #[test]
    fn csv_affected() {
        let output = sample_affected();
        let result = Format::Csv.format(&output, false).unwrap();
        let lines: Vec<&str> = result.trim().lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "affected");
        assert_eq!(lines[1], "5");
    }

    #[test]
    fn csv_aggregation() {
        let output = sample_aggregation();
        let result = Format::Csv.format(&output, false).unwrap();
        let lines: Vec<&str> = result.trim().lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "count");
        assert_eq!(lines[1], "42");
    }

    #[test]
    fn csv_empty_rows() {
        let output = FireqlOutput::Rows(vec![]);
        let result = Format::Csv.format(&output, false).unwrap();
        assert_eq!(result.trim(), "");
    }

    #[test]
    fn csv_null_value() {
        let mut data = HashMap::new();
        data.insert("field".to_string(), FireqlValue::Null);
        let output = FireqlOutput::Rows(vec![DocOutput {
            id: "d1".to_string(),
            path: "c/d1".to_string(),
            data,
        }]);
        let result = Format::Csv.format(&output, false).unwrap();
        let lines: Vec<&str> = result.trim().lines().collect();
        assert_eq!(lines[0], "id,path,data.field");
        assert_eq!(lines[1], "d1,c/d1,");
    }

    #[test]
    fn csv_value_with_comma() {
        let mut data = HashMap::new();
        data.insert(
            "desc".to_string(),
            FireqlValue::String("hello, world".to_string()),
        );
        let output = FireqlOutput::Rows(vec![DocOutput {
            id: "d1".to_string(),
            path: "c/d1".to_string(),
            data,
        }]);
        let result = Format::Csv.format(&output, false).unwrap();
        let lines: Vec<&str> = result.trim().lines().collect();
        assert_eq!(lines[1], r#"d1,c/d1,"hello, world""#);
    }

    #[test]
    fn csv_empty_aggregation() {
        let output = FireqlOutput::Aggregation(HashMap::new());
        let result = Format::Csv.format(&output, false).unwrap();
        assert_eq!(result.trim(), "");
    }

    #[test]
    fn table_rows() {
        let output = sample_rows();
        let result = Format::Table.format(&output, false).unwrap();
        assert!(result.contains("id"));
        assert!(result.contains("path"));
        assert!(result.contains("user1"));
        assert!(result.contains("users/user1"));
        assert!(result.contains("Alice"));
        assert!(result.contains("30"));
        assert!(result.contains('|'));
        assert!(result.contains('+'));
    }

    #[test]
    fn table_affected() {
        let output = sample_affected();
        let result = Format::Table.format(&output, false).unwrap();
        assert!(result.contains("affected"));
        assert!(result.contains("5"));
    }

    #[test]
    fn table_aggregation() {
        let output = sample_aggregation();
        let result = Format::Table.format(&output, false).unwrap();
        assert!(result.contains("count"));
        assert!(result.contains("42"));
    }

    #[test]
    fn table_empty_rows() {
        let output = FireqlOutput::Rows(vec![]);
        let result = Format::Table.format(&output, false).unwrap();
        assert_eq!(result.trim(), "");
    }

    #[test]
    fn table_empty_aggregation() {
        let output = FireqlOutput::Aggregation(HashMap::new());
        let result = Format::Table.format(&output, false).unwrap();
        assert_eq!(result.trim(), "");
    }

    fn multi_rows_heterogeneous() -> FireqlOutput {
        let mut data1 = HashMap::new();
        data1.insert("name".to_string(), FireqlValue::String("Alice".to_string()));
        data1.insert("age".to_string(), FireqlValue::Integer(30));

        let mut data2 = HashMap::new();
        data2.insert("name".to_string(), FireqlValue::String("Bob".to_string()));
        data2.insert(
            "email".to_string(),
            FireqlValue::String("bob@example.com".to_string()),
        );

        FireqlOutput::Rows(vec![
            DocOutput {
                id: "u1".to_string(),
                path: "users/u1".to_string(),
                data: data1,
            },
            DocOutput {
                id: "u2".to_string(),
                path: "users/u2".to_string(),
                data: data2,
            },
        ])
    }

    #[test]
    fn csv_heterogeneous_fields_uses_union() {
        let output = multi_rows_heterogeneous();
        let result = Format::Csv.format(&output, false).unwrap();
        let lines: Vec<&str> = result.trim().lines().collect();
        assert_eq!(lines.len(), 3);
        assert_eq!(lines[0], "id,path,data.age,data.email,data.name");
        // Alice has age but no email
        assert_eq!(lines[1], "u1,users/u1,30,,Alice");
        // Bob has email but no age
        assert_eq!(lines[2], "u2,users/u2,,bob@example.com,Bob");
    }

    #[test]
    fn table_heterogeneous_fields_uses_union() {
        let output = multi_rows_heterogeneous();
        let result = Format::Table.format(&output, false).unwrap();
        assert!(result.contains("age"));
        assert!(result.contains("email"));
        assert!(result.contains("name"));
        assert!(result.contains("Alice"));
        assert!(result.contains("Bob"));
        assert!(result.contains("bob@example.com"));
    }

    #[test]
    fn csv_geopoint_embedded_json() {
        let mut data = HashMap::new();
        data.insert(
            "location".to_string(),
            FireqlValue::GeoPoint {
                latitude: 35.6762,
                longitude: 139.6503,
            },
        );
        let output = FireqlOutput::Rows(vec![DocOutput {
            id: "d1".to_string(),
            path: "c/d1".to_string(),
            data,
        }]);
        let result = Format::Csv.format(&output, false).unwrap();
        let mut rdr = csv::Reader::from_reader(result.as_bytes());
        let record = rdr.records().next().unwrap().unwrap();
        let location = record.get(2).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(location).unwrap();
        assert_eq!(parsed["latitude"], 35.6762);
        assert_eq!(parsed["longitude"], 139.6503);
    }

    #[test]
    fn csv_array_embedded_json() {
        let mut data = HashMap::new();
        data.insert(
            "tags".to_string(),
            FireqlValue::Array(vec![
                FireqlValue::String("rust".to_string()),
                FireqlValue::String("firestore".to_string()),
            ]),
        );
        let output = FireqlOutput::Rows(vec![DocOutput {
            id: "d1".to_string(),
            path: "c/d1".to_string(),
            data,
        }]);
        let result = Format::Csv.format(&output, false).unwrap();
        let mut rdr = csv::Reader::from_reader(result.as_bytes());
        let record = rdr.records().next().unwrap().unwrap();
        assert_eq!(record.get(2).unwrap(), r#"["rust","firestore"]"#);
    }

    #[test]
    fn csv_data_columns_have_data_prefix() {
        let output = sample_rows();
        let result = Format::Csv.format(&output, false).unwrap();
        let header = result.lines().next().unwrap();
        assert_eq!(header, "id,path,data.age,data.name");
    }

    #[test]
    fn table_data_columns_have_data_prefix() {
        let output = sample_rows();
        let result = Format::Table.format(&output, false).unwrap();
        assert!(result.contains("data.age"));
        assert!(result.contains("data.name"));
    }
}
