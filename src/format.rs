use crate::error::Result;
use crate::output::{DocOutput, FireqlOutput};
use crate::value::FireqlValue;
use futures::StreamExt;
use std::collections::HashMap;

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

/// CSV headers use only the first row's fields so rows can be written without
/// scanning the full result set first (#28). Later rows omit values for fields
/// that appear only on the first row, and fields unique to later rows are not
/// emitted as columns. Table output still unions all fields for display.
fn collect_field_names_first_row(rows: &[DocOutput]) -> Vec<String> {
    match rows.first() {
        Some(row) => {
            let mut names: Vec<String> = row.data.keys().cloned().collect();
            names.sort();
            names
        }
        None => Vec::new(),
    }
}

fn aggregation_row(map: &HashMap<String, FireqlValue>) -> (Vec<&String>, Vec<String>) {
    let mut keys: Vec<&String> = map.keys().collect();
    keys.sort();
    let values = keys.iter().map(|k| map[*k].to_plain_string()).collect();
    (keys, values)
}

/// Spreadsheet apps execute cells starting with '=', '+', '-', '@', TAB or CR
/// as formulas, so exported CSV can trigger code execution when opened
/// (CSV injection). Only string and reference cells are escaped, since they
/// carry arbitrary author text. Numeric and JSON-encoded cells have
/// fireql-controlled leading characters, and base64 bytes stay unescaped to
/// remain round-trippable: a leading '+' only yields a harmless `#NAME?` cell
/// because the base64 alphabet cannot form an executable formula payload.
fn escape_formula_cell(text: String) -> String {
    match text.as_bytes().first() {
        Some(b'=' | b'+' | b'-' | b'@' | b'\t' | b'\r') => format!("'{text}"),
        _ => text,
    }
}

fn row_record(row: &DocOutput, field_names: &[String], escape_formulas: bool) -> Vec<String> {
    let escape = |text: String| {
        if escape_formulas {
            escape_formula_cell(text)
        } else {
            text
        }
    };

    let mut record = vec![escape(row.id.clone()), escape(row.path.clone())];
    for field in field_names {
        let cell = match row.data.get(field) {
            Some(v @ (FireqlValue::String(_) | FireqlValue::Reference(_))) => {
                escape(v.to_plain_string())
            }
            Some(v) => v.to_plain_string(),
            None => String::new(),
        };
        record.push(cell);
    }
    record
}

fn build_row_data(
    rows: &[DocOutput],
    field_names: Vec<String>,
    escape_formulas: bool,
) -> (Vec<String>, Vec<Vec<String>>) {
    // Headers are fireql-generated (`id`, `path`, `data.{field}`); the prefix
    // keeps them outside CSV formula-injection rules, unlike document values.
    let mut header = vec!["id".to_string(), "path".to_string()];
    header.extend(field_names.iter().map(|f| format!("data.{f}")));

    let data_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row_record(row, &field_names, escape_formulas))
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
            let field_names = collect_field_names_first_row(rows);
            let (header, data_rows) = build_row_data(rows, field_names, true);
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
            let (keys, values) = aggregation_row(map);
            wtr.write_record(keys.iter().map(|k| k.as_str()))
                .map_err(csv_error)?;
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

/// Writes SELECT rows as CSV using the first row's field set as the header.
/// Suitable for streaming: once the header is written, each subsequent row is
/// written and flushed independently without a full-field union scan, so
/// consumers observe rows as they are handed to the writer (#28, #55).
pub fn write_csv_rows<W: std::io::Write>(
    rows: impl IntoIterator<Item = DocOutput>,
    mut out: W,
) -> Result<()> {
    let mut rows = rows.into_iter();
    let Some(first) = rows.next() else {
        return Ok(());
    };

    let field_names = first_row_field_names(&first);
    let header = csv_header(&field_names);

    let mut wtr = csv::Writer::from_writer(&mut out);
    wtr.write_record(&header).map_err(csv_error)?;
    wtr.write_record(row_record(&first, &field_names, true))
        .map_err(csv_error)?;
    flush_csv(&mut wtr)?;
    for row in rows {
        wtr.write_record(row_record(&row, &field_names, true))
            .map_err(csv_error)?;
        flush_csv(&mut wtr)?;
    }
    Ok(())
}

fn first_row_field_names(row: &DocOutput) -> Vec<String> {
    let mut names: Vec<String> = row.data.keys().cloned().collect();
    names.sort();
    names
}

fn csv_header(field_names: &[String]) -> Vec<String> {
    // Headers are fireql-generated (`id`, `path`, `data.{field}`); the prefix
    // keeps them outside CSV formula-injection rules, unlike document values.
    let mut header = vec!["id".to_string(), "path".to_string()];
    header.extend(field_names.iter().map(|f| format!("data.{f}")));
    header
}

fn flush_csv<W: std::io::Write>(wtr: &mut csv::Writer<W>) -> Result<()> {
    wtr.flush()
        .map_err(|e| crate::error::FireqlError::Format(e.to_string()))
}

/// Streams SELECT rows as CSV as they arrive, flushing after every row so
/// downstream consumers observe documents incrementally (#55). Uses the first
/// row's field set as the header (#28) and produces the same bytes as
/// [`write_csv_rows`]. Writes nothing for an empty stream.
pub async fn write_csv_rows_stream<S, W>(mut rows: S, out: W) -> Result<()>
where
    S: futures::Stream<Item = Result<DocOutput>> + Unpin,
    W: std::io::Write,
{
    let mut wtr = csv::Writer::from_writer(out);
    let mut field_names = Vec::new();
    let mut first = true;

    while let Some(row) = rows.next().await {
        let row = row?;
        if first {
            field_names = first_row_field_names(&row);
            wtr.write_record(csv_header(&field_names))
                .map_err(csv_error)?;
            first = false;
        }
        wtr.write_record(row_record(&row, &field_names, true))
            .map_err(csv_error)?;
        flush_csv(&mut wtr)?;
    }
    Ok(())
}

/// Streams SELECT rows as a JSON array, writing and flushing each element as
/// it arrives (#55). Byte-identical to `Format::Json.format(&FireqlOutput::Rows(..), pretty)`
/// for the same rows in both compact and pretty modes. Does not write a
/// trailing newline.
pub async fn write_json_rows_stream<S, W>(mut rows: S, mut out: W, pretty: bool) -> Result<()>
where
    S: futures::Stream<Item = Result<DocOutput>> + Unpin,
    W: std::io::Write,
{
    if let Some(first) = rows.next().await {
        let first = first?;
        let sep: &[u8] = if pretty { b",\n" } else { b"," };
        let close: &[u8] = if pretty { b"\n]" } else { b"]" };

        if pretty {
            out.write_all(b"[\n")?;
            out.write_all(
                indent_pretty_element(&serde_json::to_string_pretty(&first)?).as_bytes(),
            )?;
        } else {
            out.write_all(b"[")?;
            out.write_all(serde_json::to_string(&first)?.as_bytes())?;
        }
        out.flush()?;

        while let Some(row) = rows.next().await {
            let row = row?;
            out.write_all(sep)?;
            if pretty {
                out.write_all(
                    indent_pretty_element(&serde_json::to_string_pretty(&row)?).as_bytes(),
                )?;
            } else {
                out.write_all(serde_json::to_string(&row)?.as_bytes())?;
            }
            out.flush()?;
        }
        out.write_all(close)?;
    } else {
        out.write_all(b"[]")?;
    }
    out.flush()?;
    Ok(())
}

/// Indents a pretty-printed JSON element by two spaces so it matches how
/// `serde_json` renders elements inside a pretty-printed array.
fn indent_pretty_element(json: &str) -> String {
    let mut indented = String::with_capacity(json.len());
    for line in json.lines() {
        indented.push_str("  ");
        indented.push_str(line);
        indented.push('\n');
    }
    // Drop the trailing newline; the caller joins elements with ",\n".
    indented.pop();
    indented
}

/// Firestore strings may embed ANSI/OSC escape sequences that rewrite the
/// operator's terminal when rendered. Table output is display-only, so drop
/// control characters (keeping newline and tab, which comfy-table renders
/// safely) before drawing. JSON already escapes them and CSV must stay
/// byte-faithful for machine consumers.
fn strip_control_chars(text: &str) -> String {
    text.chars()
        .filter(|c| !c.is_control() || matches!(c, '\n' | '\t'))
        .collect()
}

fn format_table(output: &FireqlOutput) -> Result<String> {
    use comfy_table::presets::ASCII_FULL;
    use comfy_table::{ContentArrangement, Table};

    let new_table = || {
        let mut table = Table::new();
        table.load_style(ASCII_FULL);
        table.set_content_arrangement(ContentArrangement::Dynamic);
        table
    };

    match output {
        FireqlOutput::Rows(rows) => {
            if rows.is_empty() {
                return Ok(String::new());
            }
            let field_names = collect_field_names(rows);
            let (header, data_rows) = build_row_data(rows, field_names, false);

            let mut table = new_table();
            table.set_header(header.iter().map(|h| strip_control_chars(h)));
            for cells in data_rows {
                table.add_row(cells.iter().map(|c| strip_control_chars(c)));
            }
            Ok(table.to_string())
        }
        FireqlOutput::Affected { affected } => {
            let mut table = new_table();
            table.set_header(["affected"]);
            table.add_row([affected.to_string()]);
            Ok(table.to_string())
        }
        FireqlOutput::Aggregation(map) => {
            if map.is_empty() {
                return Ok(String::new());
            }
            let (keys, values) = aggregation_row(map);
            let mut table = new_table();
            table.set_header(keys.iter().map(|k| strip_control_chars(k)));
            table.add_row(values.iter().map(|v| strip_control_chars(v)));
            Ok(table.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::FireqlError;
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
    fn csv_heterogeneous_fields_uses_first_row() {
        let output = multi_rows_heterogeneous();
        let result = Format::Csv.format(&output, false).unwrap();
        let lines: Vec<&str> = result.trim().lines().collect();
        assert_eq!(lines.len(), 3);
        // First row has age+name; Bob's email is omitted from the header.
        assert_eq!(lines[0], "id,path,data.age,data.name");
        assert_eq!(lines[1], "u1,users/u1,30,Alice");
        assert_eq!(lines[2], "u2,users/u2,,Bob");
    }

    #[test]
    fn write_csv_rows_streams_with_first_row_header() {
        let FireqlOutput::Rows(rows) = multi_rows_heterogeneous() else {
            panic!("expected rows");
        };
        let mut buf = Vec::new();
        write_csv_rows(rows, &mut buf).unwrap();
        let result = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = result.trim().lines().collect();
        assert_eq!(lines[0], "id,path,data.age,data.name");
        assert_eq!(lines[1], "u1,users/u1,30,Alice");
        assert_eq!(lines[2], "u2,users/u2,,Bob");
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

    #[test]
    fn csv_string_formula_cell_is_escaped() {
        let mut data = HashMap::new();
        data.insert(
            "note".to_string(),
            FireqlValue::String("=HYPERLINK(\"http://evil\")".to_string()),
        );
        let output = FireqlOutput::Rows(vec![DocOutput {
            id: "d1".to_string(),
            path: "c/d1".to_string(),
            data,
        }]);
        let result = Format::Csv.format(&output, false).unwrap();
        let mut rdr = csv::Reader::from_reader(result.as_bytes());
        let record = rdr.records().next().unwrap().unwrap();
        assert_eq!(record.get(2).unwrap(), "'=HYPERLINK(\"http://evil\")");
    }

    #[test]
    fn csv_negative_integer_is_not_escaped() {
        let mut data = HashMap::new();
        data.insert("delta".to_string(), FireqlValue::Integer(-5));
        let output = FireqlOutput::Rows(vec![DocOutput {
            id: "d1".to_string(),
            path: "c/d1".to_string(),
            data,
        }]);
        let result = Format::Csv.format(&output, false).unwrap();
        let lines: Vec<&str> = result.trim().lines().collect();
        assert_eq!(lines[1], "d1,c/d1,-5");
    }

    #[test]
    fn table_formula_cell_is_not_escaped() {
        let mut data = HashMap::new();
        data.insert(
            "note".to_string(),
            FireqlValue::String("=SUM(A1)".to_string()),
        );
        let output = FireqlOutput::Rows(vec![DocOutput {
            id: "d1".to_string(),
            path: "c/d1".to_string(),
            data,
        }]);
        let result = Format::Table.format(&output, false).unwrap();
        assert!(result.contains("=SUM(A1)"));
        assert!(!result.contains("'=SUM(A1)"));
    }

    #[test]
    fn table_control_chars_are_stripped() {
        let mut data = HashMap::new();
        data.insert(
            "note".to_string(),
            FireqlValue::String("\u{1b}]8;;http://evil\u{7}click me".to_string()),
        );
        let output = FireqlOutput::Rows(vec![DocOutput {
            id: "d1".to_string(),
            path: "c/d1".to_string(),
            data,
        }]);
        let result = Format::Table.format(&output, false).unwrap();
        assert!(!result.contains('\u{1b}'));
        assert!(!result.contains('\u{7}'));
        assert!(result.contains("click me"));
    }

    fn streaming_rows(count: usize) -> Vec<DocOutput> {
        (0..count)
            .map(|i| {
                let mut data = HashMap::new();
                data.insert("name".to_string(), FireqlValue::String(format!("n{i}")));
                data.insert("age".to_string(), FireqlValue::Integer(i as i64));
                DocOutput {
                    id: format!("d{i}"),
                    path: format!("c/d{i}"),
                    data,
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn write_json_rows_stream_matches_buffered_format() {
        for pretty in [false, true] {
            for count in [0, 1, 3] {
                let rows = streaming_rows(count);
                let expected = format_json(&FireqlOutput::Rows(rows.clone()), pretty).unwrap();

                let mut buf = Vec::new();
                write_json_rows_stream(
                    futures::stream::iter(rows.into_iter().map(Ok)),
                    &mut buf,
                    pretty,
                )
                .await
                .unwrap();

                assert_eq!(
                    String::from_utf8(buf).unwrap(),
                    expected,
                    "pretty={pretty} count={count}"
                );
            }
        }
    }

    #[tokio::test]
    async fn write_json_rows_stream_surfaces_stream_errors() {
        let rows: Vec<Result<DocOutput>> = vec![Err(FireqlError::Unsupported("boom".into()))];
        let mut buf = Vec::new();
        let err = write_json_rows_stream(futures::stream::iter(rows), &mut buf, false)
            .await
            .unwrap_err();
        assert!(matches!(err, FireqlError::Unsupported(_)));
    }

    #[tokio::test]
    async fn write_csv_rows_stream_matches_write_csv_rows() {
        for count in [0, 1, 3] {
            let rows = streaming_rows(count);

            let mut expected = Vec::new();
            write_csv_rows(rows.clone(), &mut expected).unwrap();

            let mut buf = Vec::new();
            write_csv_rows_stream(futures::stream::iter(rows.into_iter().map(Ok)), &mut buf)
                .await
                .unwrap();

            assert_eq!(buf, expected, "count={count}");
        }
    }

    /// Records `flush` calls so tests can verify rows become visible to the
    /// underlying writer one by one.
    struct FlushRecorder {
        inner: Vec<u8>,
        flushes: usize,
    }

    impl FlushRecorder {
        fn new() -> Self {
            Self {
                inner: Vec::new(),
                flushes: 0,
            }
        }
    }

    impl std::io::Write for FlushRecorder {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.inner.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn write_csv_rows_stream_flushes_after_each_row() {
        let rows = streaming_rows(3);
        let mut recorder = FlushRecorder::new();
        write_csv_rows_stream(
            futures::stream::iter(rows.into_iter().map(Ok)),
            &mut recorder,
        )
        .await
        .unwrap();

        // Header + first row, then one flush per subsequent row.
        assert!(
            recorder.flushes >= 3,
            "expected at least one flush per row, got {}",
            recorder.flushes
        );
    }

    #[tokio::test]
    async fn write_json_rows_stream_flushes_after_each_element() {
        let rows = streaming_rows(3);
        let mut recorder = FlushRecorder::new();
        write_json_rows_stream(
            futures::stream::iter(rows.into_iter().map(Ok)),
            &mut recorder,
            false,
        )
        .await
        .unwrap();

        assert!(
            recorder.flushes >= 3,
            "expected at least one flush per element, got {}",
            recorder.flushes
        );
    }
}
