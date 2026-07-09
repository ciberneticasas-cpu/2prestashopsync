use crate::models::ErpItem;
use std::collections::{HashMap, HashSet};

pub fn normalized_numeric_key(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.len() <= 1 || !trimmed.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }

    let normalized = trimmed.trim_start_matches('0');
    if normalized.is_empty() || normalized == trimmed {
        None
    } else {
        Some(normalized.to_string())
    }
}

pub fn insert_erp_match_key(
    map: &mut HashMap<String, ErpItem>,
    ambiguous: &mut HashSet<String>,
    key: &str,
    item: &ErpItem,
) {
    let key = key.trim();
    if key.is_empty() || ambiguous.contains(key) {
        return;
    }

    match map.get(key) {
        Some(existing) if existing.productoid != item.productoid => {
            map.remove(key);
            ambiguous.insert(key.to_string());
        }
        Some(_) => {}
        None => {
            map.insert(key.to_string(), item.clone());
        }
    }
}

pub fn insert_erp_match_key_with_normalized(
    map: &mut HashMap<String, ErpItem>,
    ambiguous: &mut HashSet<String>,
    key: &str,
    item: &ErpItem,
) {
    insert_erp_match_key(map, ambiguous, key, item);
    if let Some(normalized) = normalized_numeric_key(key) {
        insert_erp_match_key(map, ambiguous, &normalized, item);
    }
}

pub fn fmt_qty(value: Option<i32>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "SIN_MATCH".to_string())
}

pub fn fmt_qty_decimal(value: Option<f64>) -> String {
    value
        .map(|v| {
            if (v.fract()).abs() < 0.000001 {
                format!("{:.0}", v)
            } else {
                format!("{:.2}", v)
            }
        })
        .unwrap_or_else(|| "SIN_MATCH".to_string())
}

pub fn fmt_price(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "SIN_PRECIO".to_string())
}

pub fn fmt_decimal(value: Option<f64>) -> String {
    value.map(|v| format!("{:.6}", v)).unwrap_or_default()
}

pub fn fmt_pum_unit_price(value: Option<f64>) -> String {
    value
        .map(|v| {
            if v > 10.0 {
                format!("{:.0}", v)
            } else {
                format!("{:.1}", v)
            }
        })
        .unwrap_or_default()
}

pub fn price_is_different(current: f64, target: Option<f64>) -> bool {
    target
        .map(|price| (current - price).abs() >= 0.005)
        .unwrap_or(false)
}

pub fn price_without_tax(price_with_tax: f64, ivaid: f64) -> f64 {
    let tax_factor = if ivaid >= 2.0 {
        1.0 + (ivaid / 100.0)
    } else if ivaid > 1.0 {
        ivaid
    } else if ivaid > 0.0 {
        1.0 + ivaid
    } else {
        1.0
    };

    price_with_tax / tax_factor
}

pub fn normalize_pum_unit_price(value: f64) -> f64 {
    if value > 10.0 {
        value.round()
    } else {
        (value * 10.0).round() / 10.0
    }
}

pub fn normalize_unit(value: &str) -> String {
    let unit = value.trim().to_uppercase();
    match unit.as_str() {
        "KL" | "KG" | "KILO" | "KILOS" | "KILOGRAMO" | "KILOGRAMOS" => "KG".to_string(),
        "GR" | "G" | "GRAMO" | "GRAMOS" => "G".to_string(),
        "LT" | "L" | "LITRO" | "LITROS" => "L".to_string(),
        "ML" | "MILILITRO" | "MILILITROS" => "ML".to_string(),
        "UND" | "UN" | "UNIDAD" | "UNIDADES" => "UND".to_string(),
        "PAQ" | "PQT" | "PAQUETE" => "PAQ".to_string(),
        _ => unit,
    }
}

pub fn parse_amount_token(value: &str) -> Option<f64> {
    let cleaned = value
        .trim_start_matches('x')
        .trim_start_matches('X')
        .replace(',', ".");
    cleaned.parse::<f64>().ok()
}

pub fn parse_decimal(value: &str) -> Option<f64> {
    value.trim().replace(',', ".").parse::<f64>().ok()
}

pub fn csv_fields(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                current.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    fields.push(current.trim().to_string());
    fields
}

pub fn csv_escape(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

pub fn sql_string(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('\'', "''");
    format!("'{}'", escaped)
}
