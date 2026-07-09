use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};

use crate::models::{ErpItem, Product, PumSeed, PumUpdate};
use crate::utils::{
    csv_fields, normalize_pum_unit_price, normalize_unit, normalized_numeric_key,
    parse_amount_token, parse_decimal,
};

pub fn load_pum_plan(path: &str) -> Result<HashMap<String, PumSeed>, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut rows = HashMap::new();

    for (index, line_result) in reader.lines().enumerate() {
        let line = line_result?;
        if index == 0 || line.trim().is_empty() {
            continue;
        }

        let fields = csv_fields(&line);
        if fields.len() < 4 {
            continue;
        }

        let reference = fields[0].trim();
        let unity = fields[2].trim();
        let ratio = parse_decimal(&fields[3]).unwrap_or(0.0);
        if reference.is_empty() || unity.is_empty() || ratio <= 0.0 {
            continue;
        }

        let seed = PumSeed {
            unity: unity.to_string(),
            ratio,
            source: "PLANILLA_PUM".to_string(),
        };
        rows.insert(reference.to_string(), seed.clone());
        if let Some(normalized) = normalized_numeric_key(reference) {
            rows.insert(normalized, seed);
        }
    }

    Ok(rows)
}

pub fn normalize_pum_unity(value: &str) -> String {
    let unit = normalize_unit(value);
    match unit.as_str() {
        "G" => "Gramo".to_string(),
        "KG" => "Kilogramo".to_string(),
        "ML" => "Mililitro".to_string(),
        "L" => "Litro".to_string(),
        "UND" | "UN" => "Unidad".to_string(),
        _ => value.trim().to_string(),
    }
}

pub fn find_pum_plan_seed(
    plan: &HashMap<String, PumSeed>,
    product_ref: &str,
    erp_productoid: &str,
) -> Option<PumSeed> {
    let keys = [product_ref.trim(), erp_productoid.trim()];
    for key in keys {
        if key.is_empty() {
            continue;
        }
        if let Some(seed) = plan.get(key) {
            return Some(seed.clone());
        }
        if let Some(normalized) = normalized_numeric_key(key) {
            if let Some(seed) = plan.get(&normalized) {
                return Some(seed.clone());
            }
        }
    }

    None
}

pub fn erp_pum_seed(item: &ErpItem) -> Option<PumSeed> {
    let ratio = item.pum_content?;
    let unity = normalize_pum_unity(&item.pum_unit);
    if unity.is_empty() || ratio <= 0.0 {
        return None;
    }

    Some(PumSeed {
        unity,
        ratio,
        source: "ERP_PUM".to_string(),
    })
}

pub fn infer_pum_seed(name: &str) -> Option<PumSeed> {
    let text = name.to_lowercase();
    let tokens: Vec<&str> = text
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == ',' || c == '.'))
        .filter(|s| !s.is_empty())
        .collect();

    let suffixes = [
        ("mililitros", "Mililitro"),
        ("mililitro", "Mililitro"),
        ("ml", "Mililitro"),
        ("litros", "Litro"),
        ("litro", "Litro"),
        ("lt", "Litro"),
        ("gramos", "Gramo"),
        ("gramo", "Gramo"),
        ("gr", "Gramo"),
        ("g", "Gramo"),
        ("kilogramos", "Kilogramo"),
        ("kilogramo", "Kilogramo"),
        ("kilos", "Kilogramo"),
        ("kilo", "Kilogramo"),
        ("kg", "Kilogramo"),
        ("unidades", "Unidad"),
        ("unidad", "Unidad"),
        ("unds", "Unidad"),
        ("und", "Unidad"),
        ("u", "Unidad"),
    ];

    for (index, token) in tokens.iter().enumerate() {
        for (suffix, unity) in suffixes {
            if let Some(number_part) = token.strip_suffix(suffix) {
                if let Some(ratio) = parse_amount_token(number_part).filter(|value| *value > 0.0) {
                    return Some(PumSeed {
                        unity: unity.to_string(),
                        ratio,
                        source: "INFERIDO_NOMBRE".to_string(),
                    });
                }
            }
        }

        if let Some(ratio) = parse_amount_token(token).filter(|value| *value > 0.0) {
            if let Some(next) = tokens.get(index + 1) {
                for (suffix, unity) in suffixes {
                    if *next == suffix {
                        return Some(PumSeed {
                            unity: unity.to_string(),
                            ratio,
                            source: "INFERIDO_NOMBRE".to_string(),
                        });
                    }
                }
            }
        }
    }

    None
}

pub fn resolve_pum_update(
    product: &Product,
    erp_item: &ErpItem,
    final_price: Option<f64>,
    plan: &HashMap<String, PumSeed>,
) -> Option<PumUpdate> {
    let price = final_price?;
    let seed = find_pum_plan_seed(plan, &product.reference, &erp_item.productoid)
        .or_else(|| erp_pum_seed(erp_item))
        .or_else(|| infer_pum_seed(&erp_item.name))
        .or_else(|| infer_pum_seed(&product.name))
        .or_else(|| {
            product
                .current_unit_price_ratio
                .filter(|ratio| *ratio > 0.0)
                .map(|ratio| PumSeed {
                    unity: product.current_unity.clone(),
                    ratio,
                    source: "PRESTASHOP_EXISTENTE".to_string(),
                })
        })?;

    if seed.ratio <= 0.0 {
        return None;
    }

    Some(PumUpdate {
        unity: seed.unity,
        ratio: seed.ratio,
        unit_price: normalize_pum_unit_price(price / seed.ratio),
        source: seed.source,
    })
}

pub fn pum_is_different(product: &Product, pum: &PumUpdate) -> bool {
    product.current_unity.trim() != pum.unity.trim()
        || product
            .current_unit_price_ratio
            .map(|ratio| (ratio - pum.ratio).abs() >= 0.000001)
            .unwrap_or(true)
        || product
            .current_unit_price
            .map(|price| (price - pum.unit_price).abs() >= 0.005)
            .unwrap_or(true)
}
