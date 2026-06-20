use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::time::Instant;

use chrono::Local;
use futures_util::stream::StreamExt;
use mysql_async::{params, prelude::*, Pool};
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

// --- CONFIGURACION DE MARIADB (PrestaShop) ---
const DB_PREFIX: &str = "ps_";
const LOCAL_SYNC_CONFIG: &str = "/opt/2prestashopsync/sync_config.env";
const LOCAL_ENV_CONFIG: &str = "/opt/2prestashopsync/.env";
const DEFAULT_AUDIT_HOSTS: [&str; 2] = ["192.168.0.231", "192.168.123.231"];

#[derive(Debug)]
struct Product {
    id_product: u32,
    name: String,
    ean13: String,
    reference: String,
    current_qty: Option<i32>,
    current_price: Option<f64>,
    current_unity: String,
    current_unit_price: Option<f64>,
    current_unit_price_ratio: Option<f64>,
}

#[derive(Debug, Clone)]
struct ErpConnection {
    port: u16,
    database: String,
    user: String,
    password: String,
}

#[derive(Debug)]
struct ErpStock {
    by_ean: HashMap<String, ErpItem>,
    by_productoid: HashMap<String, ErpItem>,
    by_ref: HashMap<String, ErpItem>,
    items: Vec<ErpItem>,
}

#[derive(Debug, Clone)]
struct ErpItem {
    productoid: String,
    ean13: String,
    reference: String,
    name: String,
    qty: f64,
    unit: String,
    pum_content: Option<f64>,
    pum_unit: String,
    sales_price: Option<f64>,
    price_lists: String,
    erp_group_level_1: String,
    erp_group_level_2: String,
    erp_group_level_4: String,
    erp_group_level_6: String,
    erp_group_level_7: String,
}

fn normalized_numeric_key(value: &str) -> Option<String> {
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

fn insert_erp_match_key(
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

fn insert_erp_match_key_with_normalized(
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

#[derive(Debug)]
struct ProductUpdate {
    id_product: u32,
    current_qty: i32,
    erp_qty: i32,
    pending_qty: i32,
    final_qty: i32,
    erp_key: String,
    current_price: f64,
    final_price: Option<f64>,
    final_pum: Option<PumUpdate>,
    update_stock: bool,
    update_price: bool,
    update_pum: bool,
}

#[derive(Debug, Clone)]
struct ProductInsert {
    item: ErpItem,
    final_qty: i32,
    final_price: f64,
    final_pum: Option<PumUpdate>,
}

#[derive(Debug)]
struct AuditRow {
    id_product_erp: String,
    id_product_mariadb: u32,
    name: String,
    reference: String,
    ean13: String,
    code: String,
    erp_name: String,
    erp_unit: String,
    mariadb_unit: String,
    conversion_factor: f64,
    stock_prod: Option<f64>,
    inventory_for_mariadb: Option<i32>,
    stock_mariadb: i32,
    pending_qty: i32,
    sync_final_qty: Option<i32>,
    price_mariadb: f64,
    price_erp: Option<f64>,
    price_for_mariadb: Option<f64>,
    pum_source: String,
    pum_unity: String,
    pum_ratio: Option<f64>,
    pum_unit_price: Option<f64>,
    price_lists: String,
    erp_group_level_1: String,
    erp_group_level_2: String,
    erp_group_level_4: String,
    erp_group_level_6: String,
    erp_group_level_7: String,
    ps_group: String,
    ps_subgroup: String,
    last_purchase_date: String,
    last_purchase_qty: String,
    last_purchase_value: String,
    action: String,
}

#[derive(Debug, Clone, Default)]
struct PurchaseInfo {
    date: String,
    qty: String,
    value: String,
}

#[derive(Debug, Clone)]
struct PumSeed {
    unity: String,
    ratio: f64,
    source: String,
}

#[derive(Debug, Clone)]
struct PumUpdate {
    unity: String,
    ratio: f64,
    unit_price: f64,
    source: String,
}

#[derive(Debug, Clone)]
struct PrestashopContext {
    id_shop: u32,
    id_lang: u32,
    id_category_home: u32,
    id_category_root: u32,
    id_groups: Vec<u32>,
}

fn config_value(config: &HashMap<String, String>, key: &str, default: &str) -> String {
    config
        .get(key)
        .or_else(|| config.get(&key.to_lowercase()))
        .filter(|s| !s.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| default.to_string())
}

fn read_key_value_file(path: &str, values: &mut HashMap<String, String>) {
    if let Ok(contents) = fs::read_to_string(path) {
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((key, value)) = line.split_once('=') {
                let key = key.trim().to_string();
                let value = value
                    .trim()
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_string();
                values.insert(key.clone(), value.clone());
                values.insert(key.to_lowercase(), value);
            }
        }
    }
}

fn read_local_config() -> HashMap<String, String> {
    let mut values = HashMap::new();
    read_key_value_file(LOCAL_SYNC_CONFIG, &mut values);
    read_key_value_file(LOCAL_ENV_CONFIG, &mut values);

    values
}

fn parse_hosts(value: Option<&String>, sync_host: &str) -> Vec<String> {
    let mut hosts: Vec<String> = value
        .map(|v| v.as_str())
        .unwrap_or("192.168.0.231,192.168.123.231")
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    if hosts.is_empty() {
        hosts = DEFAULT_AUDIT_HOSTS.iter().map(|s| s.to_string()).collect();
    }

    if !hosts.iter().any(|h| h == sync_host) {
        hosts.push(sync_host.to_string());
    }

    hosts
}

fn lookup_stock<'a>(
    stock: &'a ErpStock,
    ean13: &str,
    reference: &str,
) -> (Option<&'a ErpItem>, String, &'static str) {
    if !ean13.is_empty() {
        if let Some(item) = stock.by_ean.get(ean13) {
            return (Some(item), ean13.to_string(), "EAN");
        }
        if let Some(normalized) = normalized_numeric_key(ean13) {
            if let Some(item) = stock.by_ean.get(&normalized) {
                return (Some(item), ean13.to_string(), "EAN");
            }
        }
    }

    if !reference.is_empty() {
        if let Some(item) = stock.by_productoid.get(reference) {
            return (Some(item), reference.to_string(), "PRODUCTOID");
        }
        if let Some(normalized) = normalized_numeric_key(reference) {
            if let Some(item) = stock.by_productoid.get(&normalized) {
                return (Some(item), reference.to_string(), "PRODUCTOID");
            }
        }
        if let Some(item) = stock.by_ref.get(reference) {
            return (Some(item), reference.to_string(), "REF");
        }
        if let Some(normalized) = normalized_numeric_key(reference) {
            if let Some(item) = stock.by_ref.get(&normalized) {
                return (Some(item), reference.to_string(), "REF");
            }
        }
    }

    let code = if !reference.is_empty() {
        reference.to_string()
    } else {
        ean13.to_string()
    };

    (None, code, "SIN_MATCH")
}

async fn load_erp_stock(
    host: &str,
    erp: &ErpConnection,
    almacenes_in_clause: &str,
) -> Result<ErpStock, Box<dyn Error>> {
    let mut mssql_config = Config::new();
    mssql_config.host(host);
    mssql_config.port(erp.port);
    mssql_config.authentication(tiberius::AuthMethod::sql_server(&erp.user, &erp.password));
    mssql_config.database(&erp.database);
    mssql_config.encryption(tiberius::EncryptionLevel::NotSupported);
    //    mssql_config.trust_cert();

    let tcp = TcpStream::connect(mssql_config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut mssql_client = Client::connect(mssql_config, tcp.compat_write()).await?;

    let mssql_query = format!(
        "SELECT \
            TRIM(p.productoid) AS CodigoProducto, \
            TRIM(p.barras) AS CodigoBarras, \
            TRIM(p.barras2) AS CodigoBarras2, \
            TRIM(p.Barras3) AS CodigoBarras3, \
            TRIM(hp.referencia) AS Referencia, \
            TRIM(hp.nombre) AS NombreProducto, \
            TRIM(hp.unidad) AS UnidadERP, \
            CAST(hp.PUMContenidoInterno AS VARCHAR(40)) AS PUMContenidoInterno, \
            TRIM(hp.PUMUnidadMedida) AS PUMUnidadMedida, \
            CONCAT(TRIM(hp.grupoi), CASE WHEN COALESCE(TRIM(g1.nombre), '') <> '' THEN CONCAT(' ', TRIM(g1.nombre)) ELSE '' END) AS GrupoNivel1, \
            CONCAT(TRIM(hp.grupoii), CASE WHEN COALESCE(TRIM(g2.nombre), '') <> '' THEN CONCAT(' ', TRIM(g2.nombre)) ELSE '' END) AS GrupoNivel2, \
            CONCAT(TRIM(hp.grupoiv), CASE WHEN COALESCE(TRIM(g4.nombre), '') <> '' THEN CONCAT(' ', TRIM(g4.nombre)) ELSE '' END) AS GrupoNivel4, \
            CONCAT(TRIM(hp.grupovi), CASE WHEN COALESCE(TRIM(g6.nombre), '') <> '' THEN CONCAT(' ', TRIM(g6.nombre)) ELSE '' END) AS GrupoNivel6, \
            CONCAT(TRIM(hp.Grupo7), CASE WHEN COALESCE(TRIM(g7.nombre), '') <> '' THEN CONCAT(' ', TRIM(g7.nombre)) ELSE '' END) AS GrupoNivel7, \
            CAST(p.valor AS VARCHAR(40)) AS PrecioVenta, \
            CAST(SUM(COALESCE(ip.invenactua, 0)) AS VARCHAR(40)) AS InventarioUnidades \
         FROM Producto p \
         INNER JOIN HeadProd hp ON hp.headprodid = p.headprodid \
         LEFT JOIN Grupo g1 ON g1.grupoid = hp.grupoi AND g1.nivel = 1 \
         LEFT JOIN Grupo g2 ON g2.grupoid = hp.grupoii AND g2.nivel = 2 \
         LEFT JOIN Grupo g4 ON g4.grupoid = hp.grupoiv AND g4.nivel = 4 \
         LEFT JOIN Grupo g6 ON g6.grupoid = hp.grupovi AND g6.nivel = 6 \
         LEFT JOIN Grupo g7 ON g7.grupoid = hp.Grupo7 AND g7.nivel = 7 \
         LEFT JOIN InveProd ip ON ip.productoid = p.productoid AND ip.almacenid IN ({}) \
         GROUP BY p.productoid, p.barras, p.barras2, p.Barras3, hp.referencia, hp.nombre, hp.unidad, hp.PUMContenidoInterno, hp.PUMUnidadMedida, hp.grupoi, g1.nombre, hp.grupoii, g2.nombre, hp.grupoiv, g4.nombre, hp.grupovi, g6.nombre, hp.Grupo7, g7.nombre, p.valor",
        almacenes_in_clause
    );

    let mut select_stream = mssql_client.query(mssql_query, &[]).await?;
    let mut records: Vec<(Vec<String>, String, String, ErpItem)> = Vec::new();
    let mut product_ids: HashSet<String> = HashSet::new();

    while let Some(row_result) = select_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            let productoid = row
                .get::<&str, _>("CodigoProducto")
                .unwrap_or("")
                .trim()
                .to_string();
            let ean = row
                .get::<&str, _>("CodigoBarras")
                .unwrap_or("")
                .trim()
                .to_string();
            let ean2 = row
                .get::<&str, _>("CodigoBarras2")
                .unwrap_or("")
                .trim()
                .to_string();
            let ean3 = row
                .get::<&str, _>("CodigoBarras3")
                .unwrap_or("")
                .trim()
                .to_string();
            let reference = row
                .get::<&str, _>("Referencia")
                .unwrap_or("")
                .trim()
                .to_string();
            let name = row
                .get::<&str, _>("NombreProducto")
                .unwrap_or("")
                .trim()
                .to_string();
            let unit = row
                .get::<&str, _>("UnidadERP")
                .unwrap_or("")
                .trim()
                .to_string();
            let pum_content = row
                .get::<&str, _>("PUMContenidoInterno")
                .and_then(|value| value.trim().replace(',', ".").parse::<f64>().ok())
                .filter(|value| *value > 0.0);
            let pum_unit = row
                .get::<&str, _>("PUMUnidadMedida")
                .unwrap_or("")
                .trim()
                .to_string();
            let erp_group_level_1 = row
                .get::<&str, _>("GrupoNivel1")
                .unwrap_or("")
                .trim()
                .to_string();
            let erp_group_level_2 = row
                .get::<&str, _>("GrupoNivel2")
                .unwrap_or("")
                .trim()
                .to_string();
            let erp_group_level_4 = row
                .get::<&str, _>("GrupoNivel4")
                .unwrap_or("")
                .trim()
                .to_string();
            let erp_group_level_6 = row
                .get::<&str, _>("GrupoNivel6")
                .unwrap_or("")
                .trim()
                .to_string();
            let erp_group_level_7 = row
                .get::<&str, _>("GrupoNivel7")
                .unwrap_or("")
                .trim()
                .to_string();
            let sales_price = row
                .get::<&str, _>("PrecioVenta")
                .and_then(|value| value.trim().replace(',', ".").parse::<f64>().ok());
            let qty = row
                .get::<&str, _>("InventarioUnidades")
                .and_then(|value| value.trim().replace(',', ".").parse::<f64>().ok())
                .unwrap_or(0.0)
                .max(0.0);

            if !productoid.is_empty() {
                product_ids.insert(productoid.clone());
            }

            records.push((
                vec![ean.clone(), ean2, ean3],
                productoid.clone(),
                reference.clone(),
                ErpItem {
                    productoid,
                    ean13: ean.clone(),
                    reference: reference.clone(),
                    name,
                    qty,
                    unit,
                    pum_content,
                    pum_unit,
                    sales_price,
                    price_lists: String::new(),
                    erp_group_level_1,
                    erp_group_level_2,
                    erp_group_level_4,
                    erp_group_level_6,
                    erp_group_level_7,
                },
            ));
        }
    }

    drop(select_stream);

    let price_lists_query = "SELECT \
            CAST(p.productoid AS VARCHAR(50)) AS CodigoProducto, \
            CONCAT('Producto valor3: ', CAST(p.valor3 AS VARCHAR(40))) AS PrecioLista \
         FROM Producto p \
         WHERE p.valor3 IS NOT NULL AND p.valor3 <> 0 \
         UNION ALL \
         SELECT \
            CAST(p.productoid AS VARCHAR(50)) AS CodigoProducto, \
            CONCAT('Producto valor5: ', CAST(p.valor5 AS VARCHAR(40))) AS PrecioLista \
         FROM Producto p \
         WHERE p.valor5 IS NOT NULL AND p.valor5 <> 0 \
         UNION ALL \
         SELECT \
            CAST(lp.ProductoId AS VARCHAR(50)) AS CodigoProducto, \
            CONCAT('Lista ', CAST(lp.Lista AS VARCHAR(20)), ': ', CAST(lp.Valor AS VARCHAR(40))) AS PrecioLista \
         FROM ListaPrecio lp \
         UNION ALL \
         SELECT \
            CAST(lpt.ProductoId AS VARCHAR(50)) AS CodigoProducto, \
            CONCAT('Tercero ', CAST(lpt.ListaPrecioId AS VARCHAR(20)), ' ', COALESCE(hlpt.Nombre, ''), ': ', CAST(lpt.Valor AS VARCHAR(40))) AS PrecioLista \
         FROM ListaPrecioTercero lpt \
         LEFT JOIN HeadListaPrecioTercero hlpt ON hlpt.ListaPrecioId = lpt.ListaPrecioId";

    let mut price_stream = mssql_client.query(price_lists_query, &[]).await?;
    let mut price_lists_by_product: HashMap<String, Vec<String>> = HashMap::new();
    let mut primary_price_by_product: HashMap<String, f64> = HashMap::new();

    while let Some(row_result) = price_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            let productoid = row
                .get::<&str, _>("CodigoProducto")
                .unwrap_or("")
                .trim()
                .to_string();
            if !product_ids.contains(&productoid) {
                continue;
            }

            let price_text = row
                .get::<&str, _>("PrecioLista")
                .unwrap_or("")
                .trim()
                .to_string();
            if !price_text.is_empty() {
                if let Some(value) = price_text.strip_prefix("Lista 1: ") {
                    if let Ok(price) = value.trim().replace(',', ".").parse::<f64>() {
                        primary_price_by_product.insert(productoid.clone(), price);
                    }
                }

                price_lists_by_product
                    .entry(productoid)
                    .or_default()
                    .push(price_text);
            }
        }
    }

    let mut by_ean: HashMap<String, ErpItem> = HashMap::new();
    let mut ambiguous_ean: HashSet<String> = HashSet::new();
    let mut by_productoid: HashMap<String, ErpItem> = HashMap::new();
    let mut ambiguous_productoid: HashSet<String> = HashSet::new();
    let mut by_ref: HashMap<String, ErpItem> = HashMap::new();
    let mut ambiguous_ref: HashSet<String> = HashSet::new();
    let mut items: Vec<ErpItem> = Vec::new();

    for (ean_keys, productoid_key, reference_key, mut item) in records {
        item.price_lists = price_lists_by_product
            .get(&item.productoid)
            .map(|items| items.join(" | "))
            .unwrap_or_default();
        if item.sales_price.is_none() {
            item.sales_price = primary_price_by_product.get(&item.productoid).copied();
        }

        items.push(item.clone());
        for ean_key in ean_keys {
            insert_erp_match_key_with_normalized(&mut by_ean, &mut ambiguous_ean, &ean_key, &item);
        }
        insert_erp_match_key_with_normalized(
            &mut by_productoid,
            &mut ambiguous_productoid,
            &productoid_key,
            &item,
        );
        insert_erp_match_key_with_normalized(
            &mut by_ref,
            &mut ambiguous_ref,
            &reference_key,
            &item,
        );
    }

    Ok(ErpStock {
        by_ean,
        by_productoid,
        by_ref,
        items,
    })
}

fn bracket_identifier(value: &str) -> String {
    format!("[{}]", value.replace(']', "]]"))
}

fn select_purchase_value_expression(columns: &HashSet<String>) -> String {
    let candidates = [
        "valor",
        "valortotal",
        "valor_total",
        "vrtotal",
        "vr_total",
        "total",
        "costo",
        "costototal",
        "costo_total",
        "costounitario",
        "costo_unitario",
    ];

    for candidate in candidates {
        if let Some(column) = columns
            .iter()
            .find(|column| column.eq_ignore_ascii_case(candidate))
        {
            return format!("CAST(m.{} AS VARCHAR(40))", bracket_identifier(column));
        }
    }

    "''".to_string()
}

async fn load_last_purchase_info(
    host: &str,
    erp: &ErpConnection,
) -> Result<HashMap<String, PurchaseInfo>, Box<dyn Error>> {
    let mut mssql_config = Config::new();
    mssql_config.host(host);
    mssql_config.port(erp.port);
    mssql_config.authentication(tiberius::AuthMethod::sql_server(&erp.user, &erp.password));
    mssql_config.database(&erp.database);
    mssql_config.encryption(tiberius::EncryptionLevel::NotSupported);

    let tcp = TcpStream::connect(mssql_config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut mssql_client = Client::connect(mssql_config, tcp.compat_write()).await?;
    let mut columns_stream = mssql_client
        .query(
            "SELECT COLUMN_NAME \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_NAME = 'Movimi'",
            &[],
        )
        .await?;
    let mut movimi_columns = HashSet::new();

    while let Some(row_result) = columns_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            let column = row
                .get::<&str, _>("COLUMN_NAME")
                .unwrap_or("")
                .trim()
                .to_string();
            if !column.is_empty() {
                movimi_columns.insert(column);
            }
        }
    }
    drop(columns_stream);

    let purchase_value_expression = select_purchase_value_expression(&movimi_columns);
    let query = format!(
        "SELECT \
            CodigoProducto, FechaUltimaCompra, CantidadUltimaCompra, ValorUltimaCompra \
         FROM ( \
            SELECT \
                TRIM(p.productoid) AS CodigoProducto, \
                CONVERT(VARCHAR(19), hm.fecha, 120) AS FechaUltimaCompra, \
                CAST(m.cantidad AS VARCHAR(40)) AS CantidadUltimaCompra, \
                {value_expr} AS ValorUltimaCompra, \
                ROW_NUMBER() OVER ( \
                    PARTITION BY p.productoid \
                    ORDER BY hm.fecha DESC, hm.movimientoid DESC, m.id DESC \
                ) AS rn \
            FROM Movimi m \
            JOIN HeadMovi hm ON hm.movimientoid = m.movimientoid \
            JOIN Producto p ON p.productoid = m.productoid \
            LEFT JOIN Document d ON d.documentoid = hm.documentoid \
            WHERE (d.compra IS NULL OR d.compra <> 'N') \
         ) mov \
         WHERE rn = 1",
        value_expr = purchase_value_expression
    );

    let mut stream = mssql_client.query(query, &[]).await?;
    let mut purchases = HashMap::new();

    while let Some(row_result) = stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            let productoid = row
                .get::<&str, _>("CodigoProducto")
                .unwrap_or("")
                .trim()
                .to_string();
            let info = PurchaseInfo {
                date: row
                    .get::<&str, _>("FechaUltimaCompra")
                    .unwrap_or("")
                    .trim()
                    .to_string(),
                qty: row
                    .get::<&str, _>("CantidadUltimaCompra")
                    .unwrap_or("")
                    .trim()
                    .to_string(),
                value: row
                    .get::<&str, _>("ValorUltimaCompra")
                    .unwrap_or("")
                    .trim()
                    .to_string(),
            };
            if !productoid.is_empty() {
                purchases.insert(productoid, info);
            }
        }
    }
    drop(stream);

    let fallback_query = "SELECT \
            TRIM(p.productoid) AS CodigoProducto, \
            CONVERT(VARCHAR(19), CASE \
                WHEN hp.ultimcompr IS NULL THEN CAST(hp.fechacompr AS DATETIME) \
                WHEN hp.fechacompr IS NULL THEN hp.ultimcompr \
                WHEN hp.ultimcompr >= CAST(hp.fechacompr AS DATETIME) THEN hp.ultimcompr \
                ELSE CAST(hp.fechacompr AS DATETIME) \
            END, 120) AS FechaUltimaCompra \
         FROM Producto p \
         INNER JOIN HeadProd hp ON hp.headprodid = p.headprodid \
         WHERE \
            hp.ultimcompr IS NOT NULL OR hp.fechacompr IS NOT NULL";

    let mut fallback_stream = mssql_client.query(fallback_query, &[]).await?;
    while let Some(row_result) = fallback_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            let productoid = row
                .get::<&str, _>("CodigoProducto")
                .unwrap_or("")
                .trim()
                .to_string();
            let last_purchase_date = row
                .get::<&str, _>("FechaUltimaCompra")
                .unwrap_or("")
                .trim()
                .to_string();
            if !productoid.is_empty() && !purchases.contains_key(&productoid) {
                purchases.insert(
                    productoid,
                    PurchaseInfo {
                        date: last_purchase_date,
                        qty: String::new(),
                        value: String::new(),
                    },
                );
            }
        }
    }

    Ok(purchases)
}

async fn inspect_erp_schema(host: &str, erp: &ErpConnection) -> Result<(), Box<dyn Error>> {
    let mut mssql_config = Config::new();
    mssql_config.host(host);
    mssql_config.port(erp.port);
    mssql_config.authentication(tiberius::AuthMethod::sql_server(&erp.user, &erp.password));
    mssql_config.database(&erp.database);
    mssql_config.encryption(tiberius::EncryptionLevel::NotSupported);

    let tcp = TcpStream::connect(mssql_config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut mssql_client = Client::connect(mssql_config, tcp.compat_write()).await?;
    let query = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE \
                 FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_NAME IN ('HeadProd', 'Producto', 'InveProd', 'Almacen', 'ListaPrecio', 'ListaPrecioTercero', 'HeadListaPrecioTercero') \
                    OR TABLE_NAME LIKE '%Grupo%' \
                    OR COLUMN_NAME LIKE 'grupo%' \
                 ORDER BY TABLE_NAME, ORDINAL_POSITION";
    let mut stream = mssql_client.query(query, &[]).await?;

    println!("Columnas ERP en {}:", host);
    while let Some(row_result) = stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            let table = row.get::<&str, _>("TABLE_NAME").unwrap_or("");
            let column = row.get::<&str, _>("COLUMN_NAME").unwrap_or("");
            let data_type = row.get::<&str, _>("DATA_TYPE").unwrap_or("");
            println!("{}.{} ({})", table, column, data_type);
        }
    }

    drop(stream);

    println!("Muestra de precios base en Producto:");
    let sample_query = "SELECT TOP 20 \
            TRIM(productoid) AS productoid, \
            TRIM(barras) AS barras, \
            CAST(valor AS VARCHAR(40)) AS valor, \
            CAST(valor3 AS VARCHAR(40)) AS valor3, \
            CAST(valor5 AS VARCHAR(40)) AS valor5 \
         FROM Producto \
         WHERE valor IS NOT NULL OR valor3 IS NOT NULL OR valor5 IS NOT NULL \
         ORDER BY productoid";
    let mut sample_stream = mssql_client.query(sample_query, &[]).await?;
    while let Some(row_result) = sample_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            let productoid = row.get::<&str, _>("productoid").unwrap_or("");
            let barras = row.get::<&str, _>("barras").unwrap_or("");
            let valor = row.get::<&str, _>("valor").unwrap_or("");
            let valor3 = row.get::<&str, _>("valor3").unwrap_or("");
            let valor5 = row.get::<&str, _>("valor5").unwrap_or("");
            println!(
                "productoid={} barras={} valor={} valor3={} valor5={}",
                productoid, barras, valor, valor3, valor5
            );
        }
    }

    drop(sample_stream);

    println!("Muestra de ListaPrecio:");
    let mut list_stream = mssql_client
        .query(
            "SELECT TOP 20 CAST(ProductoId AS VARCHAR(50)) AS ProductoId, CAST(Lista AS VARCHAR(20)) AS Lista, CAST(Valor AS VARCHAR(40)) AS Valor FROM ListaPrecio ORDER BY ProductoId, Lista",
            &[],
        )
        .await?;
    while let Some(row_result) = list_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            println!(
                "ProductoId={} Lista={} Valor={}",
                row.get::<&str, _>("ProductoId").unwrap_or(""),
                row.get::<&str, _>("Lista").unwrap_or(""),
                row.get::<&str, _>("Valor").unwrap_or("")
            );
        }
    }

    drop(list_stream);

    println!("Muestra de ListaPrecioTercero:");
    let mut third_stream = mssql_client
        .query(
            "SELECT TOP 20 CAST(ProductoId AS VARCHAR(50)) AS ProductoId, CAST(ListaPrecioId AS VARCHAR(20)) AS ListaPrecioId, CAST(Valor AS VARCHAR(40)) AS Valor FROM ListaPrecioTercero ORDER BY ProductoId, ListaPrecioId",
            &[],
        )
        .await?;
    while let Some(row_result) = third_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            println!(
                "ProductoId={} ListaPrecioId={} Valor={}",
                row.get::<&str, _>("ProductoId").unwrap_or(""),
                row.get::<&str, _>("ListaPrecioId").unwrap_or(""),
                row.get::<&str, _>("Valor").unwrap_or("")
            );
        }
    }

    Ok(())
}

async fn inspect_purchase_schema(host: &str, erp: &ErpConnection) -> Result<(), Box<dyn Error>> {
    let mut mssql_config = Config::new();
    mssql_config.host(host);
    mssql_config.port(erp.port);
    mssql_config.authentication(tiberius::AuthMethod::sql_server(&erp.user, &erp.password));
    mssql_config.database(&erp.database);
    mssql_config.encryption(tiberius::EncryptionLevel::NotSupported);

    let tcp = TcpStream::connect(mssql_config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut mssql_client = Client::connect(mssql_config, tcp.compat_write()).await?;
    let query = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE \
                 FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_NAME IN ('Movimi', 'MoviTemp', 'Movim', 'MovimiHead', 'HeadMovi', 'Movimiento', 'OrdeComp', 'HeadOrCo', 'RecepcionTecnica') \
                    OR TABLE_NAME LIKE '%Compr%' \
                    OR COLUMN_NAME LIKE '%compr%' \
                    OR COLUMN_NAME IN ('productoid', 'ProductoId', 'cantidad', 'Cantidad') \
                 ORDER BY TABLE_NAME, ORDINAL_POSITION";
    let mut stream = mssql_client.query(query, &[]).await?;

    println!("Columnas ERP candidatas para compras en {}:", host);
    while let Some(row_result) = stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            let table = row.get::<&str, _>("TABLE_NAME").unwrap_or("");
            let column = row.get::<&str, _>("COLUMN_NAME").unwrap_or("");
            let data_type = row.get::<&str, _>("DATA_TYPE").unwrap_or("");
            println!("{}.{} ({})", table, column, data_type);
        }
    }

    Ok(())
}

async fn inspect_erp_product(
    host: &str,
    erp: &ErpConnection,
    product_code: &str,
) -> Result<(), Box<dyn Error>> {
    let mut mssql_config = Config::new();
    mssql_config.host(host);
    mssql_config.port(erp.port);
    mssql_config.authentication(tiberius::AuthMethod::sql_server(&erp.user, &erp.password));
    mssql_config.database(&erp.database);
    mssql_config.encryption(tiberius::EncryptionLevel::NotSupported);

    let tcp = TcpStream::connect(mssql_config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut mssql_client = Client::connect(mssql_config, tcp.compat_write()).await?;
    let product_query = format!(
        "SELECT \
            TRIM(p.productoid) AS productoid, TRIM(p.barras) AS barras, TRIM(p.barras2) AS barras2, TRIM(p.Barras3) AS barras3, \
            TRIM(hp.referencia) AS referencia, TRIM(hp.nombre) AS nombre, TRIM(hp.unidad) AS unidad, \
            CAST(hp.factor AS VARCHAR(40)) AS factor, CAST(hp.PUMContenidoInterno AS VARCHAR(40)) AS pum_contenido, \
            TRIM(hp.PUMUnidadMedida) AS pum_unidad, CAST(p.Cantidad1 AS VARCHAR(40)) AS cantidad1, \
            CAST(p.Cantidad2 AS VARCHAR(40)) AS cantidad2, CAST(p.Cantidad3 AS VARCHAR(40)) AS cantidad3 \
         FROM Producto p \
         JOIN HeadProd hp ON hp.headprodid = p.headprodid \
         WHERE p.productoid = {code} OR hp.referencia = {code} OR p.barras = {code} OR p.barras2 = {code} OR p.Barras3 = {code}",
        code = sql_string(product_code)
    );

    println!("Producto ERP {}:", product_code);
    let mut product_stream = mssql_client.query(product_query, &[]).await?;
    while let Some(row_result) = product_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            println!(
                "productoid={} barras={} barras2={} barras3={} referencia={} nombre={} unidad={} factor={} pum={} {} cantidades=[{}, {}, {}]",
                row.get::<&str, _>("productoid").unwrap_or(""),
                row.get::<&str, _>("barras").unwrap_or(""),
                row.get::<&str, _>("barras2").unwrap_or(""),
                row.get::<&str, _>("barras3").unwrap_or(""),
                row.get::<&str, _>("referencia").unwrap_or(""),
                row.get::<&str, _>("nombre").unwrap_or(""),
                row.get::<&str, _>("unidad").unwrap_or(""),
                row.get::<&str, _>("factor").unwrap_or(""),
                row.get::<&str, _>("pum_contenido").unwrap_or(""),
                row.get::<&str, _>("pum_unidad").unwrap_or(""),
                row.get::<&str, _>("cantidad1").unwrap_or(""),
                row.get::<&str, _>("cantidad2").unwrap_or(""),
                row.get::<&str, _>("cantidad3").unwrap_or("")
            );
        }
    }
    drop(product_stream);

    let inventory_query = format!(
        "SELECT \
            ip.almacenid AS almacenid, a.nombre AS almacen, \
            CAST(ip.invenactua AS VARCHAR(40)) AS invenactua, \
            CAST(ip.invenfracc AS VARCHAR(40)) AS invenfracc, \
            CAST(ip.invensepar AS VARCHAR(40)) AS invensepar, \
            CAST(ip.invenpedid AS VARCHAR(40)) AS invenpedid, \
            CAST(ip.inventario AS VARCHAR(40)) AS inventario_unidades, \
            CAST(ip.inveninfra AS VARCHAR(40)) AS inveninfra, \
            CAST(ip.InvenOrdCo AS VARCHAR(40)) AS InvenOrdCo, \
            CAST(ip.InvenOrdPr AS VARCHAR(40)) AS InvenOrdPr, \
            CAST(COALESCE(ip.invenactua,0) - COALESCE(ip.invensepar,0) AS VARCHAR(40)) AS disponible \
         FROM InveProd ip \
         JOIN Producto p ON p.productoid = ip.productoid \
         LEFT JOIN Almacen a ON a.almacenid = ip.almacenid \
         LEFT JOIN HeadProd hp ON hp.headprodid = p.headprodid \
         WHERE p.productoid = {code} OR hp.referencia = {code} OR p.barras = {code} OR p.barras2 = {code} OR p.Barras3 = {code} \
         ORDER BY ip.almacenid",
        code = sql_string(product_code)
    );

    println!("Inventario ERP por almacen {}:", product_code);
    let mut inventory_stream = mssql_client.query(inventory_query, &[]).await?;
    while let Some(row_result) = inventory_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            println!(
                "almacen={} nombre={} invenactua={} invenfracc={} invensepar={} invenpedid={} inventario_unidades={} inveninfra={} InvenOrdCo={} InvenOrdPr={} disponible={}",
                row.get::<&str, _>("almacenid").unwrap_or(""),
                row.get::<&str, _>("almacen").unwrap_or(""),
                row.get::<&str, _>("invenactua").unwrap_or(""),
                row.get::<&str, _>("invenfracc").unwrap_or(""),
                row.get::<&str, _>("invensepar").unwrap_or(""),
                row.get::<&str, _>("invenpedid").unwrap_or(""),
                row.get::<&str, _>("inventario_unidades").unwrap_or(""),
                row.get::<&str, _>("inveninfra").unwrap_or(""),
                row.get::<&str, _>("InvenOrdCo").unwrap_or(""),
                row.get::<&str, _>("InvenOrdPr").unwrap_or(""),
                row.get::<&str, _>("disponible").unwrap_or("")
            );
        }
    }
    drop(inventory_stream);

    let purchases_query = format!(
        "SELECT TOP 10 \
            CONVERT(VARCHAR(19), hm.fecha, 120) AS fecha, \
            hm.documentoid AS documento, hm.numero AS numero, \
            CAST(m.cantidad AS VARCHAR(40)) AS cantidad, \
            CAST(d.compra AS VARCHAR(10)) AS compra \
         FROM Movimi m \
         JOIN HeadMovi hm ON hm.movimientoid = m.movimientoid \
         JOIN Producto p ON p.productoid = m.productoid \
         JOIN HeadProd hp ON hp.headprodid = p.headprodid \
         LEFT JOIN Document d ON d.documentoid = hm.documentoid \
         WHERE (p.productoid = {code} OR hp.referencia = {code} OR p.barras = {code} OR p.barras2 = {code} OR p.Barras3 = {code}) \
           AND (d.compra IS NULL OR d.compra <> 'N') \
         ORDER BY hm.fecha DESC, hm.movimientoid DESC, m.id DESC",
        code = sql_string(product_code)
    );

    println!("Ultimos movimientos de compra ERP {}:", product_code);
    let mut purchases_stream = mssql_client.query(purchases_query, &[]).await?;
    while let Some(row_result) = purchases_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            println!(
                "fecha={} documento={} numero={} cantidad={} compra={}",
                row.get::<&str, _>("fecha").unwrap_or(""),
                row.get::<&str, _>("documento").unwrap_or(""),
                row.get::<&str, _>("numero").unwrap_or(""),
                row.get::<&str, _>("cantidad").unwrap_or(""),
                row.get::<&str, _>("compra").unwrap_or("")
            );
        }
    }

    Ok(())
}

fn fmt_qty(value: Option<i32>) -> String {
    value
        .map(|v| v.to_string())
        .unwrap_or_else(|| "SIN_MATCH".to_string())
}

fn fmt_qty_decimal(value: Option<f64>) -> String {
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

fn fmt_price(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "SIN_PRECIO".to_string())
}

fn fmt_decimal(value: Option<f64>) -> String {
    value.map(|v| format!("{:.6}", v)).unwrap_or_default()
}

fn fmt_pum_unit_price(value: Option<f64>) -> String {
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

fn action_pum_source(pum: &Option<PumUpdate>) -> String {
    pum.as_ref()
        .map(|pum| pum.source.clone())
        .unwrap_or_default()
}

fn price_is_different(current: f64, target: Option<f64>) -> bool {
    target
        .map(|price| (current - price).abs() >= 0.005)
        .unwrap_or(false)
}

fn normalize_pum_unit_price(value: f64) -> f64 {
    if value > 10.0 {
        value.round()
    } else {
        (value * 10.0).round() / 10.0
    }
}

fn normalize_unit(value: &str) -> String {
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

fn unit_token(value: &str) -> Option<&'static str> {
    match value {
        "g" | "gr" | "gramo" | "gramos" => Some("G"),
        "kg" | "kl" | "kilo" | "kilos" | "kilogramo" | "kilogramos" => Some("KG"),
        _ => None,
    }
}

fn parse_amount_token(value: &str) -> Option<f64> {
    let cleaned = value
        .trim_start_matches('x')
        .trim_start_matches('X')
        .replace(',', ".");
    cleaned.parse::<f64>().ok()
}

fn factor_from_weight(amount: f64, unit: &str) -> f64 {
    match unit {
        "G" => amount / 1000.0,
        "KG" => amount,
        _ => 1.0,
    }
}

fn parse_decimal(value: &str) -> Option<f64> {
    value.trim().replace(',', ".").parse::<f64>().ok()
}

fn csv_fields(line: &str) -> Vec<String> {
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

fn load_pum_plan(path: &str) -> Result<HashMap<String, PumSeed>, Box<dyn Error>> {
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

fn normalize_pum_unity(value: &str) -> String {
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

fn find_pum_plan_seed(
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

fn erp_pum_seed(item: &ErpItem) -> Option<PumSeed> {
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

fn infer_pum_seed(name: &str) -> Option<PumSeed> {
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

fn resolve_pum_update(
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

fn pum_is_different(product: &Product, pum: &PumUpdate) -> bool {
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

fn infer_local_unit_and_factor(name: &str, erp_unit: &str) -> (String, f64) {
    let text = name.to_lowercase();
    let erp = normalize_unit(erp_unit);

    if erp != "KG" {
        return (erp, 1.0);
    }

    let tokens: Vec<&str> = text
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == ',' || c == '.'))
        .filter(|s| !s.is_empty())
        .collect();

    let suffixes = [
        ("kilogramos", "KG"),
        ("kilogramo", "KG"),
        ("kilos", "KG"),
        ("kilo", "KG"),
        ("kg", "KG"),
        ("kl", "KG"),
        ("gramos", "G"),
        ("gramo", "G"),
        ("gr", "G"),
        ("g", "G"),
    ];

    for (index, token) in tokens.iter().enumerate() {
        for (suffix, unit) in suffixes {
            if let Some(number_part) = token.strip_suffix(suffix) {
                if let Some(amount) = parse_amount_token(number_part) {
                    let factor = factor_from_weight(amount, unit);
                    return (format!("{} {}", amount, unit), factor);
                }
            }
        }

        if let Some(amount) = parse_amount_token(token) {
            if let Some(next) = tokens.get(index + 1) {
                if let Some(unit) = unit_token(next) {
                    let factor = factor_from_weight(amount, unit);
                    return (format!("{} {}", amount, unit), factor);
                }
            }
        }
    }

    if erp == "KG" && (text.contains("500 gr") || text.contains("500gr") || text.contains("500 g"))
    {
        return ("500 G".to_string(), 0.5);
    }

    (erp, 1.0)
}

fn csv_escape(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

fn sql_string(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('\'', "''");
    format!("'{}'", escaped)
}

fn sql_bool(value: bool) -> &'static str {
    if value {
        "1"
    } else {
        "0"
    }
}

fn slugify(value: &str) -> String {
    let mut slug = String::new();
    let mut last_dash = false;

    for ch in value.trim().to_lowercase().chars() {
        let mapped = match ch {
            'a'..='z' | '0'..='9' => Some(ch),
            'á' | 'à' | 'ä' | 'â' => Some('a'),
            'é' | 'è' | 'ë' | 'ê' => Some('e'),
            'í' | 'ì' | 'ï' | 'î' => Some('i'),
            'ó' | 'ò' | 'ö' | 'ô' => Some('o'),
            'ú' | 'ù' | 'ü' | 'û' => Some('u'),
            'ñ' => Some('n'),
            _ => None,
        };

        if let Some(ch) = mapped {
            slug.push(ch);
            last_dash = false;
        } else if !last_dash && !slug.is_empty() {
            slug.push('-');
            last_dash = true;
        }
    }

    let slug = slug.trim_matches('-').to_string();
    if slug.is_empty() {
        "producto".to_string()
    } else {
        slug
    }
}

fn valid_ean13(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() == 13 && trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        trimmed.to_string()
    } else {
        String::new()
    }
}

fn erp_category_name(value: &str) -> String {
    value.trim().chars().take(128).collect::<String>()
}

fn product_reference(item: &ErpItem) -> String {
    if !item.productoid.trim().is_empty() {
        item.productoid.trim().to_string()
    } else {
        item.reference.trim().to_string()
    }
}

async fn table_columns(
    conn: &mut mysql_async::Conn,
    table: &str,
) -> Result<HashSet<String>, Box<dyn Error>> {
    let rows: Vec<String> = conn
        .exec(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name",
            params! {
                "table_name" => table,
            },
        )
        .await?;

    Ok(rows.into_iter().collect())
}

fn push_column_value(
    cols: &HashSet<String>,
    names: &mut Vec<String>,
    values: &mut Vec<String>,
    name: &str,
    value: String,
) {
    if cols.contains(name) {
        names.push(format!("`{}`", name));
        values.push(value);
    }
}

async fn insert_dynamic(
    conn: &mut mysql_async::Conn,
    table: &str,
    columns: &[String],
    values: &[String],
) -> Result<(), Box<dyn Error>> {
    if columns.is_empty() {
        return Ok(());
    }

    conn.query_drop(format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table,
        columns.join(","),
        values.join(",")
    ))
    .await?;
    Ok(())
}

async fn load_prestashop_context(
    conn: &mut mysql_async::Conn,
) -> Result<PrestashopContext, Box<dyn Error>> {
    let config_rows: Vec<(String, Option<String>)> = conn
        .query(format!(
            "SELECT name, value FROM {}configuration \
             WHERE name IN ('PS_LANG_DEFAULT','PS_SHOP_DEFAULT','PS_ROOT_CATEGORY','PS_HOME_CATEGORY')",
            DB_PREFIX
        ))
        .await?;
    let config: HashMap<String, String> = config_rows
        .into_iter()
        .map(|(name, value)| (name, value.unwrap_or_default()))
        .collect();

    let id_shop = config_value(&config, "PS_SHOP_DEFAULT", "1")
        .parse::<u32>()
        .unwrap_or(1);
    let id_lang = config_value(&config, "PS_LANG_DEFAULT", "1")
        .parse::<u32>()
        .unwrap_or(1);
    let shop_category: Option<u32> = conn
        .exec_first(
            format!(
                "SELECT id_category FROM {}shop WHERE id_shop = :id_shop LIMIT 1",
                DB_PREFIX
            ),
            params! {
                "id_shop" => id_shop,
            },
        )
        .await?;
    let id_category_home = config_value(&config, "PS_HOME_CATEGORY", "")
        .parse::<u32>()
        .ok()
        .or(shop_category)
        .unwrap_or(2);
    let id_category_root = config_value(&config, "PS_ROOT_CATEGORY", "")
        .parse::<u32>()
        .unwrap_or(1);
    let id_groups: Vec<u32> = conn
        .query(format!(
            "SELECT id_group FROM {}group ORDER BY id_group",
            DB_PREFIX
        ))
        .await?;

    Ok(PrestashopContext {
        id_shop,
        id_lang,
        id_category_home,
        id_category_root,
        id_groups,
    })
}

async fn category_id_by_name(
    conn: &mut mysql_async::Conn,
    ctx: &PrestashopContext,
    id_parent: u32,
    name: &str,
) -> Result<Option<u32>, Box<dyn Error>> {
    let id: Option<u32> = conn
        .exec_first(
            format!(
                "SELECT c.id_category \
                 FROM {}category c \
                 INNER JOIN {}category_lang cl ON cl.id_category = c.id_category \
                    AND cl.id_lang = :id_lang AND cl.id_shop = :id_shop \
                 WHERE c.id_parent = :id_parent AND cl.name = :name \
                 LIMIT 1",
                DB_PREFIX, DB_PREFIX
            ),
            params! {
                "id_lang" => ctx.id_lang,
                "id_shop" => ctx.id_shop,
                "id_parent" => id_parent,
                "name" => name,
            },
        )
        .await?;
    Ok(id)
}

async fn create_category(
    conn: &mut mysql_async::Conn,
    ctx: &PrestashopContext,
    id_parent: u32,
    name: &str,
) -> Result<u32, Box<dyn Error>> {
    let parent_depth: Option<u32> = conn
        .exec_first(
            format!(
                "SELECT level_depth FROM {}category WHERE id_category = :id_parent",
                DB_PREFIX
            ),
            params! {
                "id_parent" => id_parent,
            },
        )
        .await?;
    let position: Option<u32> = conn
        .exec_first(
            format!(
                "SELECT COALESCE(MAX(position), -1) + 1 \
                 FROM {}category WHERE id_parent = :id_parent",
                DB_PREFIX
            ),
            params! {
                "id_parent" => id_parent,
            },
        )
        .await?;

    conn.exec_drop(
        format!(
            "INSERT INTO {}category \
             (id_parent, id_shop_default, level_depth, nleft, nright, active, date_add, date_upd, position, is_root_category) \
             VALUES (:id_parent, :id_shop, :level_depth, 0, 0, 1, NOW(), NOW(), :position, 0)",
            DB_PREFIX
        ),
        params! {
            "id_parent" => id_parent,
            "id_shop" => ctx.id_shop,
            "level_depth" => parent_depth.unwrap_or(0) + 1,
            "position" => position.unwrap_or(0),
        },
    )
    .await?;
    let id_category: Option<u32> = conn.query_first("SELECT LAST_INSERT_ID()").await?;
    let id_category = id_category.ok_or("No se pudo obtener LAST_INSERT_ID() de categoria")?;

    let languages: Vec<u32> = conn
        .query(format!("SELECT id_lang FROM {}lang", DB_PREFIX))
        .await?;
    for id_lang in languages {
        conn.exec_drop(
            format!(
                "INSERT INTO {}category_lang \
                 (id_category, id_shop, id_lang, name, description, additional_description, link_rewrite, meta_title, meta_keywords, meta_description) \
                 VALUES (:id_category, :id_shop, :id_lang, :name, '', '', :link_rewrite, :name, '', '')",
                DB_PREFIX
            ),
            params! {
                "id_category" => id_category,
                "id_shop" => ctx.id_shop,
                "id_lang" => id_lang,
                "name" => name,
                "link_rewrite" => slugify(name),
            },
        )
        .await?;
    }

    conn.exec_drop(
        format!(
            "INSERT INTO {}category_shop (id_category, id_shop, position) \
             VALUES (:id_category, :id_shop, :position)",
            DB_PREFIX
        ),
        params! {
            "id_category" => id_category,
            "id_shop" => ctx.id_shop,
            "position" => position.unwrap_or(0),
        },
    )
    .await?;

    for id_group in &ctx.id_groups {
        conn.exec_drop(
            format!(
                "INSERT IGNORE INTO {}category_group (id_category, id_group) \
                 VALUES (:id_category, :id_group)",
                DB_PREFIX
            ),
            params! {
                "id_category" => id_category,
                "id_group" => id_group,
            },
        )
        .await?;
    }

    Ok(id_category)
}

async fn ensure_category(
    conn: &mut mysql_async::Conn,
    ctx: &PrestashopContext,
    id_parent: u32,
    name: &str,
    created_count: &mut u32,
) -> Result<u32, Box<dyn Error>> {
    let name = erp_category_name(name);
    if name.is_empty() {
        return Ok(id_parent);
    }

    if let Some(id_category) = category_id_by_name(conn, ctx, id_parent, &name).await? {
        return Ok(id_category);
    }

    let id_category = create_category(conn, ctx, id_parent, &name).await?;
    *created_count += 1;
    Ok(id_category)
}

async fn regenerate_category_tree(conn: &mut mysql_async::Conn) -> Result<(), Box<dyn Error>> {
    let rows: Vec<(u32, u32)> = conn
        .query(format!(
            "SELECT id_category, id_parent FROM {}category ORDER BY id_parent, position, id_category",
            DB_PREFIX
        ))
        .await?;
    let mut children: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut ids = HashSet::new();
    for (id_category, id_parent) in rows {
        ids.insert(id_category);
        children.entry(id_parent).or_default().push(id_category);
    }

    fn walk(
        id_category: u32,
        depth: u32,
        children: &HashMap<u32, Vec<u32>>,
        counter: &mut u32,
        updates: &mut Vec<(u32, u32, u32, u32)>,
    ) {
        let nleft = *counter;
        *counter += 1;
        if let Some(child_ids) = children.get(&id_category) {
            for child_id in child_ids {
                walk(*child_id, depth + 1, children, counter, updates);
            }
        }
        let nright = *counter;
        *counter += 1;
        updates.push((id_category, nleft, nright, depth));
    }

    let mut updates = Vec::new();
    let mut counter = 1u32;
    let roots = children.get(&0).cloned().unwrap_or_else(|| {
        ids.iter()
            .filter(|id| !children.values().any(|items| items.contains(id)))
            .copied()
            .collect()
    });
    for root in roots {
        walk(root, 0, &children, &mut counter, &mut updates);
    }

    for (id_category, nleft, nright, level_depth) in updates {
        conn.exec_drop(
            format!(
                "UPDATE {}category SET nleft = :nleft, nright = :nright, level_depth = :level_depth \
                 WHERE id_category = :id_category",
                DB_PREFIX
            ),
            params! {
                "nleft" => nleft,
                "nright" => nright,
                "level_depth" => level_depth,
                "id_category" => id_category,
            },
        )
        .await?;
    }

    Ok(())
}

fn should_emit_audit_row(row: &AuditRow) -> bool {
    if row.action == "SIN_CAMBIO" {
        return false;
    }

    if row.action == "EXCLUIR_SIN_MOVIMIENTO" && row.last_purchase_date.is_empty() {
        return false;
    }

    true
}

async fn load_prestashop_category_groups(
    conn: &mut mysql_async::Conn,
) -> Result<HashMap<u32, (String, String)>, Box<dyn Error>> {
    let query = format!(
        "SELECT ps.id_product, \
            CASE \
                WHEN c.id_parent IS NULL OR c.id_parent <= 2 THEN COALESCE(cl.name, '') \
                ELSE COALESCE(parent_cl.name, '') \
            END AS grupo_prestashop, \
            CASE \
                WHEN c.id_parent IS NULL OR c.id_parent <= 2 THEN '' \
                ELSE COALESCE(cl.name, '') \
            END AS subgrupo_prestashop \
         FROM {}product_shop ps \
         LEFT JOIN {}category c ON c.id_category = ps.id_category_default \
         LEFT JOIN {}category_lang cl ON cl.id_category = c.id_category \
            AND cl.id_shop = ps.id_shop \
            AND cl.id_lang = (SELECT CAST(value AS UNSIGNED) FROM {}configuration WHERE name = 'PS_LANG_DEFAULT' LIMIT 1) \
         LEFT JOIN {}category parent_c ON parent_c.id_category = c.id_parent \
         LEFT JOIN {}category_lang parent_cl ON parent_cl.id_category = parent_c.id_category \
            AND parent_cl.id_shop = ps.id_shop \
            AND parent_cl.id_lang = (SELECT CAST(value AS UNSIGNED) FROM {}configuration WHERE name = 'PS_LANG_DEFAULT' LIMIT 1) \
         WHERE ps.id_shop = 1",
        DB_PREFIX, DB_PREFIX, DB_PREFIX, DB_PREFIX, DB_PREFIX, DB_PREFIX, DB_PREFIX
    );

    let rows: Vec<(u32, Option<String>, Option<String>)> = conn.query(query).await?;
    Ok(rows
        .into_iter()
        .map(|(id_product, group, subgroup)| {
            (
                id_product,
                (group.unwrap_or_default(), subgroup.unwrap_or_default()),
            )
        })
        .collect())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let total_start = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let apply_changes = args.iter().any(|arg| arg == "--apply");
    let audit_only = !apply_changes;

    println!(
        "[{}] Iniciando batch de sincronizacion de stock en Rust...",
        Local::now().format("%Y-%m-%d %H:%M:%S")
    );
    if audit_only {
        println!("MODO AUDITORIA: no se actualizara MariaDB. Use --apply para aplicar cambios.");
    } else {
        println!("MODO APLICAR: se actualizaran inventario, precio y PUM en MariaDB.");
    }

    let local_config = read_local_config();
    let mariadb_start = Instant::now();
    let mariadb_host = std::env::var("MARIADB_HOST")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| config_value(&local_config, "MARIADB_HOST", "www.mercaboy.com"));
    let mariadb_port = std::env::var("MARIADB_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or_else(|| {
            config_value(&local_config, "MARIADB_PORT", "")
                .parse::<u16>()
                .unwrap_or(3306)
        });
    let mariadb_user = std::env::var("MARIADB_USER")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| config_value(&local_config, "MARIADB_USER", ""));
    let mariadb_pass = std::env::var("MARIADB_PASSWORD")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| config_value(&local_config, "MARIADB_PASSWORD", ""));
    let mariadb_db = std::env::var("MARIADB_DATABASE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| config_value(&local_config, "MARIADB_DATABASE", ""));

    if mariadb_user.is_empty() || mariadb_pass.is_empty() || mariadb_db.is_empty() {
        return Err(format!(
            "Faltan parametros MariaDB en {}: MARIADB_USER, MARIADB_PASSWORD o MARIADB_DATABASE",
            LOCAL_ENV_CONFIG
        )
        .into());
    }

    let connection_url = format!(
        "mysql://{}:{}@{}:{}/{}",
        mariadb_user, mariadb_pass, mariadb_host, mariadb_port, mariadb_db
    );

    let opts = mysql_async::Opts::from_url(&connection_url)?;
    let pool = Pool::new(opts);
    let mut conn = pool.get_conn().await?;
    println!("Conectado a MariaDB con exito.");
    let mariadb_connect_ms = mariadb_start.elapsed().as_millis();

    let config_start = Instant::now();
    let config_query = format!(
        "SELECT name, value FROM {}configuration WHERE name LIKE 'MERCABOY_ERP_%'",
        DB_PREFIX
    );
    let config_rows: Vec<(String, Option<String>)> = conn.query(config_query).await?;
    let config: HashMap<String, String> = config_rows
        .into_iter()
        .map(|(k, v)| (k, v.unwrap_or_default()))
        .collect();

    let erp_connection = ErpConnection {
        port: config_value(&local_config, "MERCABOY_ERP_PORT", "")
            .parse::<u16>()
            .ok()
            .or_else(|| {
                config
                    .get("MERCABOY_ERP_PORT")
                    .filter(|s| !s.trim().is_empty())
                    .and_then(|p| p.parse().ok())
            })
            .unwrap_or(1433),
        database: config_value(
            &local_config,
            "MERCABOY_ERP_DATABASE",
            &config_value(&config, "MERCABOY_ERP_DATABASE", "ERPFIVE_MERCABOY"),
        ),
        user: config_value(
            &local_config,
            "MERCABOY_ERP_USER",
            &config_value(&config, "MERCABOY_ERP_USER", "sa"),
        ),
        password: config_value(
            &local_config,
            "MERCABOY_ERP_PASSWORD",
            &config_value(&config, "MERCABOY_ERP_PASSWORD", ""),
        ),
    };
    if erp_connection.password.is_empty() {
        return Err(format!(
            "Falta MERCABOY_ERP_PASSWORD en {} para conectar al ERP",
            LOCAL_ENV_CONFIG
        )
        .into());
    }

    let almacenes_str = config_value(&config, "MERCABOY_ERP_ALMACENES", "001,002,003");
    let pending_window: i32 = config
        .get("MERCABOY_ERP_PENDING_WINDOW")
        .filter(|s| !s.trim().is_empty())
        .and_then(|w| w.parse().ok())
        .unwrap_or(10);

    let sync_host = local_config
        .get("sync_host")
        .cloned()
        .unwrap_or_else(|| "192.168.0.231".to_string());
    let audit_hosts = parse_hosts(local_config.get("audit_hosts"), &sync_host);
    let purchase_movement_window_days: i32 = local_config
        .get("purchase_movement_window_days")
        .or_else(|| local_config.get("ventana_movimientos_compra_dias"))
        .and_then(|days| days.parse().ok())
        .filter(|days| *days > 0)
        .unwrap_or(60);
    let purchase_movement_host = local_config
        .get("purchase_movement_host")
        .cloned()
        .unwrap_or_else(|| "192.168.0.231".to_string());

    println!("Archivo local de sincronizacion: {}", LOCAL_SYNC_CONFIG);
    println!(
        "Servidor seleccionado para sincronizar MariaDB: {}",
        sync_host
    );
    println!("Servidores auditados: {}", audit_hosts.join(", "));
    println!("Almacenes ERP: {}", almacenes_str);
    println!(
        "Consulta ultima compra/ingreso: historica en {} (ventana {} dias solo informativa)",
        purchase_movement_host, purchase_movement_window_days
    );
    let pum_plan_path = local_config
        .get("pum_plan_path")
        .cloned()
        .unwrap_or_else(|| "/opt/2prestashopsync/PlanillaPUM.csv".to_string());
    let pum_plan = match load_pum_plan(&pum_plan_path) {
        Ok(plan) => {
            println!(
                "Planilla PUM cargada: {} registros desde {}",
                plan.len(),
                pum_plan_path
            );
            plan
        }
        Err(error) => {
            println!(
                "Planilla PUM no disponible en {}: {}. Se usara ERP/fallback.",
                pum_plan_path, error
            );
            HashMap::new()
        }
    };
    let config_ms = config_start.elapsed().as_millis();

    if args.iter().any(|arg| arg == "--inspect-erp-schema") {
        inspect_erp_schema(&sync_host, &erp_connection).await?;
        return Ok(());
    }
    if args.iter().any(|arg| arg == "--inspect-purchase-schema") {
        inspect_purchase_schema(&sync_host, &erp_connection).await?;
        return Ok(());
    }
    if let Some(position) = args.iter().position(|arg| arg == "--inspect-product") {
        let product_code = args
            .get(position + 1)
            .ok_or("--inspect-product requiere un codigo de producto")?;
        inspect_erp_product(&sync_host, &erp_connection, product_code).await?;
        return Ok(());
    }

    let products_start = Instant::now();
    let products_query = format!(
        "SELECT p.id_product, COALESCE(pl.name, ''), COALESCE(p.ean13, ''), COALESCE(p.reference, ''), sa.quantity, product_shop.price, \
            COALESCE(product_shop.unity, ''), product_shop.unit_price, product_shop.unit_price_ratio \
         FROM {}product p \
         INNER JOIN {}product_shop product_shop ON (product_shop.id_product = p.id_product AND product_shop.id_shop = 1) \
         LEFT JOIN {}product_lang pl ON (pl.id_product = p.id_product AND pl.id_shop = 1 \
             AND pl.id_lang = (SELECT CAST(value AS UNSIGNED) FROM {}configuration WHERE name = 'PS_LANG_DEFAULT' LIMIT 1)) \
         LEFT JOIN {}stock_available sa ON (sa.id_product = p.id_product AND sa.id_product_attribute = 0 AND sa.id_shop = 1) \
         WHERE product_shop.active = 1 \
           AND ((p.reference IS NOT NULL AND p.reference <> '') OR (p.ean13 IS NOT NULL AND p.ean13 <> ''))",
        DB_PREFIX, DB_PREFIX, DB_PREFIX, DB_PREFIX, DB_PREFIX
    );

    let ps_products: Vec<Product> = conn
        .query_map(
            products_query,
            |(
                id_product,
                name,
                ean13,
                reference,
                current_qty,
                current_price,
                current_unity,
                current_unit_price,
                current_unit_price_ratio,
            ): (
                u32,
                String,
                String,
                String,
                Option<i32>,
                Option<f64>,
                String,
                Option<f64>,
                Option<f64>,
            )| Product {
                id_product,
                name,
                ean13,
                reference,
                current_qty,
                current_price,
                current_unity,
                current_unit_price,
                current_unit_price_ratio,
            },
        )
        .await?;
    println!(
        "Encontrados {} productos activos en PrestaShop.",
        ps_products.len()
    );
    let ps_category_groups = load_prestashop_category_groups(&mut conn).await?;
    let products_ms = products_start.elapsed().as_millis();

    let almacenes_list: Vec<String> = almacenes_str
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| format!("'{}'", s.replace('\'', "''")))
        .collect();
    let almacenes_in_clause = almacenes_list.join(", ");

    let mut stocks_by_host: HashMap<String, ErpStock> = HashMap::new();
    let erp_start = Instant::now();
    for host in &audit_hosts {
        println!("Consultando inventario ERP en {}...", host);
        let stock = load_erp_stock(host, &erp_connection, &almacenes_in_clause).await?;
        println!(
            "Cargados {} registros de stock desde {}.",
            stock.by_ean.len() + stock.by_ref.len(),
            host
        );
        stocks_by_host.insert(host.clone(), stock);
    }
    let erp_ms = erp_start.elapsed().as_millis();

    let sync_stock = stocks_by_host.get(&sync_host).ok_or_else(|| {
        format!(
            "No se cargo stock para el servidor de sincronizacion {}",
            sync_host
        )
    })?;

    let purchase_info_by_product =
        load_last_purchase_info(&purchase_movement_host, &erp_connection).await?;
    println!(
        "Productos con ultima compra/ingreso historico desde {}: {}",
        purchase_movement_host,
        purchase_info_by_product.len()
    );

    let pending_start = Instant::now();
    let cancel_rows: Vec<String> = conn
        .query(format!(
            "SELECT value FROM {}configuration WHERE name IN ('PS_OS_CANCELED', 'PS_OS_ERROR')",
            DB_PREFIX
        ))
        .await?;
    let cancel_list_str = if cancel_rows.is_empty() {
        "6,8".to_string()
    } else {
        cancel_rows
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<String>>()
            .join(",")
    };

    let export_table = format!("{}erp_order_export", DB_PREFIX);
    let export_table_exists: Option<u8> = conn
        .exec_first(
            "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name LIMIT 1",
            params! {
                "table_name" => &export_table,
            },
        )
        .await?;

    let pending_rows: Vec<(u32, Option<i64>)> = if export_table_exists.is_some() {
        let pending_query = format!(
            "SELECT od.product_id, CAST(SUM(od.product_quantity) AS SIGNED) AS qty \
             FROM {}order_detail od \
             INNER JOIN {}orders o ON o.id_order = od.id_order \
             LEFT JOIN {}erp_order_export e ON e.id_order = o.id_order \
             WHERE od.product_attribute_id = 0 \
               AND o.current_state NOT IN ({}) \
               AND ( \
                     (e.id_erp_order_export IS NOT NULL AND e.erp_status IN ('pending', 'exported')) \
                     OR ( \
                         (e.erp_confirmed_at IS NULL OR e.id_erp_order_export IS NULL) \
                         AND o.date_add >= DATE_SUB(NOW(), INTERVAL :pending_window MINUTE) \
                     ) \
               ) \
             GROUP BY od.product_id",
            DB_PREFIX, DB_PREFIX, DB_PREFIX, cancel_list_str
        );

        conn.exec(
            pending_query,
            params! {
                "pending_window" => pending_window,
            },
        )
        .await?
    } else {
        println!(
            "Aviso: no existe la tabla {}; se calcularan pendientes como 0.",
            export_table
        );
        Vec::new()
    };

    let mut pending_qty_map: HashMap<u32, i32> = HashMap::new();
    for (product_id, qty) in pending_rows {
        if let Some(q) = qty {
            pending_qty_map.insert(product_id, q as i32);
        }
    }
    let pending_ms = pending_start.elapsed().as_millis();

    let audit_calc_start = Instant::now();
    let mut products_to_update: Vec<ProductUpdate> = Vec::new();
    let mut audit_rows: Vec<AuditRow> = Vec::new();
    let mut skipped_count = 0u32;
    let mut matched_by_ean = 0u32;
    let mut matched_by_ref = 0u32;
    let mut not_found_sync = 0u32;
    let mut different_stock = 0u32;
    let mut different_price = 0u32;
    let mut different_pum = 0u32;
    let production_host = "192.168.0.231";
    let mut matched_erp_productoids: HashSet<String> = HashSet::new();

    for product in &ps_products {
        let ean13 = product.ean13.trim();
        let ref_code = product.reference.trim();
        let current_qty = product.current_qty.unwrap_or(0);
        let current_price = product.current_price.unwrap_or(0.0);
        let pending_qty = pending_qty_map
            .get(&product.id_product)
            .copied()
            .unwrap_or(0);
        let (ps_group, ps_subgroup) = ps_category_groups
            .get(&product.id_product)
            .cloned()
            .unwrap_or_default();

        let (sync_item, sync_key, match_type) = lookup_stock(sync_stock, ean13, ref_code);
        if match_type == "EAN" {
            matched_by_ean += 1;
        } else if match_type == "REF" {
            matched_by_ref += 1;
        }

        let prod_qty = stocks_by_host
            .get(production_host)
            .and_then(|stock| lookup_stock(stock, ean13, ref_code).0.map(|item| item.qty));
        let (
            sync_final_qty,
            inventory_for_mariadb,
            erp_productoid,
            erp_name,
            erp_unit,
            mariadb_unit,
            conversion_factor,
            price_erp,
            price_for_mariadb,
            price_lists,
            action,
            final_pum,
        ) = if let Some(erp_item) = sync_item {
            matched_erp_productoids.insert(erp_item.productoid.clone());
            let erp_qty = erp_item.qty;
            let (mariadb_unit, conversion_factor) =
                infer_local_unit_and_factor(&product.name, &erp_item.unit);
            let stock_factor = if conversion_factor > 0.0 {
                conversion_factor
            } else {
                1.0
            };
            let inventory_for_mariadb = (erp_qty / stock_factor).floor().max(0.0) as i32;
            let final_qty = (inventory_for_mariadb - pending_qty).max(0);
            let converted_price = erp_item.sales_price.map(|price| price * conversion_factor);
            let update_stock = current_qty != final_qty;
            let update_price = price_is_different(current_price, converted_price);
            let final_pum = resolve_pum_update(product, erp_item, converted_price, &pum_plan);
            let update_pum = final_pum
                .as_ref()
                .map(|pum| pum_is_different(product, pum))
                .unwrap_or(false);

            if update_stock {
                different_stock += 1;
            }
            if update_price {
                different_price += 1;
            }
            if update_pum {
                different_pum += 1;
            }

            if update_stock || update_price || update_pum {
                products_to_update.push(ProductUpdate {
                    id_product: product.id_product,
                    current_qty,
                    erp_qty: inventory_for_mariadb,
                    pending_qty,
                    final_qty,
                    erp_key: sync_key.clone(),
                    current_price,
                    final_price: converted_price,
                    final_pum: final_pum.clone(),
                    update_stock,
                    update_price,
                    update_pum,
                });
                let mut action_parts = Vec::new();
                if update_stock {
                    action_parts.push("STOCK");
                }
                if update_price {
                    action_parts.push("PRECIO");
                }
                if update_pum {
                    action_parts.push("PUM");
                }
                let action = format!("ACTUALIZAR_{}", action_parts.join("_"));
                (
                    Some(final_qty),
                    Some(inventory_for_mariadb),
                    erp_item.productoid.clone(),
                    erp_item.name.clone(),
                    normalize_unit(&erp_item.unit),
                    mariadb_unit,
                    conversion_factor,
                    erp_item.sales_price,
                    converted_price,
                    erp_item.price_lists.clone(),
                    action,
                    final_pum,
                )
            } else {
                skipped_count += 1;
                (
                    Some(final_qty),
                    Some(inventory_for_mariadb),
                    erp_item.productoid.clone(),
                    erp_item.name.clone(),
                    normalize_unit(&erp_item.unit),
                    mariadb_unit,
                    conversion_factor,
                    erp_item.sales_price,
                    converted_price,
                    erp_item.price_lists.clone(),
                    "SIN_CAMBIO".to_string(),
                    final_pum,
                )
            }
        } else {
            not_found_sync += 1;
            skipped_count += 1;
            (
                None,
                None,
                String::new(),
                String::new(),
                String::new(),
                String::new(),
                1.0,
                None,
                None,
                String::new(),
                "PRODUCTO PARA CREAR".to_string(),
                None,
            )
        };

        let purchase_info = purchase_info_by_product
            .get(&erp_productoid)
            .cloned()
            .unwrap_or_default();
        let (
            erp_group_level_1,
            erp_group_level_2,
            erp_group_level_4,
            erp_group_level_6,
            erp_group_level_7,
        ) = sync_item
            .map(|item| {
                (
                    item.erp_group_level_1.clone(),
                    item.erp_group_level_2.clone(),
                    item.erp_group_level_4.clone(),
                    item.erp_group_level_6.clone(),
                    item.erp_group_level_7.clone(),
                )
            })
            .unwrap_or_default();

        audit_rows.push(AuditRow {
            id_product_erp: erp_productoid,
            id_product_mariadb: product.id_product,
            name: product.name.clone(),
            reference: product.reference.clone(),
            ean13: product.ean13.clone(),
            code: sync_key,
            erp_name,
            erp_unit,
            mariadb_unit,
            conversion_factor,
            stock_prod: prod_qty,
            inventory_for_mariadb,
            stock_mariadb: current_qty,
            pending_qty,
            sync_final_qty,
            price_mariadb: current_price,
            price_erp,
            price_for_mariadb,
            pum_source: action_pum_source(&final_pum),
            pum_unity: final_pum
                .as_ref()
                .map(|pum| pum.unity.clone())
                .unwrap_or_default(),
            pum_ratio: final_pum.as_ref().map(|pum| pum.ratio),
            pum_unit_price: final_pum.as_ref().map(|pum| pum.unit_price),
            price_lists,
            erp_group_level_1,
            erp_group_level_2,
            erp_group_level_4,
            erp_group_level_6,
            erp_group_level_7,
            ps_group,
            ps_subgroup,
            last_purchase_date: purchase_info.date,
            last_purchase_qty: purchase_info.qty,
            last_purchase_value: purchase_info.value,
            action,
        });
    }

    for erp_item in &sync_stock.items {
        if erp_item.productoid.is_empty() || matched_erp_productoids.contains(&erp_item.productoid)
        {
            continue;
        }

        let purchase_info = purchase_info_by_product
            .get(&erp_item.productoid)
            .cloned()
            .unwrap_or_default();

        audit_rows.push(AuditRow {
            id_product_erp: erp_item.productoid.clone(),
            id_product_mariadb: 0,
            name: String::new(),
            reference: String::new(),
            ean13: String::new(),
            code: erp_item.productoid.clone(),
            erp_name: erp_item.name.clone(),
            erp_unit: normalize_unit(&erp_item.unit),
            mariadb_unit: normalize_unit(&erp_item.unit),
            conversion_factor: 1.0,
            stock_prod: Some(erp_item.qty),
            inventory_for_mariadb: Some(erp_item.qty.floor().max(0.0) as i32),
            stock_mariadb: 0,
            pending_qty: 0,
            sync_final_qty: Some(erp_item.qty.floor().max(0.0) as i32),
            price_mariadb: 0.0,
            price_erp: erp_item.sales_price,
            price_for_mariadb: erp_item.sales_price,
            pum_source: String::new(),
            pum_unity: String::new(),
            pum_ratio: None,
            pum_unit_price: None,
            price_lists: erp_item.price_lists.clone(),
            erp_group_level_1: erp_item.erp_group_level_1.clone(),
            erp_group_level_2: erp_item.erp_group_level_2.clone(),
            erp_group_level_4: erp_item.erp_group_level_4.clone(),
            erp_group_level_6: erp_item.erp_group_level_6.clone(),
            erp_group_level_7: erp_item.erp_group_level_7.clone(),
            ps_group: String::new(),
            ps_subgroup: String::new(),
            last_purchase_date: purchase_info.date,
            last_purchase_qty: purchase_info.qty,
            last_purchase_value: purchase_info.value,
            action: "PRODUCTO_PARA_CREAR".to_string(),
        });
    }
    let audit_calc_ms = audit_calc_start.elapsed().as_millis();

    println!();
    println!("============================================================");
    println!("RESUMEN GERENCIAL DE AUDITORIA");
    println!("============================================================");
    println!("Productos Prestashop              : {}", ps_products.len());
    println!("Servidor sincronizacion MariaDB   : {}", sync_host);
    println!("Coincidencias sync por EAN        : {}", matched_by_ean);
    println!("Coincidencias sync por REF        : {}", matched_by_ref);
    println!("Sin coincidencia en sync          : {}", not_found_sync);
    println!("Inventarios diferentes           : {}", different_stock);
    println!("Precios diferentes                : {}", different_price);
    println!("PUM diferentes                    : {}", different_pum);
    println!(
        "Registros que serian actualizados : {}",
        products_to_update.len()
    );
    println!(
        "Modo                              : {}",
        if audit_only { "AUDITORIA" } else { "APLICAR" }
    );
    println!("Omitidos                          : {}", skipped_count);
    println!(
        "Excluidos EXCLUIR_SIN_MOVIMIENTO sin compra/ingreso historico: {}",
        audit_rows
            .iter()
            .filter(|row| row.action == "EXCLUIR_SIN_MOVIMIENTO")
            .filter(|row| row.last_purchase_date.is_empty())
            .count()
    );
    println!("============================================================");
    println!();

    println!(
        "{:<10} {:<20} {:>10} {:>10} {:>10} {:>8} {:>12} {:>12} {:>10} {:>10} {:>12} {:<24}",
        "ID_MDB",
        "CODIGO",
        "INV_ERP",
        "INV_MDB",
        "MARIADB",
        "UND_ERP",
        "UND_MDB",
        "FACTOR",
        "PS_PRECIO",
        "PRECIO_ERP",
        "PRECIO_MDB",
        "ACCION"
    );
    println!("{}", "-".repeat(118));

    for row in audit_rows
        .iter()
        .filter(|row| should_emit_audit_row(row))
        .take(200)
    {
        println!(
            "{:<10} {:<20} {:>10} {:>10} {:>10} {:>8} {:>12} {:>12.4} {:>10.2} {:>10} {:>12} {:<24}",
            row.id_product_mariadb,
            row.code,
            fmt_qty_decimal(row.stock_prod),
            fmt_qty(row.inventory_for_mariadb),
            row.stock_mariadb,
            row.erp_unit,
            row.mariadb_unit,
            row.conversion_factor,
            row.price_mariadb,
            fmt_price(row.price_erp),
            fmt_price(row.price_for_mariadb),
            row.action
        );
    }

    println!(
        "Auditoria detecto {} registros que deberian actualizarse contra {}.",
        products_to_update.len(),
        sync_host
    );
    println!(
        "De {} productos activos, {} requieren actualizar stock, precio y/o PUM.",
        ps_products.len(),
        products_to_update.len()
    );

    let csv_start = Instant::now();
    let csv_name = format!(
        "stock_auditoria_{}.csv",
        Local::now().format("%Y%m%d_%H%M%S")
    );

    let mut csv = File::create(&csv_name)?;
    writeln!(
        csv,
        "id_product_erp,id_product_mariadb,nombre_prestashop,nombre_erp,referencia,ean13,codigo_match,grupo_erp_nivel_1,grupo_erp_nivel_2,grupo_erp_nivel_4,grupo_erp_nivel_6,grupo_erp_nivel_7,grupo_prestashop,subgrupo_prestashop,unidad_erp,unidad_mariadb_inferida,factor_conversion_precio,inventario_erp_192_168_0_231,inventario_para_mariadb,inventario_mariadb,pendiente,final_sync,precio_mariadb,precio_erp,precio_para_mariadb,pum_fuente,pum_unidad,pum_ratio,pum_precio_unitario,otras_listas_precios_erp,servidor_sync,fecha_ultima_compra_erp,cantidad_ultima_compra_erp,valor_ultima_compra_erp,accion"
    )?;

    for row in &audit_rows {
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.6},{},{},{},{},{},{:.2},{},{},{},{},{},{},{},{},{},{},{},{}",
            csv_escape(&row.id_product_erp),
            row.id_product_mariadb,
            csv_escape(&row.name),
            csv_escape(&row.erp_name),
            csv_escape(&row.reference),
            csv_escape(&row.ean13),
            csv_escape(&row.code),
            csv_escape(&row.erp_group_level_1),
            csv_escape(&row.erp_group_level_2),
            csv_escape(&row.erp_group_level_4),
            csv_escape(&row.erp_group_level_6),
            csv_escape(&row.erp_group_level_7),
            csv_escape(&row.ps_group),
            csv_escape(&row.ps_subgroup),
            csv_escape(&row.erp_unit),
            csv_escape(&row.mariadb_unit),
            row.conversion_factor,
            fmt_qty_decimal(row.stock_prod),
            fmt_qty(row.inventory_for_mariadb),
            row.stock_mariadb,
            row.pending_qty,
            fmt_qty(row.sync_final_qty),
            row.price_mariadb,
            fmt_price(row.price_erp),
            fmt_price(row.price_for_mariadb),
            csv_escape(&row.pum_source),
            csv_escape(&row.pum_unity),
            fmt_decimal(row.pum_ratio),
            fmt_pum_unit_price(row.pum_unit_price),
            csv_escape(&row.price_lists),
            sync_host,
            csv_escape(&row.last_purchase_date),
            csv_escape(&row.last_purchase_qty),
            csv_escape(&row.last_purchase_value),
            row.action
        )?;
    }

    println!("Archivo CSV generado: {}", csv_name);
    let csv_ms = csv_start.elapsed().as_millis();

    let update_start = Instant::now();
    let mut updated_count = 0;
    if audit_only {
        println!(
            "MODO AUDITORIA: se omitio la actualizacion. Revise el CSV y ejecute con --apply para aplicar."
        );
    } else {
        for chunk in products_to_update.chunks(500) {
            let mut tx = conn
                .start_transaction(mysql_async::TxOpts::default())
                .await?;

            let stock_chunk: Vec<&ProductUpdate> = chunk
                .iter()
                .filter(|product| product.update_stock)
                .collect();
            let price_chunk: Vec<&ProductUpdate> = chunk
                .iter()
                .filter(|product| product.update_price && product.final_price.is_some())
                .collect();
            let pum_chunk: Vec<&ProductUpdate> = chunk
                .iter()
                .filter(|product| product.update_pum && product.final_pum.is_some())
                .collect();

            if !stock_chunk.is_empty() {
                let ids = stock_chunk
                    .iter()
                    .map(|product| product.id_product.to_string())
                    .collect::<Vec<String>>()
                    .join(",");
                let qty_cases = stock_chunk
                    .iter()
                    .map(|product| {
                        format!("WHEN {} THEN {}", product.id_product, product.final_qty)
                    })
                    .collect::<Vec<String>>()
                    .join(" ");

                tx.query_drop(format!(
                    "UPDATE {}stock_available \
                     SET quantity = CASE id_product {} ELSE quantity END, \
                         physical_quantity = CASE id_product {} ELSE physical_quantity END \
                     WHERE id_product_attribute = 0 AND id_shop = 1 AND id_product IN ({})",
                    DB_PREFIX, qty_cases, qty_cases, ids
                ))
                .await?;

                let log_values = stock_chunk
                    .iter()
                    .map(|product| {
                        format!(
                            "({}, {}, {}, {}, {}, {}, {}, NOW())",
                            sql_string(&product.erp_key),
                            product.id_product,
                            product.erp_qty,
                            product.pending_qty,
                            product.final_qty,
                            product.current_qty,
                            product.final_qty
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(",");

                tx.query_drop(format!(
                    "INSERT INTO {}erp_stock_sync_log ( \
                        erp_productoid, id_product, erp_disponible, ps_pendiente, \
                        qty_calculada, qty_anterior_ps, qty_aplicada, sync_at \
                     ) VALUES {}",
                    DB_PREFIX, log_values
                ))
                .await?;
            }

            if !price_chunk.is_empty() {
                let ids = price_chunk
                    .iter()
                    .map(|product| product.id_product.to_string())
                    .collect::<Vec<String>>()
                    .join(",");
                let price_cases = price_chunk
                    .iter()
                    .map(|product| {
                        format!(
                            "WHEN {} THEN {:.6}",
                            product.id_product,
                            product.final_price.unwrap_or(product.current_price)
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(" ");

                tx.query_drop(format!(
                    "UPDATE {}product_shop \
                     SET price = CASE id_product {} ELSE price END \
                     WHERE id_shop = 1 AND id_product IN ({})",
                    DB_PREFIX, price_cases, ids
                ))
                .await?;

                tx.query_drop(format!(
                    "UPDATE {}product \
                     SET price = CASE id_product {} ELSE price END \
                     WHERE id_product IN ({})",
                    DB_PREFIX, price_cases, ids
                ))
                .await?;
            }

            if !pum_chunk.is_empty() {
                let ids = pum_chunk
                    .iter()
                    .map(|product| product.id_product.to_string())
                    .collect::<Vec<String>>()
                    .join(",");
                let unity_cases = pum_chunk
                    .iter()
                    .filter_map(|product| {
                        product.final_pum.as_ref().map(|pum| {
                            format!(
                                "WHEN {} THEN {}",
                                product.id_product,
                                sql_string(&pum.unity)
                            )
                        })
                    })
                    .collect::<Vec<String>>()
                    .join(" ");
                let unit_price_cases = pum_chunk
                    .iter()
                    .filter_map(|product| {
                        product.final_pum.as_ref().map(|pum| {
                            format!("WHEN {} THEN {:.6}", product.id_product, pum.unit_price)
                        })
                    })
                    .collect::<Vec<String>>()
                    .join(" ");
                let ratio_cases = pum_chunk
                    .iter()
                    .filter_map(|product| {
                        product
                            .final_pum
                            .as_ref()
                            .map(|pum| format!("WHEN {} THEN {:.6}", product.id_product, pum.ratio))
                    })
                    .collect::<Vec<String>>()
                    .join(" ");

                tx.query_drop(format!(
                    "UPDATE {}product_shop \
                     SET unity = CASE id_product {} ELSE unity END, \
                         unit_price = CASE id_product {} ELSE unit_price END, \
                         unit_price_ratio = CASE id_product {} ELSE unit_price_ratio END \
                     WHERE id_shop = 1 AND id_product IN ({})",
                    DB_PREFIX, unity_cases, unit_price_cases, ratio_cases, ids
                ))
                .await?;

                tx.query_drop(format!(
                    "UPDATE {}product \
                     SET unity = CASE id_product {} ELSE unity END, \
                         unit_price = CASE id_product {} ELSE unit_price END, \
                         unit_price_ratio = CASE id_product {} ELSE unit_price_ratio END \
                     WHERE id_product IN ({})",
                    DB_PREFIX, unity_cases, unit_price_cases, ratio_cases, ids
                ))
                .await?;
            }

            updated_count += chunk.len();
            tx.commit().await?;
        }
    }
    let update_ms = update_start.elapsed().as_millis();

    let metadata_start = Instant::now();
    if !audit_only {
        let now_str = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        conn.exec_drop(
            format!(
                "UPDATE {}configuration SET value = :now WHERE name = 'MERCABOY_ERP_LAST_BATCH_AT'",
                DB_PREFIX
            ),
            params! { "now" => now_str },
        )
        .await?;

        conn.exec_drop(
            format!(
                "UPDATE {}configuration SET value = :updated WHERE name = 'MERCABOY_ERP_LAST_BATCH_UPDATED'",
                DB_PREFIX
            ),
            params! { "updated" => updated_count.to_string() },
        )
        .await?;
    }
    let metadata_ms = metadata_start.elapsed().as_millis();

    if audit_only {
        println!(
            "Auditoria finalizada correctamente: {} diferencias detectadas, 0 actualizados.",
            products_to_update.len()
        );
    } else {
        println!(
            "Sincronizacion finalizada correctamente: {} actualizados, {} omitidos (sin cambios o sin match).",
            updated_count, skipped_count
        );
    }
    println!();
    println!("============================================================");
    println!("TIEMPOS DE EJECUCION (ms)");
    println!("============================================================");
    println!("Conexion MariaDB          : {}", mariadb_connect_ms);
    println!("Configuracion             : {}", config_ms);
    println!("Productos PrestaShop      : {}", products_ms);
    println!("Consulta ERP total        : {}", erp_ms);
    println!("Pendientes PrestaShop     : {}", pending_ms);
    println!("Calculo auditoria         : {}", audit_calc_ms);
    println!("CSV auditoria             : {}", csv_ms);
    println!("Actualizacion MariaDB     : {}", update_ms);
    println!("Metadata batch            : {}", metadata_ms);
    println!(
        "Total programa            : {}",
        total_start.elapsed().as_millis()
    );
    println!("============================================================");
    Ok(())
}
