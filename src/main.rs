use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::time::Instant;

use chrono::Local;
use mysql_async::{params, prelude::*, Pool};

mod config;
mod erp;
mod models;
mod pum;
mod utils;

use models::{AuditRow, ErpConnection, Product, ProductUpdate, PumUpdate};

use config::{config_value, is_protected_mariadb_host, read_local_config, LOCAL_ENV_CONFIG};

use utils::{
    csv_escape, fmt_decimal, fmt_price, fmt_pum_unit_price, fmt_qty, fmt_qty_decimal,
    normalize_unit, price_is_different, price_without_tax, sql_string,
};

use pum::{load_pum_plan, pum_is_different, resolve_pum_update};

use erp::{
    inspect_erp_product, load_erp_stock,
    lookup_stock_by_reference,
};

// --- CONFIGURACION DE MARIADB (PrestaShop) ---
const DB_PREFIX: &str = "ps_";

fn action_pum_source(pum: &Option<PumUpdate>) -> String {
    pum.as_ref()
        .map(|pum| pum.source.clone())
        .unwrap_or_default()
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
    println!("Modo de ejecucion               : ACTUALIZAR_EXISTENTES");
    if audit_only {
        println!("MODO AUDITORIA: no se actualizara MariaDB. Use --apply para aplicar cambios.");
    } else {
        println!("MODO APLICAR: se actualizaran productos existentes en PrestaShop.");
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

    println!(
        "Destino MariaDB                 : {}:{} / {}",
        mariadb_host, mariadb_port, mariadb_db
    );

    if apply_changes && is_protected_mariadb_host(&mariadb_host) {
        let allow_production_apply = std::env::var("ALLOW_PRODUCTION_APPLY")
            .ok()
            .map(|value| value == "YES")
            .unwrap_or(false);
        if !allow_production_apply {
            return Err(format!(
                "Bloqueado por seguridad: --apply apunta a MariaDB protegido ({}). \
                 Para produccion se requiere ALLOW_PRODUCTION_APPLY=YES.",
                mariadb_host
            )
            .into());
        }
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

    let erp_host = local_config
        .get("erp_host")
        .or_else(|| local_config.get("MERCABOY_ERP_HOST"))
        .cloned()
        .unwrap_or_else(|| "192.168.0.231".to_string());
    println!("Archivo local de configuracion: {}", LOCAL_ENV_CONFIG);
    println!("Servidor ERP seleccionado     : {}", erp_host);
    println!("Almacenes ERP: {}", almacenes_str);
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

    if let Some(position) = args.iter().position(|arg| arg == "--inspect-product") {
        let product_code = args
            .get(position + 1)
            .ok_or("--inspect-product requiere un codigo de producto")?;
        inspect_erp_product(&erp_host, &erp_connection, product_code).await?;
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
    let products_ms = products_start.elapsed().as_millis();

    let almacenes_list: Vec<String> = almacenes_str
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| format!("'{}'", s.replace('\'', "''")))
        .collect();
    let almacenes_in_clause = almacenes_list.join(", ");

    let erp_start = Instant::now();
    println!("Consultando inventario ERP en {}...", erp_host);
    let sync_stock = load_erp_stock(&erp_host, &erp_connection, &almacenes_in_clause).await?;
    println!(
        "Cargadas claves ERP desde {}: referencia={}, ean={}, productoid={}. Match operativo: referencia.",
        erp_host,
        sync_stock.by_ref.len(),
        sync_stock.by_ean.len(),
        sync_stock.by_productoid.len()
    );
    let erp_ms = erp_start.elapsed().as_millis();

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
    let mut matched_by_ref = 0u32;
    let mut not_found_sync = 0u32;
    let mut different_stock = 0u32;
    let mut different_price = 0u32;
    let mut different_pum = 0u32;

    for product in &ps_products {
        let ref_code = product.reference.trim();
        let current_qty = product.current_qty.unwrap_or(0);
        let current_price = product.current_price.unwrap_or(0.0);
        let pending_qty = pending_qty_map
            .get(&product.id_product)
            .copied()
            .unwrap_or(0);
        let (sync_item, sync_key) = lookup_stock_by_reference(&sync_stock, ref_code);
        if sync_item.is_some() {
            matched_by_ref += 1;
        }

        let prod_qty = sync_item.map(|item| item.qty);
        let (
            sync_final_qty,
            inventory_for_mariadb,
            erp_productoid,
            erp_name,
            erp_unit,
            mariadb_unit,
            conversion_factor,
            price_erp,
            price_sin_impuesto_erp,
            price_for_mariadb,
            price_lists,
            action,
            final_pum,
        ) = if let Some(erp_item) = sync_item {
            let erp_qty = erp_item.qty;
            let mariadb_unit = normalize_unit(&erp_item.unit);
            let conversion_factor = 1.0;
            let inventory_for_mariadb = erp_qty.floor().max(0.0) as i32;
            let final_qty = (inventory_for_mariadb - pending_qty).max(0);
            let price_sin_impuesto_erp = erp_item
                .sales_price
                .map(|price| price_without_tax(price, erp_item.ivaid));
            let price_for_mariadb = price_sin_impuesto_erp;
            let update_stock = current_qty != final_qty;
            let update_price = price_is_different(current_price, price_for_mariadb);
            let final_pum = resolve_pum_update(product, erp_item, price_for_mariadb, &pum_plan);
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
                    final_price: price_for_mariadb,
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
                    price_sin_impuesto_erp,
                    price_for_mariadb,
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
                    price_sin_impuesto_erp,
                    price_for_mariadb,
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
                None,
                String::new(),
                "SIN_MATCH_ERP".to_string(),
                None,
            )
        };

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
            price_sin_impuesto_erp,
            price_for_mariadb,
            pum_source: action_pum_source(&final_pum),
            pum_unity: final_pum
                .as_ref()
                .map(|pum| pum.unity.clone())
                .unwrap_or_default(),
            pum_ratio: final_pum.as_ref().map(|pum| pum.ratio),
            pum_unit_price: final_pum.as_ref().map(|pum| pum.unit_price),
            price_lists,
            action,
        });
    }
    let audit_calc_ms = audit_calc_start.elapsed().as_millis();

    println!();
    println!("============================================================");
    println!("RESUMEN GERENCIAL DE AUDITORIA");
    println!("============================================================");
    println!("Productos Prestashop              : {}", ps_products.len());
    println!("Servidor ERP                      : {}", erp_host);
    println!("Coincidencias sync por referencia : {}", matched_by_ref);
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
    println!("============================================================");
    println!();

    println!(
        "Auditoria detecto {} registros que deberian actualizarse contra {}.",
        products_to_update.len(),
        erp_host
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
        "id_product_erp,id_product_mariadb,nombre_prestashop,nombre_erp,referencia,ean13,codigo_match,unidad_erp,unidad_mariadb_inferida,factor_conversion_precio,inventario_erp,inventario_para_mariadb,inventario_mariadb,pendiente,final_sync,precio_mariadb,precio_erp,precio_sin_impuesto_erp,precio_para_mariadb,pum_fuente,pum_unidad,pum_ratio,pum_precio_unitario,otras_listas_precios_erp,erp_host,accion"
    )?;

    for row in &audit_rows {
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{},{:.6},{},{},{},{},{},{:.2},{},{},{},{},{},{},{},{},{},{}",
            csv_escape(&row.id_product_erp),
            row.id_product_mariadb,
            csv_escape(&row.name),
            csv_escape(&row.erp_name),
            csv_escape(&row.reference),
            csv_escape(&row.ean13),
            csv_escape(&row.code),
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
            fmt_price(row.price_sin_impuesto_erp),
            fmt_price(row.price_for_mariadb),
            csv_escape(&row.pum_source),
            csv_escape(&row.pum_unity),
            fmt_decimal(row.pum_ratio),
            fmt_pum_unit_price(row.pum_unit_price),
            csv_escape(&row.price_lists),
            erp_host,
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
