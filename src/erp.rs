use std::collections::{HashMap, HashSet};
use std::error::Error;

use futures_util::stream::StreamExt;
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::models::{ErpConnection, ErpItem, ErpStock};
use crate::utils::{insert_erp_match_key_with_normalized, normalized_numeric_key, sql_string};

pub fn lookup_stock_by_reference<'a>(
    stock: &'a ErpStock,
    reference: &str,
) -> (Option<&'a ErpItem>, String) {
    let reference = reference.trim();
    if reference.is_empty() {
        return (None, String::new());
    }

    if let Some(item) = stock.by_ref.get(reference) {
        return (Some(item), reference.to_string());
    }

    if let Some(normalized) = normalized_numeric_key(reference) {
        if let Some(item) = stock.by_ref.get(&normalized) {
            return (Some(item), reference.to_string());
        }
    }

    (None, reference.to_string())
}

pub async fn load_erp_stock(
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
            CAST(COALESCE(hp.ivaid, 0) AS VARCHAR(40)) AS IvaId, \
            CAST(hp.PUMContenidoInterno AS VARCHAR(40)) AS PUMContenidoInterno, \
            TRIM(hp.PUMUnidadMedida) AS PUMUnidadMedida, \
            CAST(p.valor AS VARCHAR(40)) AS PrecioVenta, \
            CAST(SUM(COALESCE(ip.invenactua, 0)) AS VARCHAR(40)) AS InventarioUnidades \
         FROM Producto p \
         INNER JOIN HeadProd hp ON hp.headprodid = p.headprodid \
         LEFT JOIN InveProd ip ON ip.productoid = p.productoid AND ip.almacenid IN ({}) \
         GROUP BY p.productoid, p.barras, p.barras2, p.Barras3, hp.referencia, hp.nombre, hp.unidad, hp.ivaid, hp.PUMContenidoInterno, hp.PUMUnidadMedida, p.valor",
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
            let ivaid = row
                .get::<&str, _>("IvaId")
                .and_then(|value| value.trim().replace(',', ".").parse::<f64>().ok())
                .unwrap_or(0.0);
            let pum_content = row
                .get::<&str, _>("PUMContenidoInterno")
                .and_then(|value| value.trim().replace(',', ".").parse::<f64>().ok())
                .filter(|value| *value > 0.0);
            let pum_unit = row
                .get::<&str, _>("PUMUnidadMedida")
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

            let ean_keys = vec![ean.clone(), ean2, ean3];
            records.push((
                ean_keys.clone(),
                productoid.clone(),
                reference.clone(),
                ErpItem {
                    productoid,
                    name,
                    qty,
                    unit,
                    ivaid,
                    pum_content,
                    pum_unit,
                    sales_price,
                    price_lists: String::new(),
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

    for (ean_keys, productoid_key, reference_key, mut item) in records {
        item.price_lists = price_lists_by_product
            .get(&item.productoid)
            .map(|items| items.join(" | "))
            .unwrap_or_default();
        if item.sales_price.is_none() {
            item.sales_price = primary_price_by_product.get(&item.productoid).copied();
        }

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
    })
}



pub async fn inspect_erp_product(
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
