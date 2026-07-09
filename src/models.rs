use std::collections::HashMap;

#[derive(Debug)]
pub struct Product {
    pub id_product: u32,
    pub name: String,
    pub ean13: String,
    pub reference: String,
    pub current_qty: Option<i32>,
    pub current_price: Option<f64>,
    pub current_unity: String,
    pub current_unit_price: Option<f64>,
    pub current_unit_price_ratio: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct ErpConnection {
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: String,
}

#[derive(Debug)]
pub struct ErpStock {
    pub by_ean: HashMap<String, ErpItem>,
    pub by_productoid: HashMap<String, ErpItem>,
    pub by_ref: HashMap<String, ErpItem>,
}

#[derive(Debug, Clone)]
pub struct ErpItem {
    pub productoid: String,
    pub name: String,
    pub qty: f64,
    pub unit: String,
    pub pum_content: Option<f64>,
    pub pum_unit: String,
    pub sales_price: Option<f64>,
    pub price_lists: String,
    pub ivaid: f64,
}

#[derive(Debug)]
pub struct ProductUpdate {
    pub id_product: u32,
    pub current_qty: i32,
    pub erp_qty: i32,
    pub pending_qty: i32,
    pub final_qty: i32,
    pub erp_key: String,
    pub current_price: f64,
    pub final_price: Option<f64>,
    pub final_pum: Option<PumUpdate>,
    pub update_stock: bool,
    pub update_price: bool,
    pub update_pum: bool,
}

#[derive(Debug)]
pub struct AuditRow {
    pub id_product_erp: String,
    pub id_product_mariadb: u32,
    pub name: String,
    pub reference: String,
    pub ean13: String,
    pub code: String,
    pub erp_name: String,
    pub erp_unit: String,
    pub mariadb_unit: String,
    pub conversion_factor: f64,
    pub stock_prod: Option<f64>,
    pub inventory_for_mariadb: Option<i32>,
    pub stock_mariadb: i32,
    pub pending_qty: i32,
    pub sync_final_qty: Option<i32>,
    pub price_mariadb: f64,
    pub price_erp: Option<f64>,
    pub price_sin_impuesto_erp: Option<f64>,
    pub price_for_mariadb: Option<f64>,
    pub pum_source: String,
    pub pum_unity: String,
    pub pum_ratio: Option<f64>,
    pub pum_unit_price: Option<f64>,
    pub price_lists: String,
    pub action: String,
}

#[derive(Debug, Clone)]
pub struct PumSeed {
    pub unity: String,
    pub ratio: f64,
    pub source: String,
}

#[derive(Debug, Clone)]
pub struct PumUpdate {
    pub unity: String,
    pub ratio: f64,
    pub unit_price: f64,
    pub source: String,
}
