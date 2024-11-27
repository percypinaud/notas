CREATE
OR REPLACE VIEW public.v_sales AS
SELECT
    CONVERT_TIMEZONE('America/Lima', t44.timestamp_documented) AS timestamp_documented,
    TO_TIMESTAMP(t44.cuid_updated, 'YYYYMMDDHH24MISSUS') AT TIME ZONE 'America/Lima' AS timestamp_modified,
    'Orden' sales_type,
    1 quantity_sold,
    t35.id_stores,
    t35.store store_documented,
    t33.id_commerces,
    t33.commerce,
    t44.uid_orders uid_sales,
    t34.lpn,
    t4.eid_items_1,
    t5.eid_items_2,
    t4.ean,
    CASE
    WHEN t34.id_items = 4876421
    AND t1.subtotal = 0 THEN t1.total
    ELSE t1.subtotal END AS subtotal,
    t1.total,
    t4.stock_up product_type,
    t11.item_2_taxonomy_term color,
    t9.item_2_taxonomy_term unit,
    t7.item_2_taxonomy_term brand,
    t3.item_2_taxonomy_term supplier,
    t39.item_2_hierarchy hierarchy_1,
    t38.item_2_hierarchy hierarchy_2,
    t37.item_2_hierarchy hierarchy_3,
    t36.item_2_hierarchy hierarchy_4,
    t32.item_2_taxonomy_term origins,
    t24.item_2_taxonomy_term heel,
    t26.item_2_taxonomy_term sole,
    t28.item_2_taxonomy_term socks,
    t30.item_2_taxonomy_term lining,
    t5.model,
    t42.first_name || t42.last_name full_name_leader,
    t43.first_name || t43.last_name full_name_seller,
    t46.item_1_taxonomy_term size,
    t47.channel,
    t49.order_payment_type,
    t50.real_cost,
    t5.mpn,
    t44.eid_orders reference,
    CASE
    WHEN UPPER(t5.source) = 'CA' THEN 'COMPRA ASEGURADA'
    WHEN t34.consignment > 0 THEN 'CONSIGNADO'
    WHEN UPPER(t3.item_2_taxonomy_term) IN ('LOREANI', 'SASILVA') THEN 'NACIONAL'
    WHEN t35.id_commerces <> '1' THEN 'B2B'
    ELSE UPPER(t32.item_2_taxonomy_term) END AS source
FROM
    main.main.t_orders_items t1
    INNER JOIN main.main.t_orders t44 ON t44.id_orders = t1.id_orders
    INNER JOIN main.main.t_items t34 ON t34.id_items = t1.id_items
    INNER JOIN main.main.t_items_1 t4 ON t4.id_items_1 = t34.id_items_1
    INNER JOIN main.main.t_items_2 t5 ON t5.id_items_2 = t34.id_items_2
    INNER JOIN main.main.t_stores t35 ON t35.id_stores = t44.id_stores_documented
    INNER JOIN main.main.t_commerces t33 ON t33.id_commerces = t35.id_commerces
    INNER JOIN main.main.t_channels t47 ON t47.id_channels = t44.id_channels
    INNER JOIN (
        SELECT
            id_orders,
            MAX(id_orders_payments_types) id_orders_payments_types
        FROM
            main.main.t_orders_payments
        GROUP BY
            id_orders
    ) t48 ON t48.id_orders = t44.id_orders
    INNER JOIN main.main.t_orders_payments_types t49 ON t49.id_orders_payments_types = t48.id_orders_payments_types
    LEFT JOIN main.main.t_items_2_hierarchies t36 ON t36.id_items_2_hierarchies = t5.id_items_2_hierarchies
    LEFT JOIN main.main.t_items_2_hierarchies t37 ON t37.id_items_2_hierarchies = t36.id_items_2_hierarchies_parents
    LEFT JOIN main.main.t_items_2_hierarchies t38 ON t38.id_items_2_hierarchies = t37.id_items_2_hierarchies_parents
    LEFT JOIN main.main.t_items_2_hierarchies t39 ON t39.id_items_2_hierarchies = t38.id_items_2_hierarchies_parents -- IMAGE
    LEFT JOIN main.main.t_items_2_images t19 ON t19.id_items_2 = t5.id_items_2
    AND t19.width = 30
    AND t19.height = 30
    AND t19.weight = 1 -- SUPPLIER
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t2 ON t2.id_items_2 = t5.id_items_2
    AND t2.id_items_2_taxonomies = 1
    LEFT JOIN main.main.t_items_2_taxonomies_terms t3 ON t3.id_items_2_taxonomies_terms = t2.id_items_2_taxonomies_terms -- BRANDS
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t6 ON t6.id_items_2 = t5.id_items_2
    AND t6.id_items_2_taxonomies = 2
    LEFT JOIN main.main.t_items_2_taxonomies_terms t7 ON t7.id_items_2_taxonomies_terms = t6.id_items_2_taxonomies_terms -- UNITS
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t8 ON t8.id_items_2 = t5.id_items_2
    AND t8.id_items_2_taxonomies = 3
    LEFT JOIN main.main.t_items_2_taxonomies_terms t9 ON t9.id_items_2_taxonomies_terms = t8.id_items_2_taxonomies_terms -- COLOR
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t10 ON t10.id_items_2 = t5.id_items_2
    AND t10.id_items_2_taxonomies = 5
    LEFT JOIN main.main.t_items_2_taxonomies_terms t11 ON t11.id_items_2_taxonomies_terms = t10.id_items_2_taxonomies_terms -- MATERIAL
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t20 ON t20.id_items_2 = t5.id_items_2
    AND t20.id_items_2_taxonomies = 8
    LEFT JOIN main.main.t_items_2_taxonomies_terms t21 ON t21.id_items_2_taxonomies_terms = t20.id_items_2_taxonomies_terms -- HEEL
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t23 ON t23.id_items_2 = t5.id_items_2
    AND t23.id_items_2_taxonomies = 9
    LEFT JOIN main.main.t_items_2_taxonomies_terms t24 ON t24.id_items_2_taxonomies_terms = t23.id_items_2_taxonomies_terms -- SOLE
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t25 ON t25.id_items_2 = t5.id_items_2
    AND t25.id_items_2_taxonomies = 10
    LEFT JOIN main.main.t_items_2_taxonomies_terms t26 ON t26.id_items_2_taxonomies_terms = t25.id_items_2_taxonomies_terms -- SOCKS
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t27 ON t27.id_items_2 = t5.id_items_2
    AND t27.id_items_2_taxonomies = 11
    LEFT JOIN main.main.t_items_2_taxonomies_terms t28 ON t28.id_items_2_taxonomies_terms = t27.id_items_2_taxonomies_terms -- LININGS
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t29 ON t29.id_items_2 = t5.id_items_2
    AND t29.id_items_2_taxonomies = 12
    LEFT JOIN main.main.t_items_2_taxonomies_terms t30 ON t30.id_items_2_taxonomies_terms = t29.id_items_2_taxonomies_terms -- ORIGINS
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t31 ON t31.id_items_2 = t5.id_items_2
    AND t31.id_items_2_taxonomies = 13
    LEFT JOIN main.main.t_items_2_taxonomies_terms t32 ON t32.id_items_2_taxonomies_terms = t31.id_items_2_taxonomies_terms
    AND t32.id_items_2_taxonomies = t31.id_items_2_taxonomies
    LEFT JOIN main.main.t_users t42 ON t42.id_users = t1.id_users_leaders
    LEFT JOin main.main.t_users t43 ON t43.id_users = t1.id_users_sellers -- SIZE
    LEFT JOIN main.main.t_items_1_taxonomies_terms_rels_items_1 t45 ON t45.id_items_1 = t4.id_items_1
    AND t45.id_items_1_taxonomies = 1
    LEFT JOIN main.main.t_items_1_taxonomies_terms t46 ON t46.id_items_1_taxonomies_terms = t45.id_items_1_taxonomies_terms
    LEFT JOIN (
        SELECT
            t3.id_items_2,
            t3.eid_items_2,
            AVG(t1.real_cost) real_cost,
            AVG(t1.price_inv) price_inv
        FROM
            sapb1.t_items_purchases_sap t1
            LEFT JOIN main.main.t_items_1_rels_holdings t4 ON t4.eid = t1.material
            LEFT JOIN main.main.t_items_1 t2 ON t2.eid_items_1 = t1.material
            LEFT JOIN main.main.t_items_1 t5 ON t5.id_items_1 = t4.id_items_1
            LEFT JOIN main.main.t_items_2 t3 ON t3.id_items_2 = COALESCE(t5.id_items_2, t2.id_items_2)
        WHERE
            t1.description IS NOT NULL
        GROUP BY
            t3.id_items_2,
            t3.eid_items_2
    ) t50 ON t50.id_items_2 = t5.id_items_2
WHERE
    t44.id_orders_statuses >= 2
UNION ALL
SELECT
    CONVERT_TIMEZONE('America/Lima', t44.timestamp_documented) AS timestamp_documented,
    TO_TIMESTAMP(t44.cuid_updated, 'YYYYMMDDHH24MISSUS') AT TIME ZONE 'America/Lima' AS timestamp_modified,
    'RMA' sales_type,
    -1 quantity_sold,
    t35.id_stores,
    t35.store store_documented,
    t33.id_commerces,
    t33.commerce,
    t44.uid_rmas uid_sales,
    t34.lpn,
    t4.eid_items_1,
    t5.eid_items_2,
    t4.ean,
    - t1.subtotal,
    - t1.total,
    t4.stock_up product_type,
    t11.item_2_taxonomy_term color,
    t9.item_2_taxonomy_term unit,
    t7.item_2_taxonomy_term brand,
    t3.item_2_taxonomy_term supplier,
    t39.item_2_hierarchy hierarchy_1,
    t38.item_2_hierarchy hierarchy_2,
    t37.item_2_hierarchy hierarchy_3,
    t36.item_2_hierarchy hierarchy_4,
    t32.item_2_taxonomy_term origins,
    t24.item_2_taxonomy_term heel,
    t26.item_2_taxonomy_term sole,
    t28.item_2_taxonomy_term socks,
    t30.item_2_taxonomy_term lining,
    t5.model,
    t42.first_name || t42.last_name full_name_leader,
    t43.first_name || t43.last_name full_name_seller,
    t46.item_1_taxonomy_term size,
    t47.channel,
    t49.order_payment_type,
    - t50.real_cost,
    t5.mpn,
    t44.eid_rmas reference,
    CASE
    WHEN UPPER(t5.source) = 'CA' THEN 'COMPRA ASEGURADA'
    WHEN t34.consignment > 0 THEN 'CONSIGNADO'
    WHEN UPPER(t3.item_2_taxonomy_term) IN ('LOREANI', 'SASILVA') THEN 'NACIONAL'
    WHEN t35.id_commerces <> '1' THEN 'B2B'
    ELSE UPPER(t32.item_2_taxonomy_term) END AS source
FROM
    main.main.t_rmas_items t1
    INNER JOIN main.main.t_rmas t44 ON t44.id_rmas = t1.id_rmas
    INNER JOIN main.main.t_items t34 ON t34.id_items = t1.id_items
    INNER JOIN main.main.t_items_1 t4 ON t4.id_items_1 = t34.id_items_1
    INNER JOIN main.main.t_items_2 t5 ON t5.id_items_2 = t4.id_items_2
    INNER JOIN main.main.t_stores t35 ON t35.id_stores = t44.id_stores_documented
    INNER JOIN main.main.t_commerces t33 ON t33.id_commerces = t35.id_commerces
    INNER JOIN main.main.t_orders t40 ON t40.id_orders = t44.id_orders
    INNER JOIN main.main.t_orders_items t41 ON t41.id_orders = t40.id_orders
    AND t41.id_items = t1.id_items
    INNER JOIN main.main.t_channels t47 ON t47.id_channels = t44.id_channels
    INNER JOIN (
        SELECT
            id_orders,
            MAX(id_orders_payments_types) id_orders_payments_types
        FROM
            main.main.t_orders_payments
        GROUP BY
            id_orders
    ) t48 ON t48.id_orders = t44.id_orders
    INNER JOIN main.main.t_orders_payments_types t49 ON t49.id_orders_payments_types = t48.id_orders_payments_types
    LEFT JOIN main.main.t_items_2_hierarchies t36 ON t36.id_items_2_hierarchies = t5.id_items_2_hierarchies
    LEFT JOIN main.main.t_items_2_hierarchies t37 ON t37.id_items_2_hierarchies = t36.id_items_2_hierarchies_parents
    LEFT JOIN main.main.t_items_2_hierarchies t38 ON t38.id_items_2_hierarchies = t37.id_items_2_hierarchies_parents
    LEFT JOIN main.main.t_items_2_hierarchies t39 ON t39.id_items_2_hierarchies = t38.id_items_2_hierarchies_parents -- IMAGE
    LEFT JOIN main.main.t_items_2_images t19 ON t19.id_items_2 = t5.id_items_2
    AND t19.width = 30
    AND t19.height = 30
    AND t19.weight = 1 -- SUPPLIER
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t2 ON t2.id_items_2 = t5.id_items_2
    AND t2.id_items_2_taxonomies = 1
    LEFT JOIN main.main.t_items_2_taxonomies_terms t3 ON t3.id_items_2_taxonomies_terms = t2.id_items_2_taxonomies_terms -- BRANDS
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t6 ON t6.id_items_2 = t5.id_items_2
    AND t6.id_items_2_taxonomies = 2
    LEFT JOIN main.main.t_items_2_taxonomies_terms t7 ON t7.id_items_2_taxonomies_terms = t6.id_items_2_taxonomies_terms -- UNITS
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t8 ON t8.id_items_2 = t5.id_items_2
    AND t8.id_items_2_taxonomies = 3
    LEFT JOIN main.main.t_items_2_taxonomies_terms t9 ON t9.id_items_2_taxonomies_terms = t8.id_items_2_taxonomies_terms -- COLOR
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t10 ON t10.id_items_2 = t5.id_items_2
    AND t10.id_items_2_taxonomies = 5
    LEFT JOIN main.main.t_items_2_taxonomies_terms t11 ON t11.id_items_2_taxonomies_terms = t10.id_items_2_taxonomies_terms -- MATERIAL
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t20 ON t20.id_items_2 = t5.id_items_2
    AND t20.id_items_2_taxonomies = 8
    LEFT JOIN main.main.t_items_2_taxonomies_terms t21 ON t21.id_items_2_taxonomies_terms = t20.id_items_2_taxonomies_terms -- HEEL
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t23 ON t23.id_items_2 = t5.id_items_2
    AND t23.id_items_2_taxonomies = 9
    LEFT JOIN main.main.t_items_2_taxonomies_terms t24 ON t24.id_items_2_taxonomies_terms = t23.id_items_2_taxonomies_terms -- SOLE
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t25 ON t25.id_items_2 = t5.id_items_2
    AND t25.id_items_2_taxonomies = 10
    LEFT JOIN main.main.t_items_2_taxonomies_terms t26 ON t26.id_items_2_taxonomies_terms = t25.id_items_2_taxonomies_terms -- SOCKS
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t27 ON t27.id_items_2 = t5.id_items_2
    AND t27.id_items_2_taxonomies = 11
    LEFT JOIN main.main.t_items_2_taxonomies_terms t28 ON t28.id_items_2_taxonomies_terms = t27.id_items_2_taxonomies_terms -- LININGS
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t29 ON t29.id_items_2 = t5.id_items_2
    AND t29.id_items_2_taxonomies = 12
    LEFT JOIN main.main.t_items_2_taxonomies_terms t30 ON t30.id_items_2_taxonomies_terms = t29.id_items_2_taxonomies_terms -- ORIGINS
    LEFT JOIN main.main.t_items_2_taxonomies_terms_rels_items_2 t31 ON t31.id_items_2 = t5.id_items_2
    AND t31.id_items_2_taxonomies = 13
    LEFT JOIN main.main.t_items_2_taxonomies_terms t32 ON t32.id_items_2_taxonomies_terms = t31.id_items_2_taxonomies_terms
    AND t32.id_items_2_taxonomies = t31.id_items_2_taxonomies
    LEFT JOIN main.main.t_users t42 ON t42.id_users = t1.id_users_leaders
    LEFT JOin main.main.t_users t43 ON t43.id_users = t1.id_users_sellers -- SIZE
    LEFT JOIN main.main.t_items_1_taxonomies_terms_rels_items_1 t45 ON t45.id_items_1 = t4.id_items_1
    AND t45.id_items_1_taxonomies = 1
    LEFT JOIN main.main.t_items_1_taxonomies_terms t46 ON t46.id_items_1_taxonomies_terms = t45.id_items_1_taxonomies_terms
    LEFT JOIN (
        SELECT
            t3.id_items_2,
            t3.eid_items_2,
            AVG(t1.real_cost) real_cost,
            AVG(t1.price_inv) price_inv
        FROM
            sapb1.t_items_purchases_sap t1
            LEFT JOIN main.main.t_items_1_rels_holdings t4 ON t4.eid = t1.material
            LEFT JOIN main.main.t_items_1 t2 ON t2.eid_items_1 = t1.material
            LEFT JOIN main.main.t_items_1 t5 ON t5.id_items_1 = t4.id_items_1
            LEFT JOIN main.main.t_items_2 t3 ON t3.id_items_2 = COALESCE(t5.id_items_2, t2.id_items_2)
        WHERE
            t1.description IS NOT NULL
        GROUP BY
            t3.id_items_2,
            t3.eid_items_2
    ) t50 ON t50.id_items_2 = t5.id_items_2
WHERE
    t44.id_rmas_statuses NOT IN (-2, -1, 1) WITH NO SCHEMA BINDING;